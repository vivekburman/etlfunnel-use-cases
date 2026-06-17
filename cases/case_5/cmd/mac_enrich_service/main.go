// cases/case_5/cmd/mac_enrich_service/main.go
package main

// Mac Enrich Service — thin HTTP wrapper around Ollama nomic-embed-text.
//
// POST /enrich  — accept a batch of records, queue for Ollama embedding.
// GET /results  — cursor-paginated access to completed embeddings.
// GET /health   — liveness probe.
//
// Storage is in-memory. Restart clears the queue. For durability across
// restarts, replace the in-memory slices with a SQLite file (modernc.org/sqlite).
//
// Start: ENRICH_PORT=8765 OLLAMA_URL=http://localhost:11434 go run main.go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// ── data types ────────────────────────────────────────────────────────────────

type enrichRequest struct {
	BatchID string           `json:"batch_id"`
	Records []map[string]any `json:"records"`
}

type enrichedResult struct {
	ProductID   string    `json:"product_id"`
	Embedding   []float32 `json:"embedding"`
	EnrichedAt  string    `json:"enriched_at"`
	Passthrough map[string]any
}

func (r enrichedResult) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(r.Passthrough)+3)
	for k, v := range r.Passthrough {
		m[k] = v
	}
	m["product_id"] = r.ProductID
	m["embedding"] = r.Embedding
	m["enriched_at"] = r.EnrichedAt
	return json.Marshal(m)
}

type ollamaEmbedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type ollamaEmbedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// ── service state ─────────────────────────────────────────────────────────────

type service struct {
	mu        sync.Mutex
	pending   []map[string]any // records waiting for Ollama
	done      []enrichedResult // completed results (append-only, indexed by position)
	ollamaURL string
}

func (s *service) enqueue(records []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, records...)
}

func (s *service) drain() ([]map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil, false
	}
	batch := s.pending
	s.pending = nil
	return batch, true
}

func (s *service) appendDone(results []enrichedResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = append(s.done, results...)
}

func (s *service) getResults(cursor, limit int) ([]enrichedResult, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor >= len(s.done) {
		return nil, cursor, false
	}
	end := cursor + limit
	if end > len(s.done) {
		end = len(s.done)
	}
	return s.done[cursor:end], end, end < len(s.done)
}

func (s *service) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), len(s.done)
}

// ── Ollama caller ─────────────────────────────────────────────────────────────

func (s *service) embed(texts []string) ([][]float32, error) {
	body, _ := json.Marshal(ollamaEmbedRequest{Model: "nomic-embed-text", Input: texts})
	resp, err := http.Post(s.ollamaURL+"/api/embed", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ollama POST: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ollama %d: %s", resp.StatusCode, raw)
	}
	var out ollamaEmbedResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("ollama unmarshal: %w", err)
	}
	return out.Embeddings, nil
}

// ── background worker ─────────────────────────────────────────────────────────

func (s *service) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
			batch, ok := s.drain()
			if !ok {
				continue
			}

			texts := make([]string, len(batch))
			for i, rec := range batch {
				texts[i], _ = rec["embed_text"].(string)
			}

			embeddings, err := s.embed(texts)
			if err != nil {
				log.Printf("embed error (requeueing %d records): %v", len(batch), err)
				s.enqueue(batch) // requeue on transient Ollama error
				time.Sleep(2 * time.Second)
				continue
			}

			results := make([]enrichedResult, 0, len(batch))
			for i, rec := range batch {
				if i >= len(embeddings) {
					break
				}
				pid, _ := rec["product_id"].(string)
				results = append(results, enrichedResult{
					ProductID:  pid,
					Embedding:  embeddings[i],
					EnrichedAt: time.Now().UTC().Format(time.RFC3339),
					Passthrough: rec,
				})
			}
			s.appendDone(results)
			log.Printf("embedded %d records (total done: %d)", len(results), func() int { _, d := s.stats(); return d }())
		}
	}
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

func (s *service) handleEnrich(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req enrichRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.enqueue(req.Records)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{"batch_id": req.BatchID, "queued": len(req.Records)})
}

func (s *service) handleResults(w http.ResponseWriter, r *http.Request) {
	cursorStr := r.URL.Query().Get("cursor")
	limitStr := r.URL.Query().Get("limit")
	cursor, _ := strconv.Atoi(cursorStr)
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	results, nextCursor, hasMore := s.getResults(cursor, limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"results":     results,
		"next_cursor": nextCursor,
		"has_more":    hasMore,
	})
}

func (s *service) handleHealth(w http.ResponseWriter, r *http.Request) {
	pending, done := s.stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "ok", "pending": pending, "done": done})
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	port := os.Getenv("ENRICH_PORT")
	if port == "" {
		port = "8765"
	}
	ollamaURL := os.Getenv("OLLAMA_URL")
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}

	svc := &service{ollamaURL: ollamaURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.worker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/enrich", svc.handleEnrich)
	mux.HandleFunc("/results", svc.handleResults)
	mux.HandleFunc("/health", svc.handleHealth)

	log.Printf("Mac Enrich Service listening on :%s (Ollama: %s)", port, ollamaURL)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
