package handlers

// order_events.go — handles GET /api/v2/order-events
//
// Implements the cursor-based pagination contract from CASE_4_DESIGN.md §7.1.
//
// Request:  GET /api/v2/order-events?cursor=<seq>&limit=<n>
// Response: {"events":[...], "next_cursor":"seq_<n>", "has_more":bool}
//
// Cursor format: "seq_<index>" where index is the position in the event pool.
// Empty cursor means "start from the beginning" (index 0).
// The X-Internal-Token header must be non-empty (any value accepted).

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/streamcraft/zepto-etl/case4/cmd/seeder/generators"
)

type Handler struct {
	events []generators.OrderEvent
}

func NewOrderEventsHandler(pool []generators.OrderEvent) *Handler {
	return &Handler{events: pool}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.Header.Get("X-Internal-Token")
	if token == "" {
		http.Error(w, "missing X-Internal-Token header", http.StatusUnauthorized)
		return
	}

	q := r.URL.Query()

	limit := 500
	if lStr := q.Get("limit"); lStr != "" {
		if n, err := strconv.Atoi(lStr); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	startIdx := 0
	if cursor := q.Get("cursor"); cursor != "" {
		startIdx = parseCursor(cursor)
	}

	total := len(h.events)
	if startIdx >= total {
		writeResponse(w, []generators.OrderEvent{}, "", false)
		return
	}

	end := startIdx + limit
	if end > total {
		end = total
	}
	page := h.events[startIdx:end]

	hasMore := end < total
	nextCursor := ""
	if hasMore {
		nextCursor = fmt.Sprintf("seq_%d", end)
	}

	log.Printf("[seeder] GET /api/v2/order-events cursor=%d limit=%d returned=%d has_more=%v",
		startIdx, limit, len(page), hasMore)

	writeResponse(w, page, nextCursor, hasMore)
}

func writeResponse(w http.ResponseWriter, events []generators.OrderEvent, nextCursor string, hasMore bool) {
	resp := map[string]any{
		"events":      events,
		"next_cursor": nextCursor,
		"has_more":    hasMore,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// parseCursor extracts the integer index from "seq_<n>".
// Returns 0 for any unparseable cursor (safe restart from beginning).
func parseCursor(cursor string) int {
	cursor = strings.TrimPrefix(cursor, "seq_")
	n, err := strconv.Atoi(cursor)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
