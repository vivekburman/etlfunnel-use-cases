package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
)

var flagES = flag.String("es", "http://localhost:9200", "Elasticsearch base URL")

const indexName = "merchant_txn_summary"

func main() {
	flag.Parse()
	log.Println("=== Elasticsearch Schema Creator ===")

	exists, err := indexExists(*flagES, indexName)
	if err != nil {
		log.Fatalf("[es] checking index existence: %v", err)
	}
	if exists {
		log.Printf("[es] index %q already exists, skipping creation", indexName)
		log.Println("=== ES schema setup complete ===")
		return
	}

	if err := createIndex(*flagES, indexName); err != nil {
		log.Fatalf("[es] create index: %v", err)
	}
	log.Printf("[es] index %q created", indexName)
	log.Println("=== ES schema setup complete ===")
}

func esRequest(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

func indexExists(base, name string) (bool, error) {
	resp, err := esRequest(http.MethodGet, fmt.Sprintf("%s/%s", base, name), nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	respBody, _ := io.ReadAll(resp.Body)
	return false, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
}

func createIndex(base, name string) error {
	payload := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"txn_id":                map[string]string{"type": "keyword"},
				"merchant_id":           map[string]string{"type": "keyword"},
				"amount":                map[string]string{"type": "float"},
				"risk_tier":             map[string]string{"type": "keyword"},
				"fraud_score":           map[string]string{"type": "float"},
				"fraud_reason":          map[string]string{"type": "keyword"},
				"reconciliation_status": map[string]string{"type": "keyword"},
				"updated_at":            map[string]string{"type": "date"},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := esRequest(http.MethodPut, fmt.Sprintf("%s/%s", base, name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create index failed status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}
