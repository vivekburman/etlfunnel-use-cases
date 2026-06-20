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

const indexName = "pf_products"

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

func indexExists(base, name string) (bool, error) {
	resp, err := http.Get(fmt.Sprintf("%s/%s", base, name))
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
	body, _ := io.ReadAll(resp.Body)
	return false, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
}

func createIndex(base, name string) error {
	payload := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   3,
			"number_of_replicas": 1,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"product_id":  map[string]string{"type": "keyword"},
				"title":       map[string]interface{}{"type": "text", "analyzer": "english"},
				"description": map[string]interface{}{"type": "text", "analyzer": "english"},
				"category":    map[string]string{"type": "keyword"},
				"price":       map[string]string{"type": "float"},
				"source":      map[string]string{"type": "keyword"},
				"updated_at":  map[string]string{"type": "date"},
				"enriched_at": map[string]string{"type": "date"},
				"run_id":      map[string]string{"type": "keyword"},
				"embedding": map[string]interface{}{
					"type":       "dense_vector",
					"dims":       768,
					"index":      true,
					"similarity": "cosine",
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", base, name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
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
