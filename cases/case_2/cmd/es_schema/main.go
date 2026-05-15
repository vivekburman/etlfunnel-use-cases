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

var (
	flagES     = flag.String("es", "http://localhost:9200", "Elasticsearch base URL")
	flagESUser = flag.String("es-user", "elastic", "Elasticsearch username")
	flagESPass = flag.String("es-pass", "etl_pass", "Elasticsearch password")
)

const indexName = "platform_orders"

func main() {
	flag.Parse()
	log.Println("=== Elasticsearch Schema Creator ===")

	exists, err := indexExists(*flagES, *flagESUser, *flagESPass, indexName)
	if err != nil {
		log.Fatalf("[es] checking index existence: %v", err)
	}
	if exists {
		log.Printf("[es] index %q already exists, skipping creation", indexName)
		log.Println("=== ES schema setup complete ===")
		return
	}

	if err := createIndex(*flagES, *flagESUser, *flagESPass, indexName); err != nil {
		log.Fatalf("[es] create index: %v", err)
	}
	log.Printf("[es] index %q created", indexName)
	log.Println("=== ES schema setup complete ===")
}

// esRequest performs an authenticated HTTP request against Elasticsearch.
func esRequest(method, url, user, pass string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

func indexExists(base, user, pass, name string) (bool, error) {
	resp, err := esRequest(http.MethodGet, fmt.Sprintf("%s/%s", base, name), user, pass, nil)
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

func createIndex(base, user, pass, name string) error {
	payload := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   5,
			"number_of_replicas": 1,
			"refresh_interval":   "30s",
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"order_id":           map[string]string{"type": "keyword"},
				"sub_brand":          map[string]string{"type": "keyword"},
				"city":               map[string]string{"type": "keyword"},
				"zone_label":         map[string]string{"type": "keyword"},
				"state":              map[string]string{"type": "keyword"},
				"order_status":       map[string]string{"type": "keyword"},
				"canonical_status":   map[string]string{"type": "keyword"},
				"fulfilment_type":    map[string]string{"type": "keyword"},
				"sla_status":         map[string]string{"type": "keyword"},
				"meal_period":        map[string]string{"type": "keyword"},
				"event_category":     map[string]string{"type": "keyword"},
				"order_value_band":   map[string]string{"type": "keyword"},
				"cancellation_stage": map[string]string{"type": "keyword"},
				"total_amount":       map[string]string{"type": "float"},
				"item_count":         map[string]string{"type": "integer"},
				"placed_at":          map[string]string{"type": "date"},
				"completed_at":       map[string]string{"type": "date"},
				"promised_minutes":   map[string]string{"type": "integer"},
				"actual_minutes":     map[string]string{"type": "integer"},
				"customer_id_hash":   map[string]string{"type": "keyword"},
				"flow_type":          map[string]string{"type": "keyword"},
				"indexed_at":         map[string]string{"type": "date"},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := esRequest(http.MethodPut, fmt.Sprintf("%s/%s", base, name), user, pass, bytes.NewReader(body))
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
