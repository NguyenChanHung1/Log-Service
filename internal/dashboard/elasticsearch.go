package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type elasticClient struct {
	baseURL string
	client  *http.Client
}

func newElasticClient(baseURL string) *elasticClient {
	return &elasticClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *elasticClient) health(ctx context.Context) map[string]any {
	var result map[string]any
	if err := c.get(ctx, "/_cluster/health", &result); err != nil {
		return map[string]any{"status": "unavailable", "error": err.Error()}
	}
	return result
}

func (c *elasticClient) count(ctx context.Context, from time.Time, to time.Time) int64 {
	body := map[string]any{"query": rangeQuery(from, to, "", "", "")}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := c.post(ctx, "/logs-*/_count", body, &result); err != nil {
		return 0
	}
	return result.Count
}

func (c *elasticClient) metrics(ctx context.Context, from time.Time, to time.Time, ip string, path string) ([]MetricsPoint, error) {
	body := map[string]any{
		"size":  0,
		"query": rangeQuery(from, to, ip, path, ""),
		"aggs": map[string]any{
			"logs_over_time": map[string]any{
				"date_histogram": map[string]any{
					"field":          "@timestamp",
					"fixed_interval": "1m",
					"min_doc_count":  0,
					"extended_bounds": map[string]string{
						"min": from.UTC().Format(time.RFC3339),
						"max": to.UTC().Format(time.RFC3339),
					},
				},
			},
		},
	}

	var result struct {
		Aggregations struct {
			LogsOverTime struct {
				Buckets []struct {
					KeyAsString string `json:"key_as_string"`
					DocCount    int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"logs_over_time"`
		} `json:"aggregations"`
	}
	if err := c.post(ctx, "/logs-*/_search", body, &result); err != nil {
		return nil, err
	}

	points := make([]MetricsPoint, 0, len(result.Aggregations.LogsOverTime.Buckets))
	for _, bucket := range result.Aggregations.LogsOverTime.Buckets {
		ts, err := time.Parse(time.RFC3339, bucket.KeyAsString)
		if err != nil {
			continue
		}
		points = append(points, MetricsPoint{TimeBucket: ts, Count: bucket.DocCount})
	}
	return points, nil
}

func (c *elasticClient) logs(ctx context.Context, from time.Time, to time.Time, ip string, path string, status string, limit int) (LogsResponse, error) {
	body := map[string]any{
		"size":  limit,
		"query": rangeQuery(from, to, ip, path, status),
		"sort": []map[string]any{
			{"@timestamp": map[string]string{"order": "desc"}},
		},
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source LogRecord `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := c.post(ctx, "/logs-*/_search", body, &result); err != nil {
		return LogsResponse{}, err
	}

	logs := make([]LogRecord, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		logs = append(logs, hit.Source)
	}
	return LogsResponse{
		From:  from,
		To:    to,
		Limit: limit,
		Total: result.Hits.Total.Value,
		Logs:  logs,
	}, nil
}

func rangeQuery(from time.Time, to time.Time, ip string, path string, status string) map[string]any {
	filters := []map[string]any{
		{
			"range": map[string]any{
				"@timestamp": map[string]string{
					"gte": from.UTC().Format(time.RFC3339),
					"lte": to.UTC().Format(time.RFC3339),
				},
			},
		},
	}
	if ip != "" {
		filters = append(filters, map[string]any{"term": map[string]string{"ip": ip}})
	}
	if path != "" {
		filters = append(filters, map[string]any{"term": map[string]string{"path": path}})
	}
	if status != "" {
		parsed, err := strconv.Atoi(status)
		if err == nil {
			filters = append(filters, map[string]any{"term": map[string]int{"status": parsed}})
		}
	}
	return map[string]any{"bool": map[string]any{"filter": filters}}
}

func (c *elasticClient) get(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch returned %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(target)
}

func (c *elasticClient) post(ctx context.Context, path string, body any, target any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		if strings.Contains(url.PathEscape(path), "logs-%2A") {
			return nil
		}
	}
	if res.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch returned %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(target)
}
