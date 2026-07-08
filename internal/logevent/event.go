package logevent

import "time"

type Event struct {
	Source      string    `json:"source"`
	Raw         string    `json:"raw"`
	Timestamp   time.Time `json:"@timestamp"`
	IP          string    `json:"ip"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Status      int       `json:"status"`
	ReceivedAt  time.Time `json:"received_at"`
	ProcessedAt time.Time `json:"processed_at,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
}
