package logevent

import "time"

type Event struct {
	Source     string    `json:"source"`
	Raw        string    `json:"raw"`
	ReceivedAt time.Time `json:"received_at"`
}
