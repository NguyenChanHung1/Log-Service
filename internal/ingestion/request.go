package ingestion

type Request struct {
	Source  string   `json:"source"`
	Records []string `json:"records"`
}

type Response struct {
	Accepted int    `json:"accepted"`
	Storage  string `json:"storage"`
	Degraded bool   `json:"degraded,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
