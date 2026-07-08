package logline

import (
	"errors"
	"testing"
	"time"
)

func TestParseValidLine(t *testing.T) {
	record, err := Parse("2026-07-07T09:00:01Z 10.10.1.5 GET /login 200")
	if err != nil {
		t.Fatalf("expected valid line, got error: %v", err)
	}

	wantTime, err := time.Parse(time.RFC3339, "2026-07-07T09:00:01Z")
	if err != nil {
		t.Fatalf("test timestamp is invalid: %v", err)
	}

	if !record.Timestamp.Equal(wantTime) {
		t.Fatalf("expected timestamp %s, got %s", wantTime, record.Timestamp)
	}
	if got := record.IP.String(); got != "10.10.1.5" {
		t.Fatalf("expected ip 10.10.1.5, got %s", got)
	}
	if record.Method != "GET" {
		t.Fatalf("expected method GET, got %s", record.Method)
	}
	if record.Path != "/login" {
		t.Fatalf("expected path /login, got %s", record.Path)
	}
	if record.Status != 200 {
		t.Fatalf("expected status 200, got %d", record.Status)
	}
	if record.Raw != "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200" {
		t.Fatalf("expected raw line to be preserved, got %q", record.Raw)
	}
}

func TestParseTrimsOuterWhitespace(t *testing.T) {
	record, err := Parse("  2026-07-07T09:00:02Z 10.10.1.6 POST /payment 500  ")
	if err != nil {
		t.Fatalf("expected valid line, got error: %v", err)
	}

	if record.Method != "POST" || record.Path != "/payment" || record.Status != 500 {
		t.Fatalf("unexpected parsed record: %+v", record)
	}
	if record.Raw != "2026-07-07T09:00:02Z 10.10.1.6 POST /payment 500" {
		t.Fatalf("expected trimmed raw line, got %q", record.Raw)
	}
}

func TestParseRejectsInvalidLines(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr error
	}{
		{
			name:    "empty line",
			line:    "  ",
			wantErr: ErrEmptyLine,
		},
		{
			name:    "missing field",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET /login",
			wantErr: ErrInvalidFieldCount,
		},
		{
			name:    "extra field",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET /login 200 extra",
			wantErr: ErrInvalidFieldCount,
		},
		{
			name:    "invalid timestamp",
			line:    "2026-07-07 10.10.1.5 GET /login 200",
			wantErr: ErrInvalidTimestamp,
		},
		{
			name:    "invalid ip",
			line:    "2026-07-07T09:00:01Z 999.10.1.5 GET /login 200",
			wantErr: ErrInvalidIP,
		},
		{
			name:    "unsupported method",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 TRACE /login 200",
			wantErr: ErrInvalidMethod,
		},
		{
			name:    "path must be absolute",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET login 200",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "status is not an integer",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET /login OK",
			wantErr: ErrInvalidStatus,
		},
		{
			name:    "status below http range",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET /login 99",
			wantErr: ErrInvalidStatus,
		},
		{
			name:    "status above http range",
			line:    "2026-07-07T09:00:01Z 10.10.1.5 GET /login 600",
			wantErr: ErrInvalidStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.line)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestIsAllowedMethod(t *testing.T) {
	if !IsAllowedMethod("GET") {
		t.Fatal("expected GET to be allowed")
	}
	if IsAllowedMethod("TRACE") {
		t.Fatal("expected TRACE to be rejected")
	}
}
