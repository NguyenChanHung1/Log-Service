package logline

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

var (
	ErrEmptyLine         = errors.New("log line is empty")
	ErrInvalidFieldCount = errors.New("log line must contain exactly 5 fields")
	ErrInvalidTimestamp  = errors.New("invalid timestamp")
	ErrInvalidIP         = errors.New("invalid ip")
	ErrInvalidMethod     = errors.New("invalid method")
	ErrInvalidPath       = errors.New("invalid path")
	ErrInvalidStatus     = errors.New("invalid status")
)

var allowedMethods = map[string]struct{}{
	"GET":     {},
	"POST":    {},
	"PUT":     {},
	"PATCH":   {},
	"DELETE":  {},
	"HEAD":    {},
	"OPTIONS": {},
}

func Parse(line string) (Record, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return Record{}, ErrEmptyLine
	}

	fields := strings.Fields(raw)
	if len(fields) != 5 {
		return Record{}, fmt.Errorf("%w: got %d", ErrInvalidFieldCount, len(fields))
	}

	timestamp, err := time.Parse(time.RFC3339, fields[0])
	if err != nil {
		return Record{}, fmt.Errorf("%w: %q", ErrInvalidTimestamp, fields[0])
	}

	ip, err := netip.ParseAddr(fields[1])
	if err != nil {
		return Record{}, fmt.Errorf("%w: %q", ErrInvalidIP, fields[1])
	}

	method := fields[2]
	if _, ok := allowedMethods[method]; !ok {
		return Record{}, fmt.Errorf("%w: %q", ErrInvalidMethod, method)
	}

	path := fields[3]
	if !strings.HasPrefix(path, "/") {
		return Record{}, fmt.Errorf("%w: %q", ErrInvalidPath, path)
	}

	status, err := strconv.Atoi(fields[4])
	if err != nil || status < 100 || status > 599 {
		return Record{}, fmt.Errorf("%w: %q", ErrInvalidStatus, fields[4])
	}

	return Record{
		Timestamp: timestamp,
		IP:        ip,
		Method:    method,
		Path:      path,
		Status:    status,
		Raw:       raw,
	}, nil
}

func IsAllowedMethod(method string) bool {
	_, ok := allowedMethods[method]
	return ok
}
