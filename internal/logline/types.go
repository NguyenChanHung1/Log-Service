package logline

import (
	"net/netip"
	"time"
)

type Record struct {
	Timestamp time.Time
	IP        netip.Addr
	Method    string
	Path      string
	Status    int
	Raw       string
}
