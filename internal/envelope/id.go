package envelope

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

// NewID returns a UUIDv7 string: time-ordered (48-bit unix-ms prefix),
// collision-safe (74 random bits), sortable across processes.
// Locked by decision "One versioned envelope, …": ULID/UUIDv7 ids, never
// Date.now()+random (audit Avoid #10).
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; if it does, the
		// process has no usable entropy and must not mint ids.
		panic(fmt.Sprintf("envelope: crypto/rand failed: %v", err))
	}

	ms := uint64(time.Now().UnixMilli())
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms)
	copy(b[0:6], ts[2:8]) // 48-bit big-endian millisecond timestamp

	b[6] = (b[6] & 0x0F) | 0x70 // version 7
	b[8] = (b[8] & 0x3F) | 0x80 // RFC 4122 variant

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
