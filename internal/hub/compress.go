package hub

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"
)

// ── uplink compression (host → hub) ───────────────────────────────────

// minCompressSize is the payload floor below which a frame is sent raw:
// flate headers + small-input overhead usually make tiny frames BIGGER, and
// the hub→FE path uses the same threshold.
const minCompressSize = 256

// deflateFlagQUIC is OR'd into the 4-byte big-endian length prefix on the
// QUIC transport to mark a flate-compressed payload (bit 31; the effective
// frame cap stays far below 2^31 so the flag bit is always free).
const deflateFlagQUIC = 1 << 31

var flateWriterPool = sync.Pool{
	// NewWriter never fails for a valid level.
	New: func() any {
		w, _ := flate.NewWriter(nil, flate.DefaultCompression)
		return w
	},
}

var flateBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// deflatePayload flate-compresses payload (RFC 1951 raw deflate, matching
// the hub→FE writer). Returns (nil, false) when compression is not worth it
// (too small, or no gain) — the caller then sends the raw JSON.
func deflatePayload(payload []byte) ([]byte, bool) {
	if len(payload) < minCompressSize {
		return nil, false
	}
	buf := flateBufPool.Get().(*bytes.Buffer)
	w := flateWriterPool.Get().(*flate.Writer)
	defer func() {
		w.Reset(nil)
		flateWriterPool.Put(w)
	}()
	buf.Reset()
	defer flateBufPool.Put(buf)
	w.Reset(buf)
	if _, err := w.Write(payload); err != nil {
		return nil, false
	}
	if err := w.Close(); err != nil {
		return nil, false
	}
	if buf.Len() >= len(payload) {
		return nil, false
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, true
}

// inflatePayload reverses deflatePayload for a downlink frame.
func inflatePayload(payload []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(payload))
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, 32<<20))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// encodeFrameLen writes one QUIC length prefix, flagging a compressed body.
func frameLengthPrefix(n int, compressed bool) [4]byte {
	var buf [4]byte
	v := uint32(n)
	if compressed {
		v |= deflateFlagQUIC
	}
	binary.BigEndian.PutUint32(buf[:], v)
	return buf
}
