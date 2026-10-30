package hub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func drainReqCh(t *testing.T, c *Client) []reqFrame {
	t.Helper()
	var out []reqFrame
	for {
		select {
		case f := <-c.reqCh:
			out = append(out, f)
		default:
			return out
		}
	}
}

// A body over relayChunkSize must go out as several respond frames whose
// base64 chunks concatenate back to the exact original bytes, with `more`
// set on every frame but the last and `final` (the QUIC per-request stream
// close) only on the last.
func TestRespondChunksLargeBody(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.reqCh = make(chan reqFrame, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)

	body := json.RawMessage(`{"text":"` + strings.Repeat("a", 2<<20+512<<10) + `"}`)
	c.respond("r1", 200, body)

	frames := drainReqCh(t, c)
	wantFrames := (len(body) + relayChunkSize - 1) / relayChunkSize
	if wantFrames < 3 {
		t.Fatalf("test body spans %d chunks, want at least 3", wantFrames)
	}
	if len(frames) != wantFrames {
		t.Fatalf("frames = %d, want %d", len(frames), wantFrames)
	}

	var got []byte
	for i, f := range frames {
		if f.reqID != "r1" {
			t.Fatalf("frame %d reqId = %q", i, f.reqID)
		}
		if want := i == len(frames)-1; f.final != want {
			t.Fatalf("frame %d final = %v, want %v", i, f.final, want)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(f.payload, &raw); err != nil {
			t.Fatalf("frame %d is not JSON: %v", i, err)
		}
		if _, ok := raw["chunk"]; !ok {
			t.Fatalf("frame %d has no chunk: %s", i, f.payload)
		}
		if _, ok := raw["body"]; ok {
			t.Fatalf("chunk frame %d also carried body", i)
		}
		if _, ok := raw["more"]; !ok {
			t.Fatalf("frame %d has no more flag", i)
		}
		var fr struct {
			Type     string `json:"type"`
			ReqID    string `json:"reqId"`
			Status   int    `json:"status"`
			Encoding string `json:"encoding"`
			More     bool   `json:"more"`
			Chunk    string `json:"chunk"`
		}
		if err := json.Unmarshal(f.payload, &fr); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if fr.Type != "respond" || fr.ReqID != "r1" || fr.Status != 200 {
			t.Fatalf("frame %d header = %+v", i, fr)
		}
		if fr.Encoding != "base64" {
			t.Fatalf("frame %d encoding = %q", i, fr.Encoding)
		}
		if want := i < len(frames)-1; fr.More != want {
			t.Fatalf("frame %d more = %v, want %v", i, fr.More, want)
		}
		dec, err := base64.StdEncoding.DecodeString(fr.Chunk)
		if err != nil {
			t.Fatalf("frame %d chunk is not base64: %v", i, err)
		}
		if len(dec) > relayChunkSize {
			t.Fatalf("frame %d carries %d bytes, over relayChunkSize %d", i, len(dec), relayChunkSize)
		}
		got = append(got, dec...)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d bytes, want the original %d", len(got), len(body))
	}
}

// The chunk boundary is inclusive-small: a body of exactly relayChunkSize
// still travels as one frame, and small bodies keep the original `body`
// shape with no chunk fields.
func TestRespondSmallBodyStaysSingleFrame(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.reqCh = make(chan reqFrame, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)

	boundary := json.RawMessage(`{"x":"` + strings.Repeat("b", relayChunkSize-8) + `"}`)
	if len(boundary) != relayChunkSize {
		t.Fatalf("boundary body = %d bytes, want %d", len(boundary), relayChunkSize)
	}
	c.respond("r2", 201, boundary)
	c.respond("r3", 202, json.RawMessage(`{"ok":true}`))

	frames := drainReqCh(t, c)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2 single-frame answers", len(frames))
	}
	for i, wantStatus := range []int{201, 202} {
		var fr struct {
			Type   string          `json:"type"`
			ReqID  string          `json:"reqId"`
			Status int             `json:"status"`
			Body   json.RawMessage `json:"body"`
			Chunk  string          `json:"chunk"`
		}
		if err := json.Unmarshal(frames[i].payload, &fr); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if fr.Type != "respond" || fr.Status != wantStatus {
			t.Fatalf("frame %d = %+v, want status %d", i, fr, wantStatus)
		}
		if len(fr.Body) == 0 || fr.Chunk != "" {
			t.Fatalf("frame %d is not the single-frame shape: %s", i, frames[i].payload)
		}
		if !frames[i].final {
			t.Fatalf("frame %d final = false, want true", i)
		}
	}
}
