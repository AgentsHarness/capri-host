package server

import (
	"log"
	"net/http"
	"time"
)

// requestLogWriter captures the status of one local relayed handler call.
// Unwrap keeps http.ResponseController working; Flush keeps SSE-style
// handlers working when they are reached through this wrapper.
type requestLogWriter struct {
	http.ResponseWriter
	status int
}

func (w *requestLogWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *requestLogWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *requestLogWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *requestLogWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// withRequestLog emits one [relay] line for a local handler invoked on behalf
// of a hub relay request. Requests without X-Capri-Request-ID (the normal
// browser/local path) pass through untouched.
func (s *Server) withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Capri-Request-ID")
		if reqID == "" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &requestLogWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
		if rw.status == 0 {
			rw.status = http.StatusOK
		}
		log.Printf("[relay] req=%s server %s %s status=%d duration_ms=%d", reqID, r.Method, r.URL.Path, rw.status, time.Since(start).Milliseconds())
	})
}
