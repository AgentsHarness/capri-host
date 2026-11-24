package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
)

const (
	idemHeaderKey    = "Idempotency-Key"
	idemHeaderScope  = "Idempotency-Scope"
	idemHeaderReplay = "Idempotent-Replayed"
	idemScopeDirect  = "direct"

	idemKeyMaxLen      = 128
	idemScopeMaxLen    = 64
	idemMaxRequestBody = 32 << 20

	idemCodeInvalidKey         = "invalid_idempotency_key"
	idemCodeConflict           = "idempotency_key_conflict"
	idemCodeOutcomeUnknown     = "idempotency_outcome_unknown"
	idemCodeStoreUnavailable   = "idempotency_store_unavailable"
	idemCodeInProgress         = "idempotency_in_progress"
	idemCodeResultUnavailable  = "idempotency_result_unavailable"
	idemCodeInvalidRequestBody = "invalid_request_body"
	idemCodeRequestTooLarge    = "request_too_large"
)

// withIdempotency 在鉴权之后接管带 Idempotency-Key 的 POST：同一个
// (hostId, scope, key) 最多执行一次，重复请求回放第一次的终态响应。
// 无 key 的请求（GET、只读 POST、尚未接入的调用方）原样通过。
func (s *Server) withIdempotency(next http.Handler) http.Handler {
	if s.idem == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		key := strings.TrimSpace(r.Header.Get(idemHeaderKey))
		if key == "" {
			next.ServeHTTP(w, r)
			return
		}
		scope := strings.TrimSpace(r.Header.Get(idemHeaderScope))
		if scope == "" {
			scope = idemScopeDirect
		}
		if !validIdemToken(key, idemKeyMaxLen) {
			idemWriteError(w, http.StatusBadRequest, idemCodeInvalidKey, "Idempotency-Key 必须是 1..128 个可打印 ASCII 字符")
			return
		}
		if !validIdemToken(scope, idemScopeMaxLen) {
			idemWriteError(w, http.StatusBadRequest, idemCodeInvalidKey, "Idempotency-Scope 必须是 0..64 个可打印 ASCII 字符")
			return
		}

		var body []byte
		if r.Body != nil {
			var err error
			body, err = io.ReadAll(io.LimitReader(r.Body, idemMaxRequestBody+1))
			_ = r.Body.Close()
			if err != nil {
				idemWriteError(w, http.StatusBadRequest, idemCodeInvalidRequestBody, "读取请求体失败："+err.Error())
				return
			}
			if len(body) > idemMaxRequestBody {
				idemWriteError(w, http.StatusRequestEntityTooLarge, idemCodeRequestTooLarge, "请求体过大")
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fp := idemFingerprint(r, body)

		res := s.idem.begin(s.idem.hostID, scope, key, fp)
		switch res.lookup {
		case idemConflict:
			idemWriteError(w, http.StatusConflict, idemCodeConflict, "同一个 Idempotency-Key 对应不同的请求")
			return
		case idemOutcomeUnknown:
			idemWriteError(w, http.StatusConflict, idemCodeOutcomeUnknown, "上一次执行在完成前中断，结果未知；请确认后再决定是否重试")
			return
		case idemResultUnavailable:
			idemWriteError(w, http.StatusConflict, idemCodeResultUnavailable, "上一次的响应过大，无法回放；请换一个新的 Idempotency-Key")
			return
		case idemStoreUnavailable:
			idemWriteError(w, http.StatusServiceUnavailable, idemCodeStoreUnavailable, "幂等存储不可用，请求未执行")
			return
		case idemReplay:
			idemWriteReplay(w, res)
			return
		case idemWait:
			waited := s.idem.waitWithContext(r.Context().Done(), res.entry)
			switch waited.lookup {
			case idemReplay:
				idemWriteReplay(w, waited)
			case idemResultUnavailable:
				idemWriteError(w, http.StatusConflict, idemCodeResultUnavailable, "上一次的响应过大，无法回放；请换一个新的 Idempotency-Key")
			case idemOutcomeUnknown:
				idemWriteError(w, http.StatusConflict, idemCodeOutcomeUnknown, "上一次执行在完成前中断，结果未知")
			case idemWaitTimedOut:
				idemWriteError(w, http.StatusConflict, idemCodeInProgress, "同一 Idempotency-Key 的请求仍在执行，请稍后重试")
			case idemWaitCanceled:
				// 客户端已断开：原始执行继续，这里没有可写的接收方。
				// relayInProcess 会按请求 context 判定，不会把空响应兜底成 200。
			default:
				idemWriteError(w, http.StatusServiceUnavailable, idemCodeStoreUnavailable, "幂等存储不可用")
			}
			return
		}

		// 本次请求拥有执行权：缓冲响应，handler 返回后落终态，再回写。
		rw := newIdemResponseWriter(w, s.idem.maxResponseBytes)
		func() {
			defer func() {
				if p := recover(); p != nil {
					// handler 已经开始执行，结果不可知：留下 unknown 再让
					// relayInProcess / net/http 的 recover 返回 500。
					s.idem.markUnknown(res.entry)
					panic(p)
				}
			}()
			next.ServeHTTP(rw, r)
		}()
		status, respBody, ctype, replayable := rw.result()
		s.idem.finish(res.entry, status, respBody, ctype, replayable)
		rw.flush()
	})
}

// validIdemToken 与 wire.ValidateIdemKey/ValidateIdemScope 同一字符集：
// 只允许可打印 ASCII，长度上限按 header 区分。
func validIdemToken(v string, max int) bool {
	if len(v) == 0 || len(v) > max {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

// idemFingerprint 是 method + RequestURI + body 的 sha256。进程内中继的
// 请求是 client-style http.Request（RequestURI 为空），退回 URL。
func idemFingerprint(r *http.Request, body []byte) string {
	uri := r.RequestURI
	if uri == "" && r.URL != nil {
		uri = r.URL.RequestURI()
	}
	h := sha256.New()
	_, _ = h.Write([]byte(r.Method))
	_, _ = h.Write([]byte("\n"))
	_, _ = h.Write([]byte(uri))
	_, _ = h.Write([]byte("\n"))
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func idemWriteReplay(w http.ResponseWriter, res idemResult) {
	if res.ctype != "" {
		w.Header().Set("Content-Type", res.ctype)
	}
	w.Header().Set(idemHeaderReplay, "true")
	status := res.status
	if status < 100 || status > 599 {
		// A corrupted journal must not panic net/http with WriteHeader(0).
		status = http.StatusBadGateway
	}
	w.WriteHeader(status)
	if len(res.body) > 0 {
		_, _ = w.Write(res.body)
	}
}

func idemWriteError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": msg,
		"code":  code,
	})
}

// idemResponseWriter 缓存业务响应（最多 limit 字节）并在超限后转为
// 直通：超限响应标记为不可回放，但客户端仍然收到完整内容。
type idemResponseWriter struct {
	w      http.ResponseWriter
	status int
	buf    bytes.Buffer
	limit  int
	// flushed 是 buf 中已经写给底层 writer 的字节数（Flush / 超限时）。
	flushed     int
	overflow    bool
	wroteHeader bool
}

func newIdemResponseWriter(w http.ResponseWriter, limit int) *idemResponseWriter {
	if limit <= 0 {
		limit = idemMaxResponseBytes
	}
	return &idemResponseWriter{w: w, limit: limit}
}

func (rw *idemResponseWriter) Header() http.Header { return rw.w.Header() }

func (rw *idemResponseWriter) WriteHeader(code int) {
	if rw.status != 0 {
		return
	}
	rw.status = code
	if rw.overflow {
		rw.writeHeader()
	}
}

func (rw *idemResponseWriter) Write(p []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	if rw.overflow {
		rw.writeHeader()
		return rw.w.Write(p)
	}
	if rw.buf.Len()+len(p) > rw.limit {
		rw.overflow = true
		rw.writeHeader()
		rw.flushBuffer()
		return rw.w.Write(p)
	}
	return rw.buf.Write(p)
}

func (rw *idemResponseWriter) Flush() {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	rw.writeHeader()
	rw.flushBuffer()
	if f, ok := rw.w.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 让 http.ResponseController 能触达底层 writer。
func (rw *idemResponseWriter) Unwrap() http.ResponseWriter { return rw.w }

func (rw *idemResponseWriter) writeHeader() {
	if rw.wroteHeader {
		return
	}
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	rw.w.WriteHeader(rw.status)
	rw.wroteHeader = true
}

func (rw *idemResponseWriter) flushBuffer() {
	if rw.flushed >= rw.buf.Len() {
		return
	}
	_, _ = rw.w.Write(rw.buf.Bytes()[rw.flushed:])
	rw.flushed = rw.buf.Len()
}

// result 返回缓存的状态/响应体/Content-Type；超限响应不返回 body
// （要回放它没有意义，Store 会标记 done_unreplayable）。
func (rw *idemResponseWriter) result() (status int, body []byte, ctype string, replayable bool) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	ctype = rw.w.Header().Get("Content-Type")
	if rw.overflow {
		return rw.status, nil, ctype, false
	}
	body = append([]byte(nil), rw.buf.Bytes()...)
	return rw.status, body, ctype, true
}

// flush 把缓存中尚未写出的部分写给客户端。
func (rw *idemResponseWriter) flush() {
	rw.writeHeader()
	rw.flushBuffer()
}
