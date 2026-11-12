package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandleHealth 确认 /health 返回 200 和固定正文。
// TestHandleHealth checks that /health returns 200 and the fixed body.
func TestHandleHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	(&Server{}).handleHealth(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatalf("content type %q", rec.Header().Get("Content-Type"))
	}
}
