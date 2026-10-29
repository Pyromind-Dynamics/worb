package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestEntityFromAPIKeyKeepsRawKey 确认 40 位 key 会原样成为 entity。
// TestEntityFromAPIKeyKeepsRawKey checks that a 40-character key is stored as the entity.
func TestEntityFromAPIKeyKeepsRawKey(t *testing.T) {
	// Agent: 这把 key 长度正好 40，旧逻辑会得到 local。
	key := "JDJiJDEyJHpHaGNGUlEzUWJsS2ZmOEtzbmo4Q2U="
	if got := entityFromAPIKey(key); got != key {
		t.Fatalf("entity = %q, want raw key", got)
	}
	// Agent: 空 key 无法区分用户。
	if got := entityFromAPIKey(""); got != "local" {
		t.Fatalf("empty key entity = %q, want local", got)
	}
}

// TestMiddlewareUsesAuthorizationKey 确认中间件把 Authorization 里的 key 放进 context。
// TestMiddlewareUsesAuthorizationKey checks that middleware copies the Authorization key into the context.
func TestMiddlewareUsesAuthorizationKey(t *testing.T) {
	key := "JDJiJDEyJHpHaGNGUlEzUWJsS2ZmOEtzbmo4Q2U="
	var got string
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Agent: 下游只读 context，不再解析 key。
		got = EntityFromContext(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got != key {
		t.Fatalf("context entity = %q, want raw key", got)
	}

	got = ""
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	// Agent: 没有 Authorization 时保持 local。
	if got != "local" {
		t.Fatalf("missing auth entity = %q, want local", got)
	}
}
