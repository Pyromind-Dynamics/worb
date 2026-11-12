package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sarna/worb/internal/userpb"
	"google.golang.org/grpc"
)

// fakeLookup 在测试里代替 gRPC，记录被查询的 token。
// fakeLookup stands in for gRPC in tests and records the token it was given.
type fakeLookup struct {
	userID string
	status int
	calls  int
	token  string
}

// Lookup 返回预先写好的 user_id 或状态码。
// Lookup returns the prepared user_id or status code.
func (f *fakeLookup) Lookup(ctx context.Context, token string) (string, int) {
	f.calls++
	f.token = token
	return f.userID, f.status
}

// useLookup 在本测试期间换上查找器，结束时还原。
// useLookup installs a lookup for this test and restores the previous one.
func useLookup(t *testing.T, l tokenLookup) {
	t.Helper()
	prev := activeLookup()
	SetLookup(l)
	t.Cleanup(func() { SetLookup(prev) })
}

// fakeQuery 代替生成的 gRPC 客户端，不拨号。
// fakeQuery replaces the generated gRPC client and does not dial.
type fakeQuery struct {
	calls int
	token string
	kind  string
	resp  *userpb.UserInfoResponse
	err   error
}

// QueryUserByToken 返回准备好的回包，并记下请求里的 token 与 token_type。
// QueryUserByToken returns the prepared response and records the request token and token_type.
func (f *fakeQuery) QueryUserByToken(ctx context.Context, in *userpb.QueryUserByTokenRequest, opts ...grpc.CallOption) (*userpb.UserInfoResponse, error) {
	f.calls++
	f.token = in.GetToken()
	f.kind = in.GetTokenType()
	return f.resp, f.err
}

// okResponse 构造 success 为 true、error_code 仍为 500 的成功回包。
// okResponse builds a success response whose error_code remains 500.
func okResponse(id int64) *userpb.UserInfoResponse {
	return &userpb.UserInfoResponse{
		Base:            &userpb.GrpcResponse{Success: true, ErrorCode: 500, Message: "ignored"},
		UserClusterInfo: &userpb.UserClusterInfo{Id: id, Uid: 1000000359, Status: "normal"},
		TokenType:       "worb",
	}
}

// TestMiddlewareUsesUserID 确认有 key 时 context 里是 user_id，没有 Authorization 时是 local。
// TestMiddlewareUsesUserID checks that a key becomes the user_id and a missing Authorization stays local.
func TestMiddlewareUsesUserID(t *testing.T) {
	key := "JDJiJDEyJHpHaGNGUlEzUWJsS2ZmOEtzbmo4Q2U="
	fake := &fakeLookup{userID: "10000003"}
	useLookup(t, fake)
	var got string
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = EntityFromContext(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got != "10000003" || fake.calls != 1 || fake.token != key {
		t.Fatal("lookup did not produce the user id")
	}

	fake.calls = 0
	got = ""
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got != "local" || fake.calls != 0 {
		t.Fatalf("missing authorization entity = %q calls = %d", got, fake.calls)
	}
}

// TestMiddlewareEmptyKeyStaysLocal 确认空 key 不调用查找，Basic 密码会作为 token。
// TestMiddlewareEmptyKeyStaysLocal checks that an empty key skips lookup and a Basic password is the token.
func TestMiddlewareEmptyKeyStaysLocal(t *testing.T) {
	fake := &fakeLookup{userID: "10000003"}
	useLookup(t, fake)
	var got string
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = EntityFromContext(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got != "local" || fake.calls != 0 {
		t.Fatalf("empty bearer entity = %q calls = %d", got, fake.calls)
	}

	encoded := base64.StdEncoding.EncodeToString([]byte("api:" + "JDJiJDEyJHpHaGNGUlEzUWJsS2ZmOEtzbmo4Q2U="))
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic "+encoded)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if got != "10000003" || fake.calls != 1 {
		t.Fatalf("basic entity = %q calls = %d", got, fake.calls)
	}
}

// TestMiddlewareStopsOnAuthFailure 确认 401 与 500 都停止后续 handler，正文是英文纯文本。
// TestMiddlewareStopsOnAuthFailure checks that 401 and 500 stop the next handler with English plain text.
func TestMiddlewareStopsOnAuthFailure(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		code   int
	}{
		{name: "invalid", status: 401, body: "invalid key", code: http.StatusUnauthorized},
		{name: "internal", status: 500, body: "internal error", code: http.StatusInternalServerError},
		{name: "other", status: 403, body: "internal error", code: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useLookup(t, &fakeLookup{status: tc.status})
			called := false
			handler := Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				called = true
			}))
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", "Bearer key")
			handler.ServeHTTP(rec, req)
			if called {
				t.Fatal("next handler ran")
			}
			if rec.Code != tc.code || rec.Body.String() != tc.body {
				t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
				t.Fatalf("content type %q", rec.Header().Get("Content-Type"))
			}
		})
	}
}

// TestMiddlewareWithoutLookup 确认有 key 但没有客户端时回 500，不把 key 写成 entity。
// TestMiddlewareWithoutLookup checks that a key without a client returns 500 and is not stored as the entity.
func TestMiddlewareWithoutLookup(t *testing.T) {
	useLookup(t, nil)
	called := false
	handler := Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer key")
	handler.ServeHTTP(rec, req)
	if called || rec.Code != http.StatusInternalServerError || rec.Body.String() != "internal error" {
		t.Fatalf("status %d body %q called %v", rec.Code, rec.Body.String(), called)
	}
}

// TestHealthSkipsLookup 确认 /health 即使带了 key 也不调用 gRPC。
// TestHealthSkipsLookup checks that /health does not call gRPC even when a key is present.
func TestHealthSkipsLookup(t *testing.T) {
	fake := &fakeLookup{status: http.StatusInternalServerError}
	useLookup(t, fake)
	called := false
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer key")
	handler.ServeHTTP(rec, req)
	if !called || fake.calls != 0 || rec.Code != http.StatusNoContent {
		t.Fatalf("status %d calls %d called %v", rec.Code, fake.calls, called)
	}
}

// TestLoadUserGRPCConfig 确认地址缺失、证书开关和读不到 CA 时的启动规则。
// TestLoadUserGRPCConfig checks startup rules for a missing address, the cert switch, and an unreadable CA.
func TestLoadUserGRPCConfig(t *testing.T) {
	if _, err := loadUserGRPCConfig(func(string) string { return "" }); err == nil {
		t.Fatal("missing address started")
	}
	if _, err := loadUserGRPCConfig(func(k string) string {
		if k == envAddr {
			return "   "
		}
		return ""
	}); err == nil {
		t.Fatal("blank address started")
	}

	cfg, err := loadUserGRPCConfig(func(k string) string {
		switch k {
		case envAddr:
			return " user.internal:443 "
		case envCertEnabled:
			return "TRUE"
		case envCertPath:
			return "/no/such/ca.pem"
		case envCacheTTL:
			return "nope"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != "user.internal:443" || cfg.ttl != defaultCacheTTL {
		t.Fatalf("addr %q ttl %s", cfg.addr, cfg.ttl)
	}
	if cfg.creds.Info().SecurityProtocol != "insecure" {
		t.Fatalf("protocol %q", cfg.creds.Info().SecurityProtocol)
	}

	if _, err := loadUserGRPCConfig(func(k string) string {
		if k == envAddr {
			return "h:1"
		}
		if k == envCertEnabled {
			return "true"
		}
		return "   "
	}); err == nil {
		t.Fatal("empty cert path started")
	}
	if _, err := loadUserGRPCConfig(func(k string) string {
		if k == envAddr {
			return "h:1"
		}
		if k == envCertEnabled {
			return "on"
		}
		if k == envCertPath {
			return "/no/such/ca.pem"
		}
		return ""
	}); err == nil {
		t.Fatal("missing cert file started")
	}
}

// TestParseCacheTTL 确认非法缓存时间用 30 分钟，整数秒按原值使用。
// TestParseCacheTTL checks that a non-integer cache TTL uses 30 minutes and an integer is used as seconds.
func TestParseCacheTTL(t *testing.T) {
	if parseCacheTTL("") != defaultCacheTTL || parseCacheTTL("  ") != defaultCacheTTL || parseCacheTTL("abc") != defaultCacheTTL {
		t.Fatal("bad ttl did not use 30 minutes")
	}
	if parseCacheTTL("1800") != 1800*time.Second || parseCacheTTL("1800\n") != 1800*time.Second {
		t.Fatal("1800 seconds")
	}
	if parseCacheTTL("0") != 0 || parseCacheTTL("-5") != -5*time.Second {
		t.Fatal("integer seconds were replaced")
	}
}

// TestUserIDFromResponse 确认 success、空对象、token_type、status 和 id 的判定。
// TestUserIDFromResponse checks the success, empty object, token_type, status, and id decisions.
func TestUserIDFromResponse(t *testing.T) {
	id, status := userIDFromResponse(okResponse(10000003), "secret")
	if id != "10000003" || status != 0 {
		t.Fatalf("id %q status %d", id, status)
	}
	id, status = userIDFromResponse(okResponse(8), "secret")
	if id != "8" || status != 0 {
		t.Fatalf("id %q status %d", id, status)
	}

	_, status = userIDFromResponse(&userpb.UserInfoResponse{}, "secret")
	if status != 500 {
		t.Fatalf("missing base status %d", status)
	}
	_, status = userIDFromResponse(&userpb.UserInfoResponse{
		Base: &userpb.GrpcResponse{Success: false, ErrorCode: 7, Message: "no secret here"},
	}, "secret")
	if status != 500 {
		t.Fatalf("success false status %d", status)
	}

	bad := okResponse(10000003)
	bad.UserClusterInfo = nil
	_, status = userIDFromResponse(bad, "secret")
	if status != 401 {
		t.Fatalf("empty info status %d", status)
	}
	bad = okResponse(10000003)
	bad.TokenType = "other"
	_, status = userIDFromResponse(bad, "secret")
	if status != 401 {
		t.Fatalf("token type status %d", status)
	}
	bad = okResponse(10000003)
	bad.UserClusterInfo.Status = "disabled"
	_, status = userIDFromResponse(bad, "secret")
	if status != 401 {
		t.Fatalf("status field %d", status)
	}
	bad = okResponse(0)
	_, status = userIDFromResponse(bad, "secret")
	if status != 401 {
		t.Fatalf("zero id status %d", status)
	}
	bad = okResponse(-1)
	_, status = userIDFromResponse(bad, "secret")
	if status != 401 {
		t.Fatalf("negative id status %d", status)
	}
}

// TestRedactToken 确认错误文本里的 wandb key 会被替换。
// TestRedactToken checks that a wandb key inside an error string is replaced.
func TestRedactToken(t *testing.T) {
	if got := redactToken("token secret leaked", "secret"); got != "token [redacted] leaked" {
		t.Fatalf("redacted %q", got)
	}
	if got := redactToken("plain", "secret"); got != "plain" {
		t.Fatalf("kept %q", got)
	}
}

// TestProbeUserService 确认启动检查不带 key，有回包即通过，传输错误则失败。
// TestProbeUserService checks that the startup probe sends no key, passes on any response, and fails on a transport error.
func TestProbeUserService(t *testing.T) {
	ok := &fakeQuery{resp: &userpb.UserInfoResponse{Base: &userpb.GrpcResponse{Success: false, ErrorCode: 7, Message: "no token"}}}
	if err := probeUserService(context.Background(), ok); err != nil {
		t.Fatal(err)
	}
	// Agent: 连通检查不携带 wandb key。
	if ok.token != "" || ok.kind != "worb_sk" {
		t.Fatal("startup probe carried a key or the wrong token type")
	}
	if err := probeUserService(context.Background(), &fakeQuery{err: errors.New("dial")}); err == nil {
		t.Fatal("transport error passed the startup check")
	}
	if err := probeUserService(context.Background(), &fakeQuery{}); err == nil {
		t.Fatal("empty response passed the startup check")
	}
}

// TestLookupCache 确认命中缓存直接返回，失败和过期都会重新调用。
// TestLookupCache checks that a hit returns directly, while a failure or expiry calls again.
func TestLookupCache(t *testing.T) {
	q := &fakeQuery{resp: okResponse(10000003)}
	now := time.Unix(1_700_000_000, 0)
	client := &userClient{
		rpc:   q,
		cache: newIDCache(time.Second),
		now:   func() time.Time { return now },
	}
	id, status := client.Lookup(context.Background(), "secret")
	if id != "10000003" || status != 0 || q.calls != 1 || q.kind != "worb_sk" || q.token != "secret" {
		t.Fatalf("id %q status %d calls %d kind %q", id, status, q.calls, q.kind)
	}
	id, status = client.Lookup(context.Background(), "secret")
	if id != "10000003" || status != 0 || q.calls != 1 {
		t.Fatalf("cache miss id %q calls %d", id, q.calls)
	}
	now = now.Add(time.Second)
	_, _ = client.Lookup(context.Background(), "secret")
	if q.calls != 2 {
		t.Fatalf("expired cache calls %d", q.calls)
	}

	q.err = errors.New("dial failed")
	q.resp = nil
	_, status = client.Lookup(context.Background(), "other")
	if status != 500 {
		t.Fatalf("transport status %d", status)
	}
	calls := q.calls
	_, status = client.Lookup(context.Background(), "other")
	if status != 500 || q.calls != calls+1 {
		t.Fatalf("failure was cached calls %d status %d", q.calls, status)
	}

	q.err = nil
	q.resp = okResponse(10000003)
	q.resp.TokenType = ""
	_, status = client.Lookup(context.Background(), "third")
	if status != 401 {
		t.Fatalf("bad token type status %d", status)
	}
	calls = q.calls
	_, _ = client.Lookup(context.Background(), "third")
	if q.calls != calls+1 {
		t.Fatal("401 was cached")
	}
}
