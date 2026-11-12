package auth

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"sync"
)

type contextKey string

const entityKey contextKey = "entity"

const (
	bodyInvalidKey    = "invalid key"
	bodyInternalError = "internal error"
)

// tokenLookup 用 wandb key 换内部 user_id。status 为 0 表示成功，否则是要回给客户端的 HTTP 状态。
// tokenLookup exchanges a wandb key for the internal user_id. status 0 means success; any other value is the HTTP status for the client.
type tokenLookup interface {
	Lookup(ctx context.Context, token string) (userID string, status int)
}

var (
	lookupMu      sync.RWMutex
	currentLookup tokenLookup
)

// SetLookup 注入按 token 换 user_id 的实现。启动时放真实客户端，测试时放假实现。
// SetLookup installs the token-to-user_id lookup. Startup installs the real client; tests install a fake.
func SetLookup(l tokenLookup) {
	lookupMu.Lock()
	// Agent: 全进程共用一个查找器。
	currentLookup = l
	lookupMu.Unlock()
}

// activeLookup 读取当前进程的查找器。
// activeLookup returns the process-wide lookup.
func activeLookup() tokenLookup {
	lookupMu.RLock()
	defer lookupMu.RUnlock()
	return currentLookup
}

// extractAPIKey 从 Authorization 头取出 wandb key。
// extractAPIKey reads the wandb API key from an Authorization header.
func extractAPIKey(header string) string {
	header = strings.TrimPrefix(header, "Bearer ")
	if strings.HasPrefix(header, "Basic ") {
		encoded := strings.TrimPrefix(header, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return encoded
		}
		if _, after, ok := strings.Cut(string(decoded), ":"); ok {
			return after
		}
		return string(decoded)
	}
	return header
}

// Middleware 把本次请求的 user_id 写入 context。没有 key 时 entity 为 local，不调用外部服务。
// Middleware stores this request's user_id in the context. An empty key stays local and does not call out.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Agent: /health 只给启动探针用，不解析 Authorization，也不调用 gRPC。
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		header := r.Header.Get("Authorization")
		// Agent: 没有 Authorization 时不发起认证，归属保持 local。
		if header == "" {
			ctx := context.WithValue(r.Context(), entityKey, "local")
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		apiKey := extractAPIKey(header)
		// Agent: 解析出的 key 为空时同样不调用，归属保持 local。
		if apiKey == "" {
			ctx := context.WithValue(r.Context(), entityKey, "local")
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		lookup := activeLookup()
		// Agent: 有 key 但没有客户端时不能退回 key 本身，按内部错误结束。
		if lookup == nil {
			writeAuthFailure(w, http.StatusInternalServerError)
			return
		}
		// Agent: 一次请求只在这里换 user_id，后续 handler 只读 context。
		userID, status := lookup.Lookup(r.Context(), apiKey)
		if status != 0 {
			writeAuthFailure(w, status)
			return
		}
		// Agent: 成功结果必须是非空 user_id，空串不能写成 entity。
		if userID == "" {
			writeAuthFailure(w, http.StatusInternalServerError)
			return
		}
		ctx := context.WithValue(r.Context(), entityKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// writeAuthFailure 用英文纯文本结束本次请求，不调用后续 handler。
// writeAuthFailure ends the request with English plain text and does not call the next handler.
func writeAuthFailure(w http.ResponseWriter, status int) {
	body := bodyInternalError
	// Agent: 只有 401 使用 invalid key，其它失败都按内部错误返回，且正文不带换行。
	if status == http.StatusUnauthorized {
		body = bodyInvalidKey
	} else {
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// EntityFromContext 读取当前请求的 entity，即 user_id 的十进制字符串。
// EntityFromContext returns the entity for this request, the decimal user_id string.
func EntityFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(entityKey).(string); ok {
		return v
	}
	return "local"
}
