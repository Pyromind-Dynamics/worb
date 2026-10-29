package auth

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
)

type contextKey string

const entityKey contextKey = "entity"

// entityFromAPIKey 将 wandb key 原样作为 entity，用来区分用户数据归属。
// entityFromAPIKey uses the wandb API key unchanged as the entity that owns collected data.
// Agent: 去掉「不超过 40 位则返回 local」和「只保留第 40 位之后最多 128 字符」的截取。
func entityFromAPIKey(apiKey string) string {
	// Agent: 空 key 无法区分用户，归入 local。
	if apiKey == "" {
		return "local"
	}
	// Agent: 返回 key 原文，不再按 "-" 或固定长度裁切。
	return apiKey
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

// Middleware 把本次请求的 wandb key 写入 context，供后续采集接口当作 entity。
// Middleware stores the request wandb key in the context for later use as the entity.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Agent: 没有 Authorization 时归属保持 local。
		entity := "local"
		if header := r.Header.Get("Authorization"); header != "" {
			apiKey := extractAPIKey(header)
			// Agent: entity 等于 key 原文。
			entity = entityFromAPIKey(apiKey)
		}
		ctx := context.WithValue(r.Context(), entityKey, entity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// EntityFromContext 读取当前请求的 entity（wandb key 原文）。
// EntityFromContext returns the entity for this request, which is the raw wandb key.
func EntityFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(entityKey).(string); ok {
		return v
	}
	return "local"
}
