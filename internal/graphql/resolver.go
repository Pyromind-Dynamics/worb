package graphql

import (
	"context"

	"github.com/sarna/worb/internal/store"
)

type Resolver struct {
	Store *store.DB
	// Agent: 由 server 注入。有 object_key 时用它生成 24 小时 directUrl。
	Sign func(ctx context.Context, objectKey string) (string, error)
}
