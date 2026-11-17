package auth

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sarna/worb/internal/logerr"
	"github.com/sarna/worb/internal/userpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	envAddr        = "USER_GRPC_ADDR"
	envCertEnabled = "USER_GRPC_CERT_ENABLED"
	envCertPath    = "USER_GRPC_CERT_PATH"
	envCacheTTL    = "USER_GRPC_CACHE_TTL"

	defaultCacheTTL = 30 * time.Minute
	rpcTimeout      = 30 * time.Second
	// Agent: 请求里的 token_type 固定为 worb_sk。
	tokenTypeRequest = "worb_sk"
	tokenTypeWorb    = "worb"
	statusNormal     = "normal"
)

// queryAPI 是生成客户端上这一次 RPC 的方法，测试可以替换。
// queryAPI is the generated RPC used for this call. Tests can substitute it.
type queryAPI interface {
	QueryUserByToken(ctx context.Context, in *userpb.QueryUserByTokenRequest, opts ...grpc.CallOption) (*userpb.UserInfoResponse, error)
}

type cacheEntry struct {
	userID  string
	expires time.Time
}

// idCache 按 wandb key 缓存成功得到的 user_id。过期从写入时起算，失败不写入。
// idCache stores a successful user_id under the wandb key. Expiry starts at write time. Failures are not stored.
type idCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]cacheEntry
}

// newIDCache 建立进程内缓存。ttl 为 0 或负数时，条目在写入后立即视为过期。
// newIDCache builds the in-process cache. A zero or negative ttl expires an entry as soon as it is written.
func newIDCache(ttl time.Duration) *idCache {
	return &idCache{
		ttl: ttl,
		m:   make(map[string]cacheEntry),
	}
}

// get 返回未过期的 user_id。命中不延长过期时间。
// get returns an unexpired user_id. A hit does not extend the expiry.
func (c *idCache) get(token string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[token]
	// Agent: 到期条目删掉，下一次请求重新调用。
	if !ok || !now.Before(e.expires) {
		if ok {
			delete(c.m, token)
		}
		return "", false
	}
	return e.userID, true
}

// put 只在认证通过后写入。键是 wandb key，调用方不得把键打进日志。
// put stores a user_id only after authentication succeeds. The key is the wandb key and must not be logged.
func (c *idCache) put(token, userID string, now time.Time) {
	c.mu.Lock()
	// Agent: 覆盖同一把 key 的旧值，过期时间从这次写入起算。
	c.m[token] = cacheEntry{userID: userID, expires: now.Add(c.ttl)}
	c.mu.Unlock()
}

// userClient 复用一条 gRPC 连接，并在缓存未命中时查询 user_id。
// userClient reuses one gRPC connection and queries the user_id on a cache miss.
type userClient struct {
	rpc   queryAPI
	cache *idCache
	now   func() time.Time
}

// OpenFromEnv 在启动时读取地址、证书和缓存时间，并拨出一条长连接。
// OpenFromEnv reads the address, certificate, and cache TTL at startup and opens one long-lived connection.
func OpenFromEnv() error {
	cfg, err := loadUserGRPCConfig(os.Getenv)
	if err != nil {
		// Agent: 地址或证书不满足时没有发起调用，单独记下没有校验。
		logerr.Errorf("query_user_by_token startup check skipped: %v", err)
		return err
	}
	// Agent: 启动时建一条连接，之后的请求复用它。
	conn, err := grpc.NewClient(cfg.addr, grpc.WithTransportCredentials(cfg.creds))
	if err != nil {
		// Agent: 连接对象没有建成，调用没有发出，也记为没有校验。
		logerr.Errorf("query_user_by_token startup check skipped: %v", err)
		return err
	}
	rpc := userpb.NewUserClusterServiceClient(conn)
	// Agent: 建连成功不算通过，必须有一次调用到达服务。
	if err := probeUserService(context.Background(), rpc); err != nil {
		conn.Close()
		return err
	}
	SetLookup(&userClient{
		rpc:   rpc,
		cache: newIDCache(cfg.ttl),
		now:   time.Now,
	})
	return nil
}

// probeUserService 用不带 wandb key 的一次调用确认 gRPC 能通。有回包即通过。
// probeUserService confirms gRPC with one call that carries no wandb key. Any response passes.
func probeUserService(ctx context.Context, rpc queryAPI) error {
	callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := rpc.QueryUserByToken(callCtx, &userpb.QueryUserByTokenRequest{
		Token:     "",
		TokenType: tokenTypeRequest,
	})
	// Agent: 拨号失败、超时、传输错误或没有回包都无法启动。success=false 仍算调用已通。
	if err != nil || resp == nil {
		// Agent: 没有回包时补上原因，避免日志里只剩一个状态码。
		if err == nil {
			err = errors.New("empty response")
		}
		// Agent: 启动检查不带 wandb key，传输错误可以原样写入日志。
		logerr.Errorf("query_user_by_token startup check failed code=%s err=%v", status.Code(err), err)
		return fmt.Errorf("USER_GRPC_ADDR is unreachable: %w", err)
	}
	// Agent: 有回包即通过。日志不带 wandb key，这次调用本来也没有 key。
	log.Printf("query_user_by_token startup check passed")
	return nil
}

type userGRPCConfig struct {
	addr  string
	creds credentials.TransportCredentials
	ttl   time.Duration
}

// loadUserGRPCConfig 只做启动检查：地址必填；证书开关打开时必须读到 CA。
// loadUserGRPCConfig performs startup checks: the address is required, and an enabled switch requires a readable CA.
func loadUserGRPCConfig(getenv func(string) string) (*userGRPCConfig, error) {
	addr := strings.TrimSpace(getenv(envAddr))
	// Agent: 地址未设置或只有空白时拒绝启动，不退回 local。
	if addr == "" {
		return nil, fmt.Errorf("USER_GRPC_ADDR is required")
	}
	var creds credentials.TransportCredentials
	// Agent: 只有精确的 true 或 on 才读 CA，其它值都不读。
	if getenv(envCertEnabled) == "true" || getenv(envCertEnabled) == "on" {
		path := strings.TrimSpace(getenv(envCertPath))
		if path == "" {
			return nil, fmt.Errorf("USER_GRPC_CERT_PATH is required")
		}
		loaded, err := credentials.NewClientTLSFromFile(path, "")
		if err != nil {
			return nil, err
		}
		creds = loaded
	} else {
		creds = insecure.NewCredentials()
	}
	return &userGRPCConfig{
		addr:  addr,
		creds: creds,
		ttl:   parseCacheTTL(getenv(envCacheTTL)),
	}, nil
}

// parseCacheTTL 把整数秒转成缓存时间。空值或非整数用 30 分钟。
// parseCacheTTL converts integer seconds into the cache TTL. Empty or non-integer values use 30 minutes.
func parseCacheTTL(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	// Agent: 未设置时用 30 分钟，不因此启动失败。
	if raw == "" {
		return defaultCacheTTL
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	// Agent: 写了但不是整数时同样用 30 分钟。0 和负数按秒数使用。
	if err != nil {
		return defaultCacheTTL
	}
	return time.Duration(n) * time.Second
}

// Lookup 先查缓存。未命中才调用 query_user_by_token，超时 30 秒。
// Lookup checks the cache first. A miss calls query_user_by_token with a 30-second timeout.
func (c *userClient) Lookup(ctx context.Context, token string) (string, int) {
	now := c.now
	if now == nil {
		now = time.Now
	}
	if id, ok := c.cache.get(token, now()); ok {
		return id, 0
	}
	callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := c.rpc.QueryUserByToken(callCtx, &userpb.QueryUserByTokenRequest{
		Token:     token,
		TokenType: tokenTypeRequest,
	})
	// Agent: 拨号失败、超时和传输错误都不读回包，回 500。日志留状态码和错误文本，不留 key。
	if err != nil || resp == nil {
		if err == nil {
			err = errors.New("empty response")
		}
		logerr.Errorf("query_user_by_token transport error code=%s err=%s http=500", status.Code(err), redactToken(err.Error(), token))
		return "", 500
	}
	userID, status := userIDFromResponse(resp, token)
	if status != 0 {
		return "", status
	}
	c.cache.put(token, userID, now())
	return userID, 0
}

// userIDFromResponse 按已确认的成功条件取出 user_id。失败只记日志，不把 key 写入日志。
// userIDFromResponse extracts the user_id using the confirmed success rules. Failures are logged without the key.
func userIDFromResponse(resp *userpb.UserInfoResponse, token string) (string, int) {
	base := resp.GetBase()
	// Agent: 缺 base 或 success 不是 true 时忽略 id，回 500，并记下回包里能看的字段。
	if base == nil || !base.GetSuccess() {
		logerr.Errorf("query_user_by_token rejected %s", redactToken(rejectedDetail(resp), token))
		return "", 500
	}
	info := resp.GetUserClusterInfo()
	// Agent: success 为 true 后，空对象、token_type、status、id 任一不通过都回 401。
	if info == nil || resp.GetTokenType() != tokenTypeWorb || info.GetStatus() != statusNormal || info.GetId() <= 0 {
		// Agent: 响应体仍只有 invalid key。日志写明是哪一项不通过，不写 wandb key。
		logerr.Errorf("query_user_by_token invalid key: %s", redactToken(invalidKeyReason(resp), token))
		return "", 401
	}
	return strconv.FormatInt(info.GetId(), 10), 0
}

// rejectedDetail 写出 success 不是 true 时的请求类型和回包字段。不包含 wandb key。
// rejectedDetail records the request token type and response fields when success is not true. It does not include the wandb key.
func rejectedDetail(resp *userpb.UserInfoResponse) string {
	base := resp.GetBase()
	reason := "base.success=false"
	success := false
	code := int32(0)
	message := ""
	// Agent: 没有 base 时不能把 success 当成 false 回包，原因单独写明。
	if base == nil {
		reason = "missing base"
	} else {
		success = base.GetSuccess()
		code = base.GetErrorCode()
		message = base.GetMessage()
	}
	info := resp.GetUserClusterInfo()
	infoState := "missing"
	userStatus := ""
	var id int64
	if info != nil {
		infoState = "present"
		userStatus = info.GetStatus()
		id = info.GetId()
	}
	return fmt.Sprintf("reason=%s request_token_type=%s response_token_type=%q success=%t error_code=%d message=%q user_cluster_info=%s status=%q id=%d http=500",
		reason, tokenTypeRequest, resp.GetTokenType(), success, code, message, infoState, userStatus, id)
}

// invalidKeyReason 列出 success 为 true 时仍不能换出 user_id 的字段。
// invalidKeyReason lists the fields that still block a user_id after success is true.
func invalidKeyReason(resp *userpb.UserInfoResponse) string {
	var parts []string
	info := resp.GetUserClusterInfo()
	// Agent: 对象缺失时不再读取 status 和 id。
	if info == nil {
		parts = append(parts, "missing user_cluster_info")
	}
	if resp.GetTokenType() != tokenTypeWorb {
		parts = append(parts, fmt.Sprintf("token_type=%q", resp.GetTokenType()))
	}
	if info != nil && info.GetStatus() != statusNormal {
		parts = append(parts, fmt.Sprintf("status=%q", info.GetStatus()))
	}
	if info != nil && info.GetId() <= 0 {
		parts = append(parts, fmt.Sprintf("id=%d", info.GetId()))
	}
	return strings.Join(parts, " ")
}

// redactToken 在错误文本包含 wandb key 时替换掉，避免把 key 打进日志。
// redactToken replaces the wandb key when an error string contains it, so the key is not logged.
func redactToken(message, token string) string {
	if token != "" && strings.Contains(message, token) {
		return strings.ReplaceAll(message, token, "[redacted]")
	}
	return message
}
