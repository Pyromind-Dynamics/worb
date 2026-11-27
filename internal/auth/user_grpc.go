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
	// Agent: 请求和成功回包的 token_type 都是 worb_sk。用户服务原样回传请求类型。
	tokenTypeRequest = "worb_sk"
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
	enabled, err := parseCertEnabled(getenv(envCertEnabled))
	if err != nil {
		return nil, err
	}
	var creds credentials.TransportCredentials
	// Agent: 空值才关 TLS。true/on 读 CA；无法识别的非空值已在 parseCertEnabled 拒绝，不会落到明文。
	if enabled {
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

// parseCertEnabled 判定是否加载 CA。空值关闭 TLS；true/on 开启；其它非空值启动失败。
// parseCertEnabled decides whether to load the CA. Empty disables TLS; true/on enables it; any other non-empty value is a startup error.
func parseCertEnabled(raw string) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, nil
	}
	// Agent: 只认精确小写 true/on，不认 TRUE、1、yes。笔误必须启动失败，不能静默明文发 wandb key。
	if raw == "true" || raw == "on" {
		return true, nil
	}
	return false, fmt.Errorf("USER_GRPC_CERT_ENABLED must be true, on, or empty, got %q", raw)
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

// userIDFromResponse 按已确认的成功条件取出 entity，即 user_cluster_info.uid 的十进制字符串。失败只记日志，不把 key 写入日志。
// userIDFromResponse extracts the entity, the decimal user_cluster_info.uid, using the confirmed success rules. Failures are logged without the key.
func userIDFromResponse(resp *userpb.UserInfoResponse, token string) (string, int) {
	base := resp.GetBase()
	// Agent: 缺 base 是回包异常，回 500。有 base 且 success=false 是无效 token 的正常回包，回 401。
	if base == nil {
		logerr.Errorf("query_user_by_token rejected %s", redactToken(rejectedDetail(resp, 500), token))
		return "", 500
	}
	if !base.GetSuccess() {
		logerr.Errorf("query_user_by_token rejected %s", redactToken(rejectedDetail(resp, 401), token))
		return "", 401
	}
	info := resp.GetUserClusterInfo()
	// Agent: success 为 true 后，空对象、token_type、status、uid 任一不通过都回 401。回包 token_type 必须是 worb_sk。entity 取 uid，不再取 id。
	if info == nil || resp.GetTokenType() != tokenTypeRequest || info.GetStatus() != statusNormal || info.GetUid() <= 0 {
		// Agent: 响应体仍只有 invalid key。日志写明是哪一项不通过，不写 wandb key。
		logerr.Errorf("query_user_by_token invalid key: %s", redactToken(invalidKeyReason(resp), token))
		return "", 401
	}
	return strconv.FormatInt(info.GetUid(), 10), 0
}

// rejectedDetail 写出 success 不是 true 或缺少 base 时的请求类型和回包字段。不包含 wandb key。
// rejectedDetail records the request token type and response fields when success is not true or base is missing. It does not include the wandb key.
func rejectedDetail(resp *userpb.UserInfoResponse, httpStatus int) string {
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
	var uid int64
	if info != nil {
		infoState = "present"
		userStatus = info.GetStatus()
		uid = info.GetUid()
	}
	// Agent: http 状态由调用方传入，success=false 为 401，缺 base 为 500。日志写 uid，不再写 id。
	return fmt.Sprintf("reason=%s request_token_type=%s response_token_type=%q success=%t error_code=%d message=%q user_cluster_info=%s status=%q uid=%d http=%d",
		reason, tokenTypeRequest, resp.GetTokenType(), success, code, message, infoState, userStatus, uid, httpStatus)
}

// invalidKeyReason 列出 success 为 true 时仍不能换出 user_id 的字段。
// invalidKeyReason lists the fields that still block a user_id after success is true.
func invalidKeyReason(resp *userpb.UserInfoResponse) string {
	var parts []string
	info := resp.GetUserClusterInfo()
	// Agent: 对象缺失时不再读取 status 和 uid。
	if info == nil {
		parts = append(parts, "missing user_cluster_info")
	}
	if resp.GetTokenType() != tokenTypeRequest {
		parts = append(parts, fmt.Sprintf("token_type=%q", resp.GetTokenType()))
	}
	if info != nil && info.GetStatus() != statusNormal {
		parts = append(parts, fmt.Sprintf("status=%q", info.GetStatus()))
	}
	if info != nil && info.GetUid() <= 0 {
		parts = append(parts, fmt.Sprintf("uid=%d", info.GetUid()))
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
