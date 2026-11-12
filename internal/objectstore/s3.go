package objectstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	ossPingTimeout = 30 * time.Second
	// Agent: 探测对象内容固定且不含密钥，用来确认读回的就是刚写上的字节。
	probeBody = "worb-startup"
)

// objectAPI 是上传、读取、删除和启动检查用到的对象存储调用。
// objectAPI is the object storage calls used for upload, read, delete, and the startup check.
type objectAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// presignAPI 只生成下载地址，不把对象读回进程。
// presignAPI only builds a download URL and does not read the object back.
type presignAPI interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// s3API 用 S3 兼容接口访问阿里云 OSS。
// s3API talks to Alibaba Cloud OSS through its S3-compatible API.
type s3API struct {
	bucket string
	// Agent: client 只连内网地址，负责业务上传、读取、删除，以及启动时的内网检查。
	client objectAPI
	// Agent: public 只连公网地址。业务下载仍走签名，启动时用它再做一遍三项检查。
	public  objectAPI
	presign presignAPI
}

// newS3Client 按同一个密钥和桶选项连接指定地址。内网使用虚拟主机风格。
// newS3Client connects to one endpoint with the shared credentials and bucket options. The internal endpoint uses virtual hosted style.
func newS3Client(cfg Config, endpoint string, customDomain bool) (*s3.Client, error) {
	opts, err := s3ClientOptions(cfg, endpoint, customDomain)
	if err != nil {
		return nil, err
	}
	return s3.New(opts), nil
}

// s3ClientOptions 组装 S3 客户端选项。customDomain 为真时，签名前恢复自定义域名的主机名。
// s3ClientOptions builds S3 client options. When customDomain is set, signing restores the custom-domain host.
func s3ClientOptions(cfg Config, endpoint string, customDomain bool) (s3.Options, error) {
	opts := s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		// Agent: 阿里云拒绝 path style。虚拟主机风格把桶名放在主机名前面。
		UsePathStyle: false,
	}
	if !customDomain {
		return opts, nil
	}
	host, err := configuredHost(endpoint)
	if err != nil {
		return s3.Options{}, err
	}
	// Agent: 公网自定义域名已经绑定到桶，签名前把主机名改回配置值。
	opts.APIOptions = append(opts.APIOptions, keepCustomDomainHost(host))
	return opts, nil
}

// configuredHost 取出 endpoint 的主机名，含端口。没有协议或主机名则失败。
// configuredHost returns the endpoint host, including a port. A missing scheme or host is an error.
func configuredHost(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("object store endpoint %q is not a valid URI", endpoint)
	}
	return u.Host, nil
}

// customDomainHost 在签名前把请求主机改回自定义域名。
// customDomainHost restores the custom-domain host before the request is signed.
type customDomainHost struct {
	host string
}

func (*customDomainHost) ID() string { return "ossCustomDomainHost" }

func (m *customDomainHost) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if ok {
		// Agent: 路径仍是对象键。只改主机名，避免变成桶名加在自定义域名前面。
		req.URL.Host = m.host
		req.Host = m.host
	}
	return next.HandleFinalize(ctx, in)
}

// keepCustomDomainHost 把主机名还原插到地址解析之后、签名之前。
// keepCustomDomainHost inserts host restoration after endpoint resolution and before signing.
func keepCustomDomainHost(host string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Insert(&customDomainHost{host: host}, "ResolveEndpointV2", middleware.After)
	}
}

// NewS3 创建内网管理客户端和公网签名客户端。内网用虚拟主机风格，公网保持自定义域名。
// NewS3 builds the internal management client and the public signing client. Internal uses virtual hosted style, and the public client keeps its custom domain.
func NewS3(cfg Config) (*s3API, error) {
	// Agent: 业务上传和删除走内网地址，主机名是桶名加内网 Endpoint。
	internal, err := newS3Client(cfg, cfg.Endpoint, false)
	if err != nil {
		return nil, err
	}
	// Agent: 同一个公网客户端既签发临时下载地址，也在启动时做公网三项检查。
	public, err := newS3Client(cfg, cfg.EndpointPub, true)
	if err != nil {
		return nil, err
	}
	return &s3API{bucket: cfg.Bucket, client: internal, public: public, presign: s3.NewPresignClient(public)}, nil
}

// Ping 对内网地址和公网地址分别做上传、读回、删除。任一地址少一项权限或连不上，进程不能启动。
// Ping checks upload, read, and delete on the internal endpoint and again on the public endpoint. Either address missing a permission or unreachable stops startup.
func (s *s3API) Ping(ctx context.Context) error {
	var internalErr, publicErr error
	var wg sync.WaitGroup
	wg.Add(2)
	// Agent: 两个地址各有 30 秒，并行检查，一个地址变慢不会吃掉另一个地址的时间。
	go func() {
		defer wg.Done()
		internalErr = s.probeEndpoint(ctx, s.client, "internal")
	}()
	go func() {
		defer wg.Done()
		publicErr = s.probeEndpoint(ctx, s.public, "public")
	}()
	wg.Wait()
	return errors.Join(internalErr, publicErr)
}

// probeEndpoint 在一个地址上依次上传、读回、删除一个探测对象。上传失败不再读和删。
// probeEndpoint uploads, reads back, and deletes one probe object on one endpoint. A failed upload does not continue to read or delete.
func (s *s3API) probeEndpoint(ctx context.Context, api objectAPI, which string) error {
	if api == nil {
		return fmt.Errorf("oss %s client is missing", which)
	}
	callCtx, cancel := context.WithTimeout(ctx, ossPingTimeout)
	defer cancel()
	key, err := probeObjectKey()
	if err != nil {
		return err
	}
	// Agent: 先确认能写。写失败时桶里没有探测对象，不再继续读和删。
	_, err = api.PutObject(callCtx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(probeBody),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("oss %s PutObject check failed: %w", which, err)
	}
	getErr := readProbe(callCtx, api, s.bucket, key, which)
	// Agent: 读失败也要删除探测对象，避免在桶里留下这次检查的文件。
	_, delErr := api.DeleteObject(callCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if delErr != nil {
		delErr = fmt.Errorf("oss %s DeleteObject check failed: %w", which, delErr)
	}
	return errors.Join(getErr, delErr)
}

// probeObjectKey 生成不会和用户文件对象键冲突的探测键。
// probeObjectKey builds a probe key that does not collide with user file keys.
func probeObjectKey() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "worb-startup-probe/" + hex.EncodeToString(raw[:]), nil
}

// readProbe 读回探测对象并核对内容。失败表示这个地址没有 GetObject 权限或对象不可读。
// readProbe reads the probe object and checks its body. Failure means this endpoint lacks GetObject or the object is unreadable.
func readProbe(ctx context.Context, api objectAPI, bucket, key, which string) error {
	out, err := api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("oss %s GetObject check failed: %w", which, err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("oss %s GetObject check failed: %w", which, err)
	}
	// Agent: 内容不一致也算这个地址的读取权限检查没有通过。
	if string(got) != probeBody {
		return fmt.Errorf("oss %s GetObject check failed: body mismatch", which)
	}
	return nil
}

// Put 上传对象。Content-Type 为空时用二进制流。
// Put uploads the object. An empty content type becomes application/octet-stream.
func (s *s3API) Put(ctx context.Context, key string, body io.Reader, contentType string) (string, error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", err
	}
	if out.ETag == nil {
		return "", nil
	}
	return strings.Trim(*out.ETag, `"`), nil
}

// Delete 删除对象。404 / NoSuchKey 返回 ErrNotFound。
// Delete removes the object. 404 and NoSuchKey become ErrNotFound.
func (s *s3API) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" {
			return ErrNotFound
		}
	}
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
		return ErrNotFound
	}
	return err
}

// SignedURL 用公网地址生成指定有效期的 GET 地址。
// SignedURL returns a GET URL on the public endpoint that expires after ttl.
func (s *s3API) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	out, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}
