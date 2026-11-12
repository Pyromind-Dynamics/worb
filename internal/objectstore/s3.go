package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const ossPingTimeout = 30 * time.Second

// bucketHead 只用于启动时确认桶能访问。测试可以替换。
// bucketHead is the startup bucket check. Tests can substitute it.
type bucketHead interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// objectAPI 是上传、删除和启动检查用到的对象存储调用。
// objectAPI is the object storage calls used for upload, delete, and the startup check.
type objectAPI interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
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
	// Agent: client 只连内网地址，负责上传、删除和内网连通检查。
	client objectAPI
	// Agent: pub 只连公网地址，启动时检查它，签名也从这条客户端生成。
	pub     bucketHead
	presign presignAPI
}

// newS3Client 按同一个密钥和桶选项连接指定地址。
// newS3Client connects to one endpoint with the shared credentials and bucket options.
func newS3Client(cfg Config, endpoint string) *s3.Client {
	return s3.New(s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		UsePathStyle: cfg.PathStyle,
	})
}

// NewS3 创建内网管理客户端和公网签名客户端。OSS 使用 path style。
// NewS3 builds the internal management client and the public signing client. OSS turns path style on.
func NewS3(cfg Config) (*s3API, error) {
	// Agent: 上传和删除走内网地址，不把公网地址用在管理请求上。
	internal := newS3Client(cfg, cfg.Endpoint)
	// Agent: 临时下载地址从公网客户端签发，用户浏览器才能打开。
	public := newS3Client(cfg, cfg.EndpointPub)
	return &s3API{bucket: cfg.Bucket, client: internal, pub: public, presign: s3.NewPresignClient(public)}, nil
}

// Ping 分别确认内网地址和公网地址都能访问同一个桶。任一失败则进程不能启动。
// Ping checks that both the internal and public endpoints can reach the same bucket. Either failure stops startup.
func (s *s3API) Ping(ctx context.Context) error {
	errInternal := headBucket(ctx, s.client, s.bucket, "internal")
	var errPublic error
	// Agent: 没有公网客户端时不能签出用户可打开的地址，同样视为没连上。
	if s.pub == nil {
		errPublic = errors.New("oss public endpoint is not configured")
	} else {
		errPublic = headBucket(ctx, s.pub, s.bucket, "public")
	}
	return errors.Join(errInternal, errPublic)
}

// headBucket 对桶做一次 Head。任何错误都表示这个地址没有连上。
// headBucket issues one Head on the bucket. Any error means this endpoint is not reachable.
func headBucket(ctx context.Context, api bucketHead, bucket, which string) error {
	callCtx, cancel := context.WithTimeout(ctx, ossPingTimeout)
	defer cancel()
	_, err := api.HeadBucket(callCtx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		// Agent: 错误里标明是内网还是公网，不写入密钥。
		return fmt.Errorf("oss %s endpoint unreachable: %w", which, err)
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
