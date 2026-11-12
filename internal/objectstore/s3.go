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
	// Agent: 用接口保住 Put、Delete、签名的真实调用，测试可以换成假客户端。
	client  objectAPI
	presign presignAPI
}

// NewS3 创建路径风格可选的 S3 客户端。OSS 使用 path style。
// NewS3 builds an S3 client. OSS turns path style on.
func NewS3(cfg Config) (*s3API, error) {
	client := s3.New(s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: aws.String(cfg.Endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		UsePathStyle: cfg.PathStyle,
	})
	return &s3API{bucket: cfg.Bucket, client: client, presign: s3.NewPresignClient(client)}, nil
}

// Ping 确认 endpoint、密钥和 bucket 能访问。失败时进程不能启动。
// Ping checks that the endpoint, credentials, and bucket are reachable. Failure stops startup.
func (s *s3API) Ping(ctx context.Context) error {
	// Agent: 限时 30 秒，避免 OSS 无响应时启动一直停住。
	return headBucket(ctx, s.client, s.bucket)
}

// headBucket 对桶做一次 Head。任何错误都表示没有连上。
// headBucket issues one Head on the bucket. Any error means it is not reachable.
func headBucket(ctx context.Context, api bucketHead, bucket string) error {
	callCtx, cancel := context.WithTimeout(ctx, ossPingTimeout)
	defer cancel()
	_, err := api.HeadBucket(callCtx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("oss unreachable: %w", err)
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

// SignedURL 生成 指定有效期的 GET 地址。
// SignedURL returns a GET URL that expires after ttl.
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
