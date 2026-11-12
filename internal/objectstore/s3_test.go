package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeObjects struct {
	bucket      string
	key         string
	body        string
	contentType string
	putErr      error
	etag        *string
	delErr      error
}

func (f *fakeObjects) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	// Agent: 读完 body，确认上传内容没有被丢掉。
	b, _ := io.ReadAll(params.Body)
	f.bucket = aws.ToString(params.Bucket)
	f.key = aws.ToString(params.Key)
	f.body = string(b)
	f.contentType = aws.ToString(params.ContentType)
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{ETag: f.etag}, nil
}

func (f *fakeObjects) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.body))}, nil
}

func (f *fakeObjects) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.bucket = aws.ToString(params.Bucket)
	f.key = aws.ToString(params.Key)
	if f.delErr != nil {
		return nil, f.delErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

type fakePresign struct {
	bucket string
	key    string
	ttl    time.Duration
	url    string
	err    error
}

func (f *fakePresign) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	f.bucket = aws.ToString(params.Bucket)
	f.key = aws.ToString(params.Key)
	opts := &s3.PresignOptions{}
	for _, fn := range optFns {
		fn(opts)
	}
	f.ttl = opts.Expires
	if f.err != nil {
		return nil, f.err
	}
	return &v4.PresignedHTTPRequest{URL: f.url}, nil
}

type statusErr struct{ code int }

func (e statusErr) Error() string       { return "status" }
func (e statusErr) HTTPStatusCode() int { return e.code }

// TestPutWritesObject 确认上传使用指定桶和 key，并去掉 ETag 两侧引号。
// TestPutWritesObject checks that upload uses the bucket and key and strips quotes around the ETag.
func TestPutWritesObject(t *testing.T) {
	fake := &fakeObjects{etag: aws.String(`"abc"`)}
	api := &s3API{bucket: "bkt", client: fake}
	etag, err := api.Put(context.Background(), "k", bytes.NewReader([]byte("hello")), "")
	if err != nil {
		t.Fatal(err)
	}
	if etag != "abc" || fake.bucket != "bkt" || fake.key != "k" || fake.body != "hello" || fake.contentType != "application/octet-stream" {
		t.Fatalf("etag %q fake %+v", etag, fake)
	}
	fake.etag = nil
	fake.putErr = errors.New("denied")
	if _, err := api.Put(context.Background(), "k", bytes.NewReader(nil), "text/plain"); err == nil {
		t.Fatal("put error was ignored")
	}
}

// TestDeleteMapsMissingObject 确认对象不存在时删除返回 ErrNotFound，其它错误原样返回。
// TestDeleteMapsMissingObject checks that a missing object becomes ErrNotFound and other errors stay unchanged.
func TestDeleteMapsMissingObject(t *testing.T) {
	fake := &fakeObjects{}
	api := &s3API{bucket: "bkt", client: fake}
	if err := api.Delete(context.Background(), "k"); err != nil || fake.key != "k" || fake.bucket != "bkt" {
		t.Fatalf("delete %v %+v", err, fake)
	}
	for _, code := range []string{"NoSuchKey", "NotFound"} {
		fake.delErr = &smithy.GenericAPIError{Code: code, Message: "gone"}
		if err := api.Delete(context.Background(), "k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("code %s err %v", code, err)
		}
	}
	fake.delErr = statusErr{code: http.StatusNotFound}
	if err := api.Delete(context.Background(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	fake.delErr = errors.New("timeout")
	if err := api.Delete(context.Background(), "k"); !errors.Is(err, fake.delErr) {
		t.Fatal(err)
	}
}

// TestSignedURLReturnsGetAddress 确认签名地址原样返回，并带上 key 和有效期。
// TestSignedURLReturnsGetAddress checks that the signed URL is returned with the key and expiry.
func TestSignedURLReturnsGetAddress(t *testing.T) {
	fake := &fakePresign{url: "https://oss.example/k"}
	api := &s3API{bucket: "bkt", presign: fake}
	url, err := api.SignedURL(context.Background(), "k", 24*time.Hour)
	if err != nil || url != "https://oss.example/k" || fake.key != "k" || fake.bucket != "bkt" || fake.ttl != 24*time.Hour {
		t.Fatalf("url %q err %v fake %+v", url, err, fake)
	}
	fake.err = errors.New("sign")
	if _, err := api.SignedURL(context.Background(), "k", time.Minute); err == nil {
		t.Fatal("sign error was ignored")
	}
}

type captureHTTP struct {
	url *url.URL
}

func (c *captureHTTP) Do(r *http.Request) (*http.Response, error) {
	copied := *r.URL
	c.url = &copied
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
		Request:    r,
	}, nil
}

// TestOSSAddressStyle 确认内网使用虚拟主机风格，公网自定义域名保持配置的主机名。
// TestOSSAddressStyle checks virtual hosted style on the internal endpoint and an unchanged host on the public custom domain.
func TestOSSAddressStyle(t *testing.T) {
	cfg := Config{
		Endpoint:    "https://oss-cn-shanghai-internal.aliyuncs.com",
		EndpointPub: "http://oss-pub.example.test",
		Region:      "cn-shanghai",
		Bucket:      "aliyun-huosi-worb-data-v1",
		AccessKey:   "ak",
		SecretKey:   "sk",
	}
	internal := &captureHTTP{}
	opts, err := s3ClientOptions(cfg, cfg.Endpoint, false)
	if err != nil {
		t.Fatal(err)
	}
	opts.HTTPClient = internal
	_, err = s3.New(opts).PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String("worb-startup-probe/abc"),
		Body:   strings.NewReader("worb-startup"),
	})
	if err != nil || internal.url == nil || internal.url.Scheme != "https" || internal.url.Host != "aliyun-huosi-worb-data-v1.oss-cn-shanghai-internal.aliyuncs.com" || internal.url.Path != "/worb-startup-probe/abc" {
		t.Fatalf("internal %v %v", err, internal.url)
	}
	public := &captureHTTP{}
	opts, err = s3ClientOptions(cfg, cfg.EndpointPub, true)
	if err != nil {
		t.Fatal(err)
	}
	opts.HTTPClient = public
	client := s3.New(opts)
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String("worb-startup-probe/abc"),
		Body:   strings.NewReader("worb-startup"),
	})
	if err != nil || public.url == nil || public.url.Scheme != "http" || public.url.Host != "oss-pub.example.test" || public.url.Path != "/worb-startup-probe/abc" {
		t.Fatalf("public %v %v", err, public.url)
	}
	signed, err := s3.NewPresignClient(client).PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String("worb-startup-probe/abc"),
	}, s3.WithPresignExpires(time.Hour))
	if err != nil || !strings.HasPrefix(signed.URL, "http://oss-pub.example.test/worb-startup-probe/abc?") {
		t.Fatalf("signed %v %v", err, signed)
	}
}
