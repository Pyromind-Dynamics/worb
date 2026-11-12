package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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
