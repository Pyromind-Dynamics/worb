package objectstore

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeHead struct {
	err error
}

func (f fakeHead) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, f.err
}

func (f fakeHead) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (f fakeHead) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

// TestOpenRejectsS3 确认 s3 不能作为存储类型启动。
// TestOpenRejectsS3 checks that s3 is not a storage type that can start.
func TestOpenRejectsS3(t *testing.T) {
	// Agent: 不读 secrets，s3 在选择阶段就被拒绝。
	if _, err := Open("s3", t.TempDir()); err == nil {
		t.Fatal("s3 started")
	}
	client, err := Open("off", t.TempDir())
	if err != nil || client != nil {
		t.Fatal("off should skip object storage")
	}
}

// TestHeadBucket 确认桶访问失败时启动检查失败，成功时通过。
// TestHeadBucket checks that a bucket access failure fails the startup check and success passes.
func TestHeadBucket(t *testing.T) {
	if err := headBucket(context.Background(), fakeHead{}, "bucket", "internal"); err != nil {
		t.Fatal(err)
	}
	// Agent: 任何访问错误都算没有连上，不能只凭配置启动。
	if err := headBucket(context.Background(), fakeHead{err: errors.New("dial")}, "bucket", "public"); err == nil {
		t.Fatal("unreachable bucket started")
	}
}

// TestPingChecksBothEndpoints 确认内网和公网都要能访问桶。
// TestPingChecksBothEndpoints checks that both the internal and public endpoints must reach the bucket.
func TestPingChecksBothEndpoints(t *testing.T) {
	api := &s3API{bucket: "b", client: fakeHead{}, pub: fakeHead{}}
	if err := api.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Agent: 只有公网失败时也要退出，不能只用内网地址启动。
	api.pub = fakeHead{err: errors.New("dial")}
	if err := api.Ping(context.Background()); err == nil {
		t.Fatal("public endpoint failure was ignored")
	}
	api.client = fakeHead{err: errors.New("dial")}
	api.pub = fakeHead{}
	if err := api.Ping(context.Background()); err == nil {
		t.Fatal("internal endpoint failure was ignored")
	}
}
