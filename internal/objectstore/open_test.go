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
	if err := headBucket(context.Background(), fakeHead{}, "bucket"); err != nil {
		t.Fatal(err)
	}
	// Agent: 任何访问错误都算没有连上，不能只凭配置启动。
	if err := headBucket(context.Background(), fakeHead{err: errors.New("dial")}, "bucket"); err == nil {
		t.Fatal("unreachable bucket started")
	}
}
