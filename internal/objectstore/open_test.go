package objectstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeProbe struct {
	putErr error
	getErr error
	delErr error
	body   string
	puts   int
	gets   int
	dels   int
}

func (f *fakeProbe) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.puts++
	b, _ := io.ReadAll(params.Body)
	f.body = string(b)
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeProbe) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.body))}, nil
}

func (f *fakeProbe) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.dels++
	if f.delErr != nil {
		return nil, f.delErr
	}
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

// TestPingRequiresPutGetDelete 确认内网和公网都必须依次通过上传、读取和删除。
// TestPingRequiresPutGetDelete checks that both endpoints require upload, read, and delete in that order.
func TestPingRequiresPutGetDelete(t *testing.T) {
	internal := &fakeProbe{}
	public := &fakeProbe{}
	if err := (&s3API{bucket: "b", client: internal, public: public}).Ping(context.Background()); err != nil || internal.puts != 1 || internal.gets != 1 || internal.dels != 1 || public.puts != 1 || public.gets != 1 || public.dels != 1 {
		t.Fatalf("err %v internal %d %d %d public %d %d %d", err, internal.puts, internal.gets, internal.dels, public.puts, public.gets, public.dels)
	}
	// Agent: 内网上传失败时，这个地址不再读、不再删；公网仍要完成三项检查。
	putDenied := &fakeProbe{putErr: errors.New("403")}
	public = &fakeProbe{}
	err := (&s3API{bucket: "b", client: putDenied, public: public}).Ping(context.Background())
	if err == nil || !strings.Contains(err.Error(), "internal PutObject") || putDenied.gets != 0 || putDenied.dels != 0 || public.puts != 1 || public.gets != 1 || public.dels != 1 {
		t.Fatal(err)
	}
	// Agent: 公网读取失败仍删除该地址上的探测对象，并拒绝启动。
	getDenied := &fakeProbe{getErr: errors.New("403")}
	err = (&s3API{bucket: "b", client: &fakeProbe{}, public: getDenied}).Ping(context.Background())
	if err == nil || !strings.Contains(err.Error(), "public GetObject") || getDenied.dels != 1 {
		t.Fatal(err)
	}
	delDenied := &fakeProbe{delErr: errors.New("403")}
	err = (&s3API{bucket: "b", client: &fakeProbe{}, public: delDenied}).Ping(context.Background())
	if err == nil || !strings.Contains(err.Error(), "public DeleteObject") {
		t.Fatal(err)
	}
}
