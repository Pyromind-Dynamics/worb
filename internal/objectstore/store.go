package objectstore

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

// ErrNotFound 表示对象不存在。删除时把它当成已经删掉。
// ErrNotFound means the object is already absent. Delete treats that as success.
var ErrNotFound = errors.New("object not found")

// Client 是 OSS 的上传、删除和签名接口。
// Client is the upload, delete, and sign API for OSS.
type Client interface {
	// Put 上传一个对象。成功时返回 ETag，没有则返回空串。
	// Put uploads one object and returns its ETag when the service sends one.
	Put(ctx context.Context, key string, body io.Reader, contentType string) (string, error)
	// Delete 删除对象。对象不存在时返回 ErrNotFound。
	// Delete removes an object. A missing object returns ErrNotFound.
	Delete(ctx context.Context, key string) error
	// SignedURL 生成限时下载地址，不写入数据库。
	// SignedURL returns a time-limited download URL and does not store it.
	SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// Config 是启动时从 secrets 文件读出的对象存储参数。
// Config holds object storage settings loaded from secret files at startup.
type Config struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	PathStyle    bool
}

// LoadConfig 读取 /etc/secrets 下的 object_store_* 文件。缺文件则失败。
// LoadConfig reads object_store_* files under /etc/secrets. A missing file is an error.
func LoadConfig(dir string, pathStyle bool) (Config, error) {
	read := func(name string, required bool) (string, error) {
		b, err := os.ReadFile(dir + "/" + name)
		if err != nil {
			if !required && os.IsNotExist(err) {
				return "", nil
			}
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	cfg := Config{PathStyle: pathStyle}
	var err error
	if cfg.Endpoint, err = read("object_store_endpoint", true); err != nil {
		return Config{}, err
	}
	if cfg.Region, err = read("object_store_region", false); err != nil {
		return Config{}, err
	}
	if cfg.Bucket, err = read("object_store_bucket", true); err != nil {
		return Config{}, err
	}
	if cfg.AccessKey, err = read("object_store_access_key", true); err != nil {
		return Config{}, err
	}
	if cfg.SecretKey, err = read("object_store_secret_key", true); err != nil {
		return Config{}, err
	}
	if cfg.SessionToken, err = read("object_store_session_token", false); err != nil {
		return Config{}, err
	}
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return Config{}, errors.New("object store secret is empty")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	return cfg, nil
}
