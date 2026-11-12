package objectstore

import (
	"context"
	"fmt"
)

// Open 按 OBJECT_STORE 的取值创建客户端。off 或空串返回 nil。oss 要先连上桶。
// Open builds a client from OBJECT_STORE. off and empty return nil. oss must reach the bucket first.
func Open(kind, secretDir string) (Client, error) {
	switch kind {
	case "", "off":
		return nil, nil
	case "oss":
		cfg, err := LoadConfig(secretDir, true)
		if err != nil {
			return nil, fmt.Errorf("oss config: %w", err)
		}
		client, err := NewS3(cfg)
		if err != nil {
			return nil, err
		}
		// Agent: 启动对内网和公网分别检查上传、读取、删除。任一地址少一个权限就不能启动。
		if err := client.Ping(context.Background()); err != nil {
			return nil, err
		}
		return client, nil
	default:
		// Agent: s3 以及其它取值都不是本期可选项，不能当成 OSS 启动。
		return nil, fmt.Errorf("OBJECT_STORE %q is not supported", kind)
	}
}
