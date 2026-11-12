package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Notifier 发送运维告警。未配置时使用不发网络请求的实现。
// Notifier sends operator alerts. The unconfigured implementation does not call the network.
type Notifier interface {
	// Notify 发送一条文本。失败只记日志。
	// Notify sends one text message. Failures are logged only.
	Notify(ctx context.Context, text string) error
}

type noop struct{}

// Notify 不发送，避免没有 webhook 时阻断主流程。
// Notify does nothing so a missing webhook cannot block uploads.
func (noop) Notify(context.Context, string) error { return nil }

// Feishu 向自定义机器人发文本。磁盘满告警在同一进程内冷却 10 分钟。
// Feishu posts text to a custom bot. Disk-full alerts cool down for 10 minutes in this process.
type Feishu struct {
	url      string
	client   *http.Client
	mu       sync.Mutex
	nextDisk time.Time
}

// New 按 ALERT_FEISHU 和 webhook 文件决定是否真正发送。
// New enables sending only when ALERT_FEISHU allows it and the webhook file exists.
func New(secretDir string) Notifier {
	if strings.EqualFold(os.Getenv("ALERT_FEISHU"), "off") {
		return noop{}
	}
	b, err := os.ReadFile(secretDir + "/feishu_webhook_url")
	if err != nil || strings.TrimSpace(string(b)) == "" {
		return noop{}
	}
	return &Feishu{url: strings.TrimSpace(string(b)), client: &http.Client{Timeout: 5 * time.Second}}
}

// Notify 发送普通文本。
// Notify posts a plain text body.
func (f *Feishu) Notify(ctx context.Context, text string) error {
	return f.post(ctx, text)
}

// NotifyDiskFull 在 10 分钟冷却外发送磁盘满告警。
// NotifyDiskFull sends a disk-full alert outside the 10 minute cooldown.
func (f *Feishu) NotifyDiskFull(ctx context.Context, text string) {
	f.mu.Lock()
	if time.Now().Before(f.nextDisk) {
		f.mu.Unlock()
		log.Printf("disk full alert suppressed")
		return
	}
	f.nextDisk = time.Now().Add(10 * time.Minute)
	f.mu.Unlock()
	_ = f.post(ctx, text)
}

func (f *Feishu) post(ctx context.Context, text string) error {
	body, _ := json.Marshal(map[string]any{
		"msg_type": "text",
		"content":  map[string]string{"text": text},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		log.Printf("feishu notify: %v", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("feishu notify status %d", resp.StatusCode)
	}
	return nil
}

// AsFeishu 在具体类型上调用磁盘满冷却。Noop 时直接返回。
// AsFeishu reaches the disk-full cooldown. A noop notifier returns nil.
func AsFeishu(n Notifier) *Feishu {
	f, _ := n.(*Feishu)
	return f
}
