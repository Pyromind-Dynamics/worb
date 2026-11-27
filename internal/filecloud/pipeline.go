package filecloud

import (
	"context"
	"errors"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sarna/worb/internal/alert"
	"github.com/sarna/worb/internal/objectstore"
	"github.com/sarna/worb/internal/store"
)

// Pipeline 跑上云和删除队列。定时任务由环境变量单独打开。
// Pipeline runs the upload and delete queues. Scheduled jobs stay off unless their env is on.
type Pipeline struct {
	db       *store.DB
	cloud    objectstore.Client
	alerts   alert.Notifier
	dataDir  string
	kind     string
	uploadCh chan string
	deleteCh chan string
	ctx      context.Context
	cancel   context.CancelFunc
}

// Start 启动固定数量的 worker。云端未配置时只跑本地删除。
// Start launches a fixed number of workers. Without cloud config it only deletes local files.
func Start(db *store.DB, cloud objectstore.Client, alerts alert.Notifier, dataDir, kind string) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		db: db, cloud: cloud, alerts: alerts, dataDir: dataDir, kind: kind,
		uploadCh: make(chan string, 1024),
		deleteCh: make(chan string, 256),
		ctx:      ctx, cancel: cancel,
	}
	if cloud != nil {
		for i := 0; i < 2; i++ {
			go p.uploadLoop()
		}
	}
	go p.deleteLoop()
	p.startJobs()
	return p
}

// Stop 结束后台循环。
// Stop ends the background loops.
func (p *Pipeline) Stop() {
	if p != nil && p.cancel != nil {
		p.cancel()
	}
}

// EnqueueUpload 非阻塞投入上云队列。队满则留给下一轮扫表。
// EnqueueUpload queues an upload without blocking. A full queue waits for the next scan.
func (p *Pipeline) EnqueueUpload(id string) {
	if p == nil || p.cloud == nil || store.FileKindOf(id) == store.FileKindUnknown {
		return
	}
	select {
	case p.uploadCh <- id:
	default:
	}
}

// EnqueueDelete 非阻塞投入删除队列。
// EnqueueDelete queues cleanup without blocking.
func (p *Pipeline) EnqueueDelete(ids []string) {
	if p == nil {
		return
	}
	for i, id := range ids {
		// Agent: 无前缀的旧行也入队。删除时只查 files，不因为缺少前缀而留在库里。
		select {
		case p.deleteCh <- id:
		default:
			// Agent: 队列满时后面的 id 不会入队。写日志留痕；清理循环对仍有残留的 run 会按退避间隔重新送入，不会永久丢失。
			log.Printf("delete queue full, %d of %d ids not queued; cleanup will re-queue them", len(ids)-i, len(ids))
			return
		}
	}
}

// Sign 为已有 object_key 生成 24 小时下载地址。
// Sign builds a 24 hour download URL for an existing object key.
func (p *Pipeline) Sign(ctx context.Context, key string) (string, error) {
	if p == nil || p.cloud == nil || key == "" {
		return "", errors.New("no cloud client")
	}
	return p.cloud.SignedURL(ctx, key, 24*time.Hour)
}

func (p *Pipeline) uploadLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case id := <-p.uploadCh:
			p.uploadOne(id)
		}
	}
}

func (p *Pipeline) uploadOne(id string) {
	file, err := p.db.GetStoredFile(id)
	if err != nil || file.DeletedAt != "" {
		return
	}
	if file.RetryCount >= 3 {
		_ = p.db.StopExhaustedUpload(id)
		return
	}
	ok, err := p.db.ClaimUpload(id)
	if err != nil || !ok {
		return
	}
	file, err = p.db.GetStoredFile(id)
	if err != nil {
		return
	}
	if file.DeletedAt != "" {
		_ = p.db.LeaveUploading(id)
		return
	}
	body, err := os.Open(file.LocalPath)
	if err != nil {
		// Agent: 打开失败时若行已软删，必须离开 uploading，否则删除协程会一直等到超时回收。
		p.failUpload(id, err)
		return
	}
	defer body.Close()
	key := store.ObjectKey(file.Kind, file.EntityKey, file.RunID, file.Name)
	etag, err := p.cloud.Put(p.ctx, key, body, contentType(file.Name))
	if err != nil {
		p.failUpload(id, err)
		return
	}
	wrote, err := p.db.FinishCloud(id, p.kind, key, etag)
	if err != nil {
		return
	}
	if !wrote {
		// Agent: Put 已经成功但行被软删，由本 worker 删掉刚传的对象。
		if delErr := p.cloud.Delete(p.ctx, key); delErr != nil && !errors.Is(delErr, objectstore.ErrNotFound) {
			log.Printf("delete raced upload %s: %v", id, delErr)
		}
		_ = p.db.LeaveUploading(id)
		return
	}
	if err := os.Remove(file.LocalPath); err != nil && !os.IsNotExist(err) {
		log.Printf("unlink after cloud %s: %v", id, err)
		return
	}
	_ = p.db.MarkLocalDeleted(id)
}

func (p *Pipeline) deleteLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case id := <-p.deleteCh:
			if err := p.deleteOne(id); err != nil {
				// Agent: 失败不立刻回队，避免打满 OSS。一分钟后再试一次。
				time.AfterFunc(time.Minute, func() { p.EnqueueDelete([]string{id}) })
			}
		}
	}
}

func (p *Pipeline) deleteOne(id string) error {
	file, err := p.db.GetStoredFile(id)
	if err != nil {
		return nil
	}
	if file.DeletedAt == "" {
		return nil
	}
	if file.Status == "uploading" {
		return errors.New("uploading")
	}
	if file.ObjectKey != "" && p.cloud != nil {
		n, err := p.db.LiveObjectKeyCount(file.ObjectKey)
		if err != nil {
			return err
		}
		if n == 0 {
			if err := p.cloud.Delete(p.ctx, file.ObjectKey); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
				return err
			}
		}
	}
	path := file.LocalPath
	if path == "" {
		path = filepath.Join(p.dataDir, "files", id)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := p.db.MarkLocalDeleted(id); err != nil {
		return err
	}
	return p.db.HardDeleteStoredFile(id)
}

func (p *Pipeline) startJobs() {
	if envOn("JOB_UPLOAD_SCAN") {
		go p.every(time.Minute, p.scanUploads)
	}
	if envOn("JOB_UNLINK_LOCAL") {
		go func() {
			p.repairUnlink()
			p.every(time.Minute, p.repairUnlink)
		}()
	}
	if envOn("JOB_REAP_UPLOADING") {
		go p.clockLoop([]int{0, 12}, p.reap)
	}
	if envOn("JOB_RETRY_DELETE") {
		go p.clockLoop([]int{1}, p.retryDelete)
	}
	if envOn("JOB_FAIL_DIGEST") {
		go p.beijing10(p.digest)
	}
}

func (p *Pipeline) every(d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

func (p *Pipeline) scanUploads() {
	ids, err := p.db.DueUploadIDs(time.Now().UTC())
	if err != nil {
		log.Printf("upload scan: %v", err)
		return
	}
	for _, id := range ids {
		p.EnqueueUpload(id)
	}
}

func (p *Pipeline) repairUnlink() {
	ids, err := p.db.IDsNeedingUnlink()
	if err != nil {
		return
	}
	for _, id := range ids {
		file, err := p.db.GetStoredFile(id)
		if err != nil || file.LocalPath == "" {
			continue
		}
		if err := os.Remove(file.LocalPath); err != nil && !os.IsNotExist(err) {
			continue
		}
		_ = p.db.MarkLocalDeleted(id)
	}
}

func (p *Pipeline) reap() {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	ids, err := p.db.StaleUploadingIDs(cutoff)
	if err != nil {
		return
	}
	for _, id := range ids {
		ok, err := p.db.ReapUploading(id, cutoff)
		if err == nil && ok {
			p.EnqueueUpload(id)
		}
	}
}

func (p *Pipeline) retryDelete() {
	if !p.db.TryJobLock("worb_job_retry_delete") {
		return
	}
	now := time.Now().UTC()
	ids, err := p.db.RetryDeleteIDs(now.Add(-7*24*time.Hour), now.Add(-3*24*time.Hour))
	if err != nil {
		return
	}
	p.EnqueueDelete(ids)
}

func (p *Pipeline) digest() {
	if !ordinalZero() {
		return
	}
	loc := time.FixedZone("CST", 8*3600)
	now := time.Now().In(loc)
	var days []time.Time
	for i := 2; i >= 0; i-- {
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -i)
		days = append(days, day.UTC())
	}
	counts, err := p.db.PermanentFailCounts(days)
	if err != nil {
		return
	}
	var b strings.Builder
	b.WriteString("worb permanent upload failures\n")
	for i := 2; i >= 0; i-- {
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -i)
		key := day.Format("2006-01-02")
		b.WriteString(key + ": " + strconv.Itoa(counts[key]) + "\n")
	}
	_ = p.alerts.Notify(p.ctx, b.String())
}

func (p *Pipeline) clockLoop(hours []int, fn func()) {
	var last string
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			if now.Minute() != 0 {
				continue
			}
			for _, hour := range hours {
				if now.Hour() != hour {
					continue
				}
				stamp := now.Format("2006-01-02-15")
				if stamp == last {
					continue
				}
				last = stamp
				fn()
			}
		}
	}
}

func (p *Pipeline) beijing10(fn func()) {
	loc := time.FixedZone("CST", 8*3600)
	var last string
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			now := time.Now().In(loc)
			if now.Hour() != 10 || now.Minute() != 0 {
				continue
			}
			stamp := now.Format("2006-01-02")
			if stamp == last {
				continue
			}
			last = stamp
			fn()
		}
	}
}

// failUpload 在行已软删时离开 uploading，否则记失败以便退避重试。
// failUpload leaves uploading when the row is soft-deleted, otherwise records a failure for backoff.
func (p *Pipeline) failUpload(id string, cause error) {
	latest, getErr := p.db.GetStoredFile(id)
	if getErr == nil && latest.DeletedAt != "" {
		_ = p.db.LeaveUploading(id)
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	_ = p.db.FailUpload(id, msg)
}

// envOn 只在环境变量精确等于 on 时打开对应任务。
// envOn enables a job only when its environment value is exactly on.
func envOn(key string) bool {
	// Agent: 文档要求只有精确的 on 才开启，ON/Off 都保持关闭。
	return os.Getenv(key) == "on"
}

func ordinalZero() bool {
	host := os.Getenv("HOSTNAME")
	if host == "" {
		host, _ = os.Hostname()
	}
	i := strings.LastIndex(host, "-")
	if i < 0 {
		return false
	}
	n, err := strconv.Atoi(host[i+1:])
	return err == nil && n == 0
}

func contentType(name string) string {
	if t := mime.TypeByExtension(filepath.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
}

// SaveLocal 把上传内容写入扁平目录，并在写满磁盘时告警。
// SaveLocal writes the upload into the flat files directory and alerts when the disk is full.
func SaveLocal(dir, id string, r io.Reader, alerts alert.Notifier) (int64, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	path := filepath.Join(dir, id)
	f, err := os.Create(path)
	if err != nil {
		notifyDisk(alerts, err)
		return 0, err
	}
	n, err := io.Copy(f, r)
	closeErr := f.Close()
	if err != nil {
		notifyDisk(alerts, err)
		return n, err
	}
	return n, closeErr
}

func notifyDisk(alerts alert.Notifier, err error) {
	if alerts == nil || !isDiskFull(err) {
		return
	}
	if f := alert.AsFeishu(alerts); f != nil {
		f.NotifyDiskFull(context.Background(), "worb disk full: "+err.Error())
	}
}

func isDiskFull(err error) bool {
	return err != nil && (errors.Is(err, syscall.ENOSPC) || strings.Contains(err.Error(), "no space left") || strings.Contains(err.Error(), "disk full"))
}
