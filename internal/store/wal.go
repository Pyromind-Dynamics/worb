package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	walMaxSize      = 32 << 30
	walSoftSize     = 28 << 30
	walBatchItems   = 50_000
	walFlushEvery   = 500 * time.Millisecond
	walPollSleep    = 50 * time.Millisecond
	walCompactMin   = 1 << 30
	walFsyncEvery   = time.Second
	walPosSaveEvery = 5 * time.Second
)

type walEntry struct {
	RunID    string          `json:"run_id"`
	History  []walHistoryRow `json:"history,omitempty"`
	Events   []walEventRow   `json:"events,omitempty"`
	Logs     []walLogRow     `json:"logs,omitempty"`
	ForkFrom *walForkOp      `json:"fork_from,omitempty"`
}

type walForkOp struct {
	ParentRunID string `json:"parent_run_id"`
	MaxStep     int    `json:"max_step"`
}

type walHistoryRow struct {
	Step int             `json:"step"`
	Data json.RawMessage `json:"data"`
}

type walEventRow struct {
	LineNum int             `json:"line_num"`
	Data    json.RawMessage `json:"data"`
}

type walLogRow struct {
	LineNum int    `json:"line_num"`
	Line    string `json:"line"`
}

type WALStats struct {
	Size     int64 `json:"size_bytes"`
	Position int64 `json:"position_bytes"`
	Lag      int64 `json:"lag_bytes"`
}

func (w *WAL) Stats() WALStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	lag := w.size - w.pos
	if lag < 0 {
		lag = 0
	}
	return WALStats{Size: w.size, Position: w.pos, Lag: lag}
}

type WAL struct {
	file     *os.File
	mu       sync.Mutex
	size     int64
	maxSize  int64
	softSize int64
	pos      int64
	dataDir  string
	// Agent: 本实例专用路径，文件名含 APP_ENV 与 Pod 序号，多副本不共读一份 wal.ndjson。
	ndjsonPath string
	posPath    string

	flushNow  chan chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

// newWAL 打开本实例的 WAL 文件。文件名带 APP_ENV 与 StatefulSet 序号，只读写自己的内容。
// newWAL opens this instance's WAL files. Names include APP_ENV and the StatefulSet ordinal so pods do not read each other.
func newWAL(dataDir string) (*WAL, error) {
	// Agent: 打开前先算出本实例文件名，读写和 compact 都只用这一对路径。
	ndjsonPath, posPath, err := walInstancePaths(dataDir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(ndjsonPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}

	// Agent: 进度文件也带同一前缀，避免 prod/pre 或其它 Pod 覆盖 pos。
	pos := readPosFile(posPath)
	if pos > info.Size() {
		pos = 0
	}

	w := &WAL{
		file:       f,
		size:       info.Size(),
		maxSize:    walMaxSize,
		softSize:   walSoftSize,
		pos:        pos,
		dataDir:    dataDir,
		ndjsonPath: ndjsonPath,
		posPath:    posPath,
		flushNow:   make(chan chan struct{}, 1),
		done:       make(chan struct{}),
	}

	log.Printf("[wal] instance file=%s pos=%s", filepath.Base(ndjsonPath), filepath.Base(posPath))
	// Agent: 升级后不再消费无前缀文件，避免再次读到别人写下的内容。
	legacy := filepath.Join(dataDir, "wal.ndjson")
	if _, err := os.Stat(legacy); err == nil && legacy != ndjsonPath {
		log.Printf("[wal] ignoring unprefixed wal.ndjson; this instance uses %s", filepath.Base(ndjsonPath))
	}

	return w, nil
}

func (w *WAL) Append(entry walEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal wal entry: %w", err)
	}

	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size >= w.maxSize {
		return fmt.Errorf("WAL full (%d bytes)", w.size)
	}

	if w.size >= w.softSize {
		fraction := float64(w.size-w.softSize) / float64(w.maxSize-w.softSize)
		delay := time.Duration(fraction * float64(100*time.Millisecond))
		w.mu.Unlock()
		time.Sleep(delay)
		w.mu.Lock()
	}

	n, err := w.file.Write(data)
	if err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	w.size += int64(n)
	return nil
}

func (w *WAL) startReader(db *DB) {
	go w.readerLoop(db)
	go w.fsyncLoop()
}

func (w *WAL) fsyncLoop() {
	ticker := time.NewTicker(walFsyncEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.file.Sync()
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

func (w *WAL) readerLoop(db *DB) {
	// Agent: 只打开本实例 ndjson，不扫描目录里其它环境/序号的 WAL。
	rf, err := os.Open(w.ndjsonPath)
	if err != nil {
		log.Printf("[wal] ERROR opening read handle: %v", err)
		return
	}
	defer rf.Close()

	if w.pos > 0 {
		if _, err := rf.Seek(w.pos, io.SeekStart); err != nil {
			log.Printf("[wal] ERROR seeking to pos %d: %v", w.pos, err)
			w.pos = 0
			rf.Seek(0, io.SeekStart)
		}
	}

	var (
		scalars     []scalarItem
		histograms  []histogramItem
		events      []eventItem
		logs        []logItem
		counters    []counterDelta
		forkOps     []forkOpItem
		itemCount   int
		lastFlush   = time.Now()
		lastPosSave = time.Now()
		lastCompact = time.Now()
	)

	flush := func() {
		if itemCount == 0 {
			return
		}
		if len(scalars) > 0 {
			db.processScalars(scalars)
			scalars = scalars[:0]
		}
		if len(histograms) > 0 {
			db.processHistograms(histograms)
			histograms = histograms[:0]
		}
		if len(events) > 0 {
			db.processEvents(events)
			events = events[:0]
		}
		if len(logs) > 0 {
			db.processLogs(logs)
			logs = logs[:0]
		}
		if len(counters) > 0 {
			db.processCounters(counters)
			counters = counters[:0]
		}
		for _, fop := range forkOps {
			if err := db.processForkOp(fop.runID, fop.op); err != nil {
				log.Printf("[flush] ERROR fork run=%s parent=%s: %v", fop.runID[:8], fop.op.ParentRunID[:8], err)
			}
		}
		forkOps = forkOps[:0]
		itemCount = 0
		lastFlush = time.Now()
	}

	const scanBufMax = 256 << 20
	newScanner := func() *bufio.Scanner {
		s := bufio.NewScanner(rf)
		s.Buffer(make([]byte, 0, 256<<10), scanBufMax)
		return s
	}
	scanner := newScanner()

	for {
		if w.closed.Load() {
			flush()
			w.savePos()
			return
		}

		select {
		case ch := <-w.flushNow:
			for scanner.Scan() {
				w.pos += int64(len(scanner.Bytes())) + 1
				w.processLine(scanner.Bytes(), &scalars, &histograms, &events, &logs, &counters, &forkOps, &itemCount)
			}
			flush()
			w.savePos()
			scanner = newScanner()
			close(ch)
			continue
		default:
		}

		if scanner.Scan() {
			line := scanner.Bytes()
			w.pos += int64(len(line)) + 1
			w.processLine(line, &scalars, &histograms, &events, &logs, &counters, &forkOps, &itemCount)

			if itemCount >= walBatchItems || time.Since(lastFlush) >= walFlushEvery {
				flush()
			}
			if time.Since(lastPosSave) >= walPosSaveEvery {
				w.savePos()
				lastPosSave = time.Now()
			}
			if time.Since(lastCompact) >= 30*time.Second {
				w.maybeCompact(rf)
				rf, _ = os.Open(w.ndjsonPath)
				rf.Seek(w.pos, io.SeekStart)
				scanner = newScanner()
				lastCompact = time.Now()
			}
			continue
		}

		if err := scanner.Err(); err != nil {
			log.Printf("[wal] scanner error: %v", err)
		}

		flush()

		if time.Since(lastPosSave) >= walPosSaveEvery {
			w.savePos()
			lastPosSave = time.Now()
		}

		time.Sleep(walPollSleep)

		rf.Seek(w.pos, io.SeekStart)
		scanner = newScanner()
	}
}

type forkOpItem struct {
	runID string
	op    walForkOp
}

func (w *WAL) processLine(line []byte, scalars *[]scalarItem, histograms *[]histogramItem, events *[]eventItem, logs *[]logItem, counters *[]counterDelta, forkOps *[]forkOpItem, itemCount *int) {
	if len(line) == 0 {
		return
	}

	var entry walEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		log.Printf("[wal] bad line (skipping): %v", err)
		return
	}

	if entry.ForkFrom != nil {
		*forkOps = append(*forkOps, forkOpItem{runID: entry.RunID, op: *entry.ForkFrom})
		*itemCount++
		return
	}

	if len(entry.History) > 0 {
		rows := make([]struct {
			Step int
			Data json.RawMessage
		}, len(entry.History))
		for i, h := range entry.History {
			rows[i].Step = h.Step
			rows[i].Data = h.Data
		}
		s, h := parseHistoryRows(rows)
		for _, sc := range s {
			*scalars = append(*scalars, scalarItem{runID: entry.RunID, parsedScalar: sc})
		}
		for _, hh := range h {
			*histograms = append(*histograms, histogramItem{runID: entry.RunID, parsedHistogram: hh})
		}
		*counters = append(*counters, counterDelta{runID: entry.RunID, column: "history_line_count", delta: len(entry.History)})
		*itemCount += len(s) + len(h)
	}

	for _, e := range entry.Events {
		*events = append(*events, eventItem{runID: entry.RunID, lineNum: e.LineNum, data: string(e.Data)})
		*itemCount++
	}
	if len(entry.Events) > 0 {
		*counters = append(*counters, counterDelta{runID: entry.RunID, column: "events_line_count", delta: len(entry.Events)})
	}

	for _, l := range entry.Logs {
		*logs = append(*logs, logItem{runID: entry.RunID, lineNum: l.LineNum, line: l.Line})
		*itemCount++
	}
	if len(entry.Logs) > 0 {
		*counters = append(*counters, counterDelta{runID: entry.RunID, column: "log_line_count", delta: len(entry.Logs)})
	}
}

func (w *WAL) flushSync() {
	ch := make(chan struct{})
	w.flushNow <- ch
	<-ch
}

func (w *WAL) close() {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.done)
		time.Sleep(100 * time.Millisecond)
		w.mu.Lock()
		w.file.Sync()
		w.file.Close()
		w.mu.Unlock()
	})
}

func (w *WAL) savePos() {
	// Agent: pos 与 ndjson 共用前缀，多 Pod 不会抢同一个 wal.pos。
	os.WriteFile(w.posPath, []byte(strconv.FormatInt(w.pos, 10)), 0644)
}

// walInstancePaths 拼出本进程专用的 ndjson 与 pos 路径：{APP_ENV}-{ordinal}-wal.ndjson。
// walInstancePaths builds this process's ndjson and pos paths: {APP_ENV}-{ordinal}-wal.ndjson.
func walInstancePaths(dataDir string) (ndjsonPath, posPath string, err error) {
	// Agent: 前缀 1 是环境，前缀 2 是 Pod 序号，中间用 - 拼接后再接到原文件名。
	prefix, err := walFilePrefix()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(dataDir, prefix+"-wal.ndjson"), filepath.Join(dataDir, prefix+"-wal.pos"), nil
}

// walFilePrefix 把 APP_ENV 与 Pod 序号拼成 WAL 文件名前缀。
// walFilePrefix joins APP_ENV and the pod ordinal into the WAL filename prefix.
func walFilePrefix() (string, error) {
	env, err := walAppEnvPrefix()
	if err != nil {
		return "", err
	}
	// Agent: 例 prod-0、pre-1；本机无配置时是 local-0。
	return env + "-" + walPodOrdinal(), nil
}

// walAppEnvPrefix 读取 APP_ENV 作为前缀 1。生产 prod、预发 pre；未设置时用 local。
// walAppEnvPrefix reads APP_ENV as prefix 1. Use prod/pre in cluster; local when unset.
func walAppEnvPrefix() (string, error) {
	// Agent: STS 注入 APP_ENV；空值不再落到共用的 wal.ndjson。
	raw := strings.TrimSpace(os.Getenv("APP_ENV"))
	if raw == "" {
		return "local", nil
	}
	// Agent: 前缀会拼进路径，只允许字母数字、下划线和短横，避免写出 dataDir 之外。
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			continue
		}
		return "", fmt.Errorf("APP_ENV is not a safe WAL file prefix")
	}
	return raw, nil
}

// walPodOrdinal 从 StatefulSet 的 Pod 名取出序号作为前缀 2。K8s 把 HOSTNAME 设成 {name}-{ordinal}。
// walPodOrdinal reads the StatefulSet ordinal as prefix 2. Kubernetes sets HOSTNAME to {name}-{ordinal}.
func walPodOrdinal() string {
	// Agent: 与 filecloud.ordinalZero 同一套 HOSTNAME 规则，不另引入环境变量。
	host := os.Getenv("HOSTNAME")
	if host == "" {
		host, _ = os.Hostname()
	}
	i := strings.LastIndex(host, "-")
	if i < 0 || i == len(host)-1 {
		return "0"
	}
	n, err := strconv.Atoi(host[i+1:])
	if err != nil || n < 0 {
		return "0"
	}
	return strconv.Itoa(n)
}

func readPosFile(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (w *WAL) maybeCompact(rf *os.File) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.pos <= w.size/2 || w.size < int64(walCompactMin) {
		return
	}

	// Agent: compact 只改写本实例文件，临时文件也带前缀以免并发 rename。
	walPath := w.ndjsonPath
	tmpPath := walPath + ".tmp"

	src, err := os.Open(walPath)
	if err != nil {
		log.Printf("[wal] compact: open src: %v", err)
		return
	}
	defer src.Close()

	if _, err := src.Seek(w.pos, io.SeekStart); err != nil {
		log.Printf("[wal] compact: seek: %v", err)
		return
	}

	dst, err := os.Create(tmpPath)
	if err != nil {
		log.Printf("[wal] compact: create tmp: %v", err)
		return
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		dst.Close()
		os.Remove(tmpPath)
		log.Printf("[wal] compact: copy: %v", err)
		return
	}
	dst.Sync()
	dst.Close()
	src.Close()
	rf.Close()

	w.file.Close()

	if err := os.Rename(tmpPath, walPath); err != nil {
		log.Printf("[wal] compact: rename: %v", err)
		return
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("[wal] compact: reopen write: %v", err)
		return
	}
	w.file = f
	w.pos = 0
	w.size = n
	w.savePos()

	log.Printf("[wal] compacted: %d bytes remaining", n)
}
