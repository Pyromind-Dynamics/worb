package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	// Agent: 匿名导入注册 MySQL 驱动，镜像/二进制开箱即可 --db-engine mysql，无需宿主机再装客户端。
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

type flexTime struct {
	time.Time
}

func (ft *flexTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ft.Time = v
		return nil
	case string:
		for _, layout := range []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			time.RFC3339,
		} {
			if t, err := time.Parse(layout, v); err == nil {
				ft.Time = t
				return nil
			}
		}
		return fmt.Errorf("flexTime: cannot parse %q", v)
	case []byte:
		// Agent: MySQL 文本时间列返回 []byte，按字符串再解析一次。
		return ft.Scan(string(v))
	case nil:
		ft.Time = time.Time{}
		return nil
	default:
		return fmt.Errorf("flexTime: unsupported type %T (%v)", value, value)
	}
}

func (ft flexTime) Value() (driver.Value, error) {
	return ft.Time, nil
}

type DB struct {
	*sql.DB
	Engine       string
	wal          *WAL
	dataDir      string
	ctx          context.Context
	cancel       context.CancelFunc
	sqlPerf      *sqlPerfTracker
	storageCache *storageUsageCache
}

type SQLPerfTopQuery struct {
	Query   string  `json:"query"`
	Count   int64   `json:"count"`
	AvgMS   float64 `json:"avgMs"`
	MaxMS   float64 `json:"maxMs"`
	TotalMS float64 `json:"totalMs"`
}

type SQLPerfHistogramBucket struct {
	Label   string  `json:"label"`
	Count   int64   `json:"count"`
	Percent float64 `json:"percent"`
}

type SQLPerfSnapshot struct {
	TotalQueries int64                    `json:"totalQueries"`
	TopQueries   []SQLPerfTopQuery        `json:"topQueries"`
	Histogram    []SQLPerfHistogramBucket `json:"histogram"`
}

type sqlPerfTracker struct {
	mu       sync.Mutex
	total    int64
	byType   map[string]*sqlPerfAgg
	boundsMS []float64
	labels   []string
	counts   []int64
}

type sqlPerfAgg struct {
	count int64
	total float64
	max   float64
}

func newSQLPerfTracker() *sqlPerfTracker {
	labels := []string{
		"<=1ms",
		"<=5ms",
		"<=10ms",
		"<=25ms",
		"<=50ms",
		"<=100ms",
		"<=250ms",
		"<=500ms",
		"<=1s",
		"<=2.5s",
		">2.5s",
	}
	return &sqlPerfTracker{
		boundsMS: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		labels:   labels,
		counts:   make([]int64, len(labels)),
		byType:   make(map[string]*sqlPerfAgg),
	}
}

func normalizeSQLQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return "<empty>"
	}
	fields := strings.Fields(query)
	query = strings.Join(fields, " ")
	if len(query) > 260 {
		return query[:260] + "..."
	}
	return query
}

func (t *sqlPerfTracker) record(query string, d time.Duration) {
	query = normalizeSQLQuery(query)
	ms := float64(d) / float64(time.Millisecond)
	if ms < 0 {
		ms = 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.total++
	idx := len(t.boundsMS)
	for i, b := range t.boundsMS {
		if ms <= b {
			idx = i
			break
		}
	}
	t.counts[idx]++

	agg, ok := t.byType[query]
	if !ok {
		agg = &sqlPerfAgg{}
		t.byType[query] = agg
	}
	agg.count++
	agg.total += ms
	if ms > agg.max {
		agg.max = ms
	}
}

func (t *sqlPerfTracker) snapshot() SQLPerfSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	top := make([]SQLPerfTopQuery, 0, len(t.byType))
	for typ, agg := range t.byType {
		avg := 0.0
		if agg.count > 0 {
			avg = agg.total / float64(agg.count)
		}
		top = append(top, SQLPerfTopQuery{
			Query:   typ,
			Count:   agg.count,
			AvgMS:   avg,
			MaxMS:   agg.max,
			TotalMS: agg.total,
		})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].AvgMS == top[j].AvgMS {
			return top[i].MaxMS > top[j].MaxMS
		}
		return top[i].AvgMS > top[j].AvgMS
	})
	if len(top) > 10 {
		top = top[:10]
	}
	h := make([]SQLPerfHistogramBucket, len(t.labels))
	var maxCount int64
	for i := range t.counts {
		if t.counts[i] > maxCount {
			maxCount = t.counts[i]
		}
	}
	for i := range t.labels {
		percent := 0.0
		if maxCount > 0 {
			percent = float64(t.counts[i]) * 100.0 / float64(maxCount)
		}
		h[i] = SQLPerfHistogramBucket{
			Label:   t.labels[i],
			Count:   t.counts[i],
			Percent: percent,
		}
	}
	return SQLPerfSnapshot{
		TotalQueries: t.total,
		TopQueries:   top,
		Histogram:    h,
	}
}

func (db *DB) castJSON(col string) string {
	return col
}

// isMySQL 判断当前引擎是不是 MySQL。
// isMySQL reports whether the current engine is MySQL.
func (db *DB) isMySQL() bool {
	// Agent: 空库对象按 SQLite 处理，避免改写原语句。
	return db != nil && db.Engine == "mysql"
}

// isSQLite 判断当前引擎是否使用 SQLite 语法。MySQL 返回 false。
// isSQLite reports whether the engine uses SQLite SQL. MySQL returns false.
func (db *DB) isSQLite() bool {
	// Agent: 只有 MySQL 离开 SQLite 专用路径，turso 仍走 SQLite 语句。
	return !db.isMySQL()
}

func New(dataDir, engine string) (*DB, error) {
	if engine == "" {
		engine = "sqlite"
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	var db *sql.DB
	var err error

	switch engine {
	case "sqlite":
		dbPath := filepath.Join(dataDir, "worb.db")
		db, err = sql.Open("sqlite", dbPath+"?_pragma=journal_mode(wal)&_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)")
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		db.SetMaxOpenConns(32)
	case "turso":
		url := os.Getenv("TURSO_URL")
		if url == "" {
			return nil, fmt.Errorf("TURSO_URL environment variable is required for turso engine")
		}
		token := os.Getenv("TURSO_AUTH_TOKEN")
		dsn := url
		if token != "" {
			dsn += "?authToken=" + token
		}
		db, err = sql.Open("libsql", dsn)
		if err != nil {
			return nil, fmt.Errorf("open turso: %w", err)
		}
	case "mysql":
		// Agent: 只连接已存在的库。参数缺失或 Ping 失败时直接返回，不退回 SQLite。
		var cfg MySQLConfig
		cfg, err = LoadMySQLConfig(mysqlSecretDir)
		if err != nil {
			return nil, err
		}
		db, err = OpenMySQL(cfg)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported db engine: %s", engine)
	}

	// Agent: SQLite、turso、MySQL 共用这一次初始化和一个连接池。
	return startStore(db, dataDir, engine)
}

// startStore 完成 schema 处理并启动 WAL 和后台清理。整个进程只调用一次。
// startStore finishes schema setup and starts the WAL and background cleanup. It runs once per process.
func startStore(raw *sql.DB, dataDir, engine string) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &DB{DB: raw, Engine: engine, dataDir: dataDir, ctx: ctx, cancel: cancel, sqlPerf: newSQLPerfTracker(), storageCache: newStorageUsageCache()}
	if err := store.migrate(); err != nil {
		raw.Close()
		cancel()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	wal, err := newWAL(dataDir)
	if err != nil {
		raw.Close()
		cancel()
		return nil, fmt.Errorf("init wal: %w", err)
	}
	store.wal = wal
	wal.startReader(store)

	store.StartCleanup()
	store.StartStorageUsageRefresh()

	return store, nil
}

func (db *DB) WalStats() WALStats { return db.wal.Stats() }

func (db *DB) Flush() {
	db.wal.flushSync()
}

func (db *DB) Close() error {
	db.wal.flushSync()
	db.wal.close()
	db.cancel()
	return db.DB.Close()
}

func (db *DB) recordSQLPerf(query string, start time.Time) {
	if db.sqlPerf == nil {
		return
	}
	db.sqlPerf.record(query, time.Since(start))
}

func (db *DB) SQLPerfSnapshot() SQLPerfSnapshot {
	if db.sqlPerf == nil {
		return SQLPerfSnapshot{}
	}
	return db.sqlPerf.snapshot()
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	res, err := db.DB.Exec(query, args...)
	db.recordSQLPerf(query, start)
	return res, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	res, err := db.DB.ExecContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return res, err
}

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	rows, err := db.DB.Query(query, args...)
	db.recordSQLPerf(query, start)
	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	rows, err := db.DB.QueryContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	row := db.DB.QueryRow(query, args...)
	db.recordSQLPerf(query, start)
	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// Agent: 语句由调用方用编号从 sql_manager 取出，这里不再改写。
	start := time.Now()
	row := db.DB.QueryRowContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return row
}

type QueryResult struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	Truncated bool            `json:"truncated,omitempty"`
}

const maxQueryRows = 10000

func (db *DB) ExecuteQuery(query string) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Agent: 页面提交的 SQL 原样执行，不改写成另一种引擎的语句。
	start := time.Now()
	rows, err := db.DB.QueryContext(ctx, query)
	db.recordSQLPerf(query, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &QueryResult{Columns: cols}

	for rows.Next() {
		if len(result.Rows) >= maxQueryRows {
			result.Truncated = true
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]interface{}, len(cols))
		for i, v := range vals {
			switch val := v.(type) {
			case []byte:
				row[i] = string(val)
			default:
				row[i] = val
			}
		}
		result.Rows = append(result.Rows, row)
	}

	if result.Rows == nil {
		result.Rows = [][]interface{}{}
	}

	return result, rows.Err()
}

// migrate SQLite 建表；MySQL 只检查已有表结构。
// migrate creates tables on SQLite; on MySQL it only checks the existing schema.
func (db *DB) migrate() error {
	// Agent: MySQL 不执行 CREATE / ALTER，表由部署侧提前建好。
	return db.migrateEngine()
}

// activeRunIDs returns the subset of ids that exist and are not deleted.
func (db *DB) activeRunIDs(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	// Agent: IN 列表长度随 run 数量变化，由 sql_manager 按编号拼装。
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := db.Query(db.SQLActiveRunIDs(len(ids)), args...)
	if err != nil {
		// On error, assume all are active to avoid dropping data
		active := make(map[string]bool, len(ids))
		for _, id := range ids {
			active[id] = true
		}
		return active
	}
	defer rows.Close()
	active := make(map[string]bool)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			active[id] = true
		}
	}
	return active
}
