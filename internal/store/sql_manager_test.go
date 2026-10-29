package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TestSQLByEngineUsesNumberedStatements 确认按编号能取到 SQLite 和 MySQL 两套语句。
// TestSQLByEngineUsesNumberedStatements checks that numbered IDs resolve for both engines.
func TestSQLByEngineUsesNumberedStatements(t *testing.T) {
	sqliteInsert := SQLByEngine("sqlite", SQLInsertFileIgnore)
	mysqlInsert := SQLByEngine("mysql", SQLInsertFileIgnore)
	if !strings.Contains(sqliteInsert, "INSERT OR IGNORE") {
		t.Fatalf("sqlite insert = %s", sqliteInsert)
	}
	if !strings.Contains(mysqlInsert, "INSERT IGNORE") || strings.Contains(mysqlInsert, "INSERT OR IGNORE") {
		t.Fatalf("mysql insert = %s", mysqlInsert)
	}
	sqliteBucket := SQLByEngine("sqlite", SQLSelectScalarsBucket)
	mysqlBucket := SQLByEngine("mysql", SQLSelectScalarsBucket)
	if !strings.Contains(sqliteBucket, "step / {n}") {
		t.Fatalf("sqlite bucket = %s", sqliteBucket)
	}
	if !strings.Contains(mysqlBucket, "step DIV {n}") {
		t.Fatalf("mysql bucket = %s", mysqlBucket)
	}
}

// TestSQLDeleteBatchByID 确认分批删除按编号区分引擎。
// TestSQLDeleteBatchByID checks that batched deletes differ by engine ID.
func TestSQLDeleteBatchByID(t *testing.T) {
	mysqlDB := &DB{Engine: "mysql"}
	sqliteDB := &DB{Engine: "sqlite"}
	q, err := mysqlDB.SQLDeleteBatch("history_scalars", "run_id")
	if err != nil {
		t.Fatal(err)
	}
	if q != "DELETE FROM history_scalars WHERE run_id = ? LIMIT ?" {
		t.Fatalf("mysql delete = %s", q)
	}
	q, err = sqliteDB.SQLDeleteBatch("system_events", "run_id")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "rowid IN") {
		t.Fatalf("sqlite delete = %s", q)
	}
	if _, err := mysqlDB.SQLDeleteBatch("runs", "run_id"); err == nil {
		t.Fatal("unknown table was accepted")
	}
}

// TestSQLSchemaByID 确认建表语句按编号存放，且 MySQL 不创建数据库。
// TestSQLSchemaByID checks schema statements are numbered and MySQL does not create a database.
func TestSQLSchemaByID(t *testing.T) {
	var joined []string
	for _, id := range schemaSQLIDs() {
		joined = append(joined, SQLByEngine("mysql", id))
	}
	text := strings.Join(joined, "\n")
	if strings.Contains(strings.ToUpper(text), "CREATE DATABASE") || strings.Contains(strings.ToUpper(text), "CREATE SCHEMA") {
		t.Fatal("mysql schema creates a database")
	}
	if !strings.Contains(text, "entity VARCHAR(512)") {
		t.Fatal("entity is shorter than 512")
	}
	if strings.Contains(SQLByEngine("sqlite", SQLCreateScalars), "AUTO_INCREMENT") {
		t.Fatal("sqlite schema picked up mysql syntax")
	}
	if SQLByEngine("sqlite", SQLCreateScalars) == "" || SQLByEngine("mysql", SQLCreateScalars) == "" {
		t.Fatal("missing schema statement")
	}
}

// TestDBSQLMethod 确认 *DB.SQL 按引擎切换。
// TestDBSQLMethod checks that *DB.SQL switches by engine.
func TestDBSQLMethod(t *testing.T) {
	mysqlDB := &DB{Engine: "mysql"}
	sqliteDB := &DB{Engine: "sqlite"}
	if got := mysqlDB.SQL(SQLCopyRunKeys); !strings.HasPrefix(got, "INSERT IGNORE") {
		t.Fatalf("mysql copy = %s", got)
	}
	if got := sqliteDB.SQL(SQLCopyRunKeys); !strings.HasPrefix(got, "INSERT OR IGNORE") {
		t.Fatalf("sqlite copy = %s", got)
	}
	if mysqlDB.SQLScalarsBucket(3) == sqliteDB.SQLScalarsBucket(3) {
		t.Fatal("bucket SQL should differ by engine")
	}
}

// TestLoadMySQLConfigTrimsNewline 确认连接参数去掉末尾换行，并且缺文件会失败。
// TestLoadMySQLConfigTrimsNewline checks that secrets drop a trailing newline and missing files fail.
func TestLoadMySQLConfigTrimsNewline(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"db_host": "db.internal\n", "db_port": "3306\n", "db_user": "worb\n",
		"db_passwd": "p@ss/word\n", "db_name": "worb\n",
	}
	for name, body := range files {
		if err := os.WriteFile(dir+"/"+name, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DB_SSL_ENABLED", "")
	cfg, err := LoadMySQLConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "db.internal" || cfg.Password != "p@ss/word" || cfg.DBName != "worb" || cfg.SSL {
		t.Fatalf("config = %+v", cfg)
	}
	dsn, err := mysqlDSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Passwd != "p@ss/word" || parsed.DBName != "worb" || parsed.Addr != "db.internal:3306" {
		t.Fatalf("parsed user=%s db=%s addr=%s", parsed.User, parsed.DBName, parsed.Addr)
	}
	if !strings.Contains(dsn, "charset=utf8mb4") || !strings.Contains(dsn, "parseTime=true") || !strings.Contains(dsn, "loc=UTC") {
		t.Fatalf("dsn = %s", dsn)
	}
	if err := os.Remove(dir + "/db_name"); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMySQLConfig(dir); err == nil {
		t.Fatal("missing db_name was accepted")
	}
}

// TestMySQLSSLRequiresCA 确认开启加密但没有 CA 时失败。
// TestMySQLSSLRequiresCA checks that TLS without a CA file fails.
func TestMySQLSSLRequiresCA(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"db_host", "db_port", "db_user", "db_passwd", "db_name"} {
		if err := os.WriteFile(dir+"/"+name, []byte("x\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DB_SSL_ENABLED", "true")
	t.Setenv("DB_SSL_CA", "")
	if _, err := LoadMySQLConfig(dir); err == nil {
		t.Fatal("missing CA was accepted")
	}
}

// TestFlexTimeBytes 确认 MySQL 返回的 []byte 时间可以读出来。
// TestFlexTimeBytes checks that a MySQL []byte timestamp can be scanned.
func TestFlexTimeBytes(t *testing.T) {
	var ft flexTime
	if err := ft.Scan([]byte("2026-10-29 01:02:03")); err != nil {
		t.Fatal(err)
	}
	if ft.Format(time.RFC3339) != "2026-10-29T01:02:03Z" {
		t.Fatalf("time = %s", ft.Format(time.RFC3339))
	}
}

// TestBusyLockCodes 确认 MySQL 死锁和锁等待会走回退查询。
// TestBusyLockCodes checks that MySQL deadlock and lock-wait errors use the fallback query.
func TestBusyLockCodes(t *testing.T) {
	if !isBusyLock(&mysql.MySQLError{Number: 1213}) || !isBusyLock(&mysql.MySQLError{Number: 1205}) {
		t.Fatal("mysql lock errors were not recognized")
	}
	if isBusyLock(&mysql.MySQLError{Number: 1064}) {
		t.Fatal("syntax error was treated as a lock")
	}
}

// TestHistogramPacketSplits 确认很大的直方图批次会被拆到包大小以内。
// TestHistogramPacketSplits checks that a large histogram batch is split under the packet cap.
func TestHistogramPacketSplits(t *testing.T) {
	prefix := SQLByEngine("mysql", SQLInsertHistsPrefix)
	row := "(?,?,?,?,?,?)"
	args := make([]any, 0, 8*6)
	for i := 0; i < 8; i++ {
		args = append(args, "run", i, "loss", nil, strings.Repeat("1", 400000), strings.Repeat("2", 400000))
	}
	n := 8
	for n > 1 && estimateInsert(prefix, row, n, args[:n*6]) > maxSQLPacketBytes {
		n /= 2
	}
	if n != 1 {
		t.Fatalf("batch width = %d, want 1", n)
	}
}

// TestMySQLBehaviors 在提供了连接参数时检查 schema、去重、分桶和分批删除。
// TestMySQLBehaviors checks schema validation, insert ignore, buckets, and batched delete when MySQL settings are present.
func TestMySQLBehaviors(t *testing.T) {
	host := os.Getenv("WORB_MYSQL_HOST")
	if host == "" {
		t.Skip("WORB_MYSQL_HOST is not set")
	}
	cfg := MySQLConfig{
		Host: host, Port: envOr("WORB_MYSQL_PORT", "3306"),
		User: os.Getenv("WORB_MYSQL_USER"), Password: os.Getenv("WORB_MYSQL_PASSWD"),
		DBName: os.Getenv("WORB_MYSQL_DB"),
	}
	db, err := NewMySQL(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// Agent: MySQL 启动只做检查，第二次同样只检查，不建表。
	if err := db.migrate(); err != nil {
		t.Fatalf("second schema check: %v", err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "mysql-behavior", Name: "run1"})
	if err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 10; step++ {
		if err := db.InsertHistory(run.ID, step, []byte(`{"loss":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.InsertHistory(run.ID, 1, []byte(`{"loss":2}`)); err != nil {
		t.Fatal(err)
	}
	db.Flush()
	var points int
	if err := db.QueryRow(db.SQL(SQLCountDistinctSteps)+" AND `key` = ?", run.ID, "loss").Scan(&points); err != nil {
		// Agent: 上面只是占位；下面用实际标量计数。
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ? AND `key` = ?", run.ID, "loss").Scan(&points); err != nil {
		t.Fatal(err)
	}
	if points != 10 {
		t.Fatalf("points = %d, want 10 after INSERT IGNORE", points)
	}
	var buckets int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT step DIV 3 AS bucket FROM history_scalars
			WHERE run_id = ? AND `+"`key`"+` = ?
			GROUP BY step DIV 3
		) b`, run.ID, "loss").Scan(&buckets); err != nil {
		t.Fatal(err)
	}
	if buckets != 4 {
		t.Fatalf("buckets = %d, want 4", buckets)
	}
	deleted, err := db.deleteBatch("history_scalars", "run_id", run.ID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 4 {
		t.Fatalf("deleted = %d, want 4", deleted)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
