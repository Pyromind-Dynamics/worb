package store

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	// Agent: 连接参数目录。库由部署侧提前建好，应用不创建数据库。
	mysqlSecretDir = "/etc/secrets"
	// Agent: 一个进程只建一个池。数字是启动时的上限，不随请求变化。
	mysqlMaxOpenConns    = 32
	mysqlMaxIdleConns    = 8
	mysqlConnMaxLifetime = 30 * time.Minute
	mysqlConnMaxIdleTime = 5 * time.Minute
	// Agent: 单条语句控制在 1MiB 内，低于常见的 max_allowed_packet，超限时拆批。
	maxSQLPacketBytes = 1 << 20
)

// MySQLConfig 是进程内的 MySQL 连接参数，不写回磁盘。
// MySQLConfig holds MySQL connection settings in memory and is never written back to disk.
type MySQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSL      bool
	SSLCA    string
}

// LoadMySQLConfig 从目录读取连接参数，并按环境变量决定是否加密。
// LoadMySQLConfig reads connection settings from a directory and TLS settings from the environment.
func LoadMySQLConfig(dir string) (MySQLConfig, error) {
	// Agent: 缺任何一个文件都失败，不退回 SQLite。
	host, err := readSecret(dir, "db_host")
	if err != nil {
		return MySQLConfig{}, err
	}
	port, err := readSecret(dir, "db_port")
	if err != nil {
		return MySQLConfig{}, err
	}
	user, err := readSecret(dir, "db_user")
	if err != nil {
		return MySQLConfig{}, err
	}
	password, err := readSecret(dir, "db_passwd")
	if err != nil {
		return MySQLConfig{}, err
	}
	dbName, err := readSecret(dir, "db_name")
	if err != nil {
		return MySQLConfig{}, err
	}
	cfg := MySQLConfig{Host: host, Port: port, User: user, Password: password, DBName: dbName}
	// Agent: 只有 DB_SSL_ENABLED 精确等于 true 才加密。
	if os.Getenv("DB_SSL_ENABLED") == "true" {
		cfg.SSL = true
		cfg.SSLCA = os.Getenv("DB_SSL_CA")
		if cfg.SSLCA == "" {
			return MySQLConfig{}, fmt.Errorf("DB_SSL_CA is required when DB_SSL_ENABLED=true")
		}
	}
	return cfg, nil
}

// readSecret 读取一个参数文件，并去掉末尾换行。
// readSecret reads one secret file and strips a trailing newline.
func readSecret(dir, name string) (string, error) {
	b, err := os.ReadFile(dir + "/" + name)
	if err != nil {
		return "", fmt.Errorf("read mysql %s: %w", name, err)
	}
	// Agent: 只去掉末尾换行，保留密码里的其它字符。
	v := strings.TrimRight(string(b), "\r\n")
	if v == "" {
		return "", fmt.Errorf("mysql %s is empty", name)
	}
	return v, nil
}

// OpenMySQL 打开一个连接池并 Ping。失败时不包含密码。
// OpenMySQL opens one pool and pings it. Errors do not include the password.
func OpenMySQL(cfg MySQLConfig) (*sql.DB, error) {
	dsn, err := mysqlDSN(cfg)
	if err != nil {
		return nil, err
	}
	// Agent: 全进程只 sql.Open 一次，后面的请求复用这个池。
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(mysqlMaxOpenConns)
	db.SetMaxIdleConns(mysqlMaxIdleConns)
	db.SetConnMaxLifetime(mysqlConnMaxLifetime)
	db.SetConnMaxIdleTime(mysqlConnMaxIdleTime)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql ping failed: %w", err)
	}
	return db, nil
}

// mysqlDSN 组装 DSN。密码里的特殊字符交给驱动转义。
// mysqlDSN builds a DSN. The driver escapes special characters in the password.
func mysqlDSN(cfg MySQLConfig) (string, error) {
	if cfg.Host == "" || cfg.Port == "" || cfg.User == "" || cfg.DBName == "" {
		return "", fmt.Errorf("mysql host, port, user and database are required")
	}
	mc := mysql.NewConfig()
	mc.User = cfg.User
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, cfg.Port)
	mc.DBName = cfg.DBName
	// Agent: 时间和字符集与 SQLite 的 UTC 文本对齐。
	mc.ParseTime = true
	mc.Loc = time.UTC
	// Agent: loc=UTC 要写进 DSN。驱动在 Loc 已经是 UTC 时不会自己写出这个参数。
	mc.Params = map[string]string{"charset": "utf8mb4", "loc": "UTC"}
	if cfg.SSL {
		if err := registerMySQLTLS(cfg.SSLCA); err != nil {
			return "", err
		}
		mc.TLSConfig = mysqlTLSName
	}
	return mc.FormatDSN(), nil
}

const mysqlTLSName = "worb"

// registerMySQLTLS 用 CA 文件注册加密连接。文件打不开就失败。
// registerMySQLTLS registers TLS from a CA file. A missing file is a startup error.
func registerMySQLTLS(caPath string) error {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("read DB_SSL_CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("DB_SSL_CA has no certificates")
	}
	// Agent: 重复启动测试时先去掉同名配置，再注册这一份 CA。
	mysql.DeregisterTLSConfig(mysqlTLSName)
	return mysql.RegisterTLSConfig(mysqlTLSName, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
}

// NewMySQL 用调用方传入的连接参数打开 MySQL，供没有 /etc/secrets 的测试使用。
// NewMySQL opens MySQL with the given settings so tests do not need /etc/secrets.
func NewMySQL(dataDir string, cfg MySQLConfig) (*DB, error) {
	// Agent: 测试直接传参数，不读 /etc/secrets。
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	raw, err := OpenMySQL(cfg)
	if err != nil {
		return nil, err
	}
	return startStore(raw, dataDir, "mysql")
}

// isBusyLock 识别 SQLite 锁等待，以及 MySQL 死锁和锁等待超时。
// isBusyLock reports SQLite lock waits and MySQL deadlock or lock-wait timeout.
func isBusyLock(err error) bool {
	if err == nil {
		return false
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		// Agent: 1213 死锁，1205 锁等待超时。调用方改走回退查询。
		return me.Number == 1213 || me.Number == 1205
	}
	return strings.Contains(err.Error(), "SQLITE_BUSY")
}

// mysqlColumnSpec 是期望列的名称与类型片段。
// mysqlColumnSpec is an expected column name and type fragment.
type mysqlColumnSpec struct {
	Name string
	Type string
}

// mysqlTableSpec 是期望表、列与索引。
// mysqlTableSpec is an expected table with columns and indexes.
type mysqlTableSpec struct {
	Name    string
	Columns []mysqlColumnSpec
	Indexes []string
}

// mysqlExpectedTables 返回部署侧应提前建好的表结构期望值。
// mysqlExpectedTables returns the schema the deployer must create ahead of time.
func mysqlExpectedTables() []mysqlTableSpec {
	// Agent: 与 mysqlSQLs 里 CREATE 定义对齐；应用只检查，不建表。
	return []mysqlTableSpec{
		{
			Name: "projects",
			Columns: []mysqlColumnSpec{
				{"id", "varchar(128)"}, {"entity", "varchar(512)"}, {"name", "varchar(191)"},
				{"created_at", "datetime"}, {"deleted_at", "datetime"},
			},
			Indexes: []string{"PRIMARY", "uq_projects_entity_name"},
		},
		{
			Name: "runs",
			Columns: []mysqlColumnSpec{
				{"id", "varchar(128)"}, {"project_id", "varchar(128)"}, {"name", "varchar(255)"},
				{"display_name", "varchar(255)"}, {"config", "longtext"}, {"summary", "longtext"},
				{"state", "varchar(32)"}, {"host", "varchar(1024)"}, {"program", "varchar(1024)"},
				{"git_commit", "varchar(128)"}, {"tags", "longtext"}, {"notes", "longtext"},
				{"group_name", "varchar(255)"}, {"job_type", "varchar(255)"}, {"sweep_name", "varchar(255)"},
				{"history_line_count", "int"}, {"events_line_count", "int"}, {"log_line_count", "int"},
				{"received_history_count", "int"}, {"fork_run_id", "varchar(128)"}, {"fork_step", "int"},
				{"created_at", "datetime"}, {"updated_at", "datetime"}, {"heartbeat_at", "datetime"},
				{"deleted_at", "datetime"},
			},
			Indexes: []string{"PRIMARY", "idx_runs_project_id"},
		},
		{
			Name: "history_scalars",
			Columns: []mysqlColumnSpec{
				{"run_id", "varchar(128)"}, {"step", "int"}, {"key", "varchar(255)"},
				{"value", "double"}, {"x_step", "double"},
			},
			Indexes: []string{"PRIMARY", "idx_history_scalars_run_step"},
		},
		{
			Name: "history_histograms",
			Columns: []mysqlColumnSpec{
				{"run_id", "varchar(128)"}, {"step", "int"}, {"key", "varchar(255)"},
				{"x_step", "double"}, {"bins", "longtext"}, {"vals", "longtext"},
			},
			Indexes: []string{"PRIMARY"},
		},
		{
			Name: "run_steps",
			Columns: []mysqlColumnSpec{
				{"run_id", "varchar(128)"}, {"step", "int"},
			},
			Indexes: []string{"PRIMARY", "idx_run_steps_run_step"},
		},
		{
			Name: "run_keys",
			Columns: []mysqlColumnSpec{
				{"run_id", "varchar(128)"}, {"key", "varchar(255)"},
			},
			Indexes: []string{"PRIMARY"},
		},
		{
			Name: "system_events",
			Columns: []mysqlColumnSpec{
				{"id", "bigint"}, {"run_id", "varchar(128)"}, {"line_num", "int"},
				{"data", "longtext"}, {"timestamp", "datetime"},
			},
			Indexes: []string{"PRIMARY", "idx_system_events_run_id"},
		},
		{
			Name: "console_logs",
			Columns: []mysqlColumnSpec{
				{"id", "bigint"}, {"run_id", "varchar(128)"}, {"line_num", "int"},
				{"line", "longtext"}, {"stream", "varchar(32)"}, {"timestamp", "datetime"},
			},
			Indexes: []string{"PRIMARY", "idx_console_logs_run_id"},
		},
		{
			Name: "artifacts",
			Columns: []mysqlColumnSpec{
				{"id", "varchar(128)"}, {"run_id", "varchar(128)"}, {"type", "varchar(255)"},
				{"name", "varchar(255)"}, {"digest", "varchar(255)"}, {"state", "varchar(32)"},
				{"metadata", "longtext"}, {"created_at", "datetime"},
			},
			Indexes: []string{"PRIMARY"},
		},
		{
			Name: "files",
			Columns: []mysqlColumnSpec{
				{"id", "varchar(128)"}, {"run_id", "varchar(128)"}, {"name", "varchar(1024)"},
				{"url", "longtext"}, {"size", "bigint"}, {"created_at", "datetime"},
			},
			Indexes: []string{"PRIMARY"},
		},
	}
}

// migrateEngine SQLite 执行建表；MySQL 只检查表结构与索引，不创建。
// migrateEngine creates tables on SQLite; on MySQL it only checks schema and indexes.
func (db *DB) migrateEngine() error {
	// Agent: MySQL 表由部署侧提前建好，应用禁止 CREATE / ALTER。
	if db.isMySQL() {
		return db.checkMySQLSchema()
	}
	for _, id := range schemaSQLIDs() {
		stmt := db.SQL(id)
		if stmt == "" || stmt == "SELECT 1" {
			continue
		}
		if _, err := db.DB.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed id=%d: %w", id, err)
		}
	}
	for _, id := range alterSQLIDs() {
		stmt := db.SQL(id)
		if stmt == "" || stmt == "SELECT 1" {
			continue
		}
		// Agent: SQLite 重复 ADD COLUMN 可忽略；其它错误在 Exec 失败时由调用方处理。
		_, _ = db.DB.Exec(stmt)
	}
	return nil
}

// checkMySQLSchema 核对表、列类型和索引；通过或失败都打出明显日志。
// checkMySQLSchema verifies tables, column types, and indexes; pass and fail both emit prominent logs.
func (db *DB) checkMySQLSchema() error {
	tables := mysqlExpectedTables()
	var issues []string
	for _, table := range tables {
		// Agent: 缺表直接记错误，不再查列和索引。
		var n int
		if err := db.DB.QueryRow(`
			SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ? AND table_type = 'BASE TABLE'`, table.Name).Scan(&n); err != nil {
			issues = append(issues, fmt.Sprintf("table %s: %v", table.Name, err))
			continue
		}
		if n == 0 {
			issues = append(issues, fmt.Sprintf("missing table %s", table.Name))
			continue
		}
		cols, err := db.mysqlColumnTypes(table.Name)
		if err != nil {
			issues = append(issues, fmt.Sprintf("columns %s: %v", table.Name, err))
			continue
		}
		for _, want := range table.Columns {
			got, ok := cols[want.Name]
			if !ok {
				issues = append(issues, fmt.Sprintf("table %s missing column %s", table.Name, want.Name))
				continue
			}
			// Agent: 只比对类型前缀，忽略显示宽度和 collation 细节。
			if !mysqlTypeCompatible(got, want.Type) {
				issues = append(issues, fmt.Sprintf("table %s column %s type %s want %s", table.Name, want.Name, got, want.Type))
			}
		}
		indexes, err := db.mysqlIndexNames(table.Name)
		if err != nil {
			issues = append(issues, fmt.Sprintf("indexes %s: %v", table.Name, err))
			continue
		}
		for _, want := range table.Indexes {
			if !indexes[strings.ToLower(want)] {
				issues = append(issues, fmt.Sprintf("table %s missing index %s", table.Name, want))
			}
		}
	}
	if len(issues) > 0 {
		// Agent: 失败时用分隔线包住，逐条 ERROR，方便运维一眼看到。
		log.Printf("========== [MySQL SCHEMA CHECK FAILED] issues=%d ==========", len(issues))
		for i, msg := range issues {
			log.Printf("[mysql-schema] ERROR (%d/%d) %s", i+1, len(issues), msg)
		}
		log.Printf("========== [MySQL SCHEMA CHECK FAILED] fix tables/indexes then restart ==========")
		return fmt.Errorf("mysql schema check failed (%d issues)", len(issues))
	}
	// Agent: 全部通过时打一行醒目成功日志，含表数量。
	log.Printf("========== [MySQL SCHEMA CHECK PASSED] tables=%d columns/indexes OK ==========", len(tables))
	return nil
}

// mysqlColumnTypes 读取一张表的列名到 column_type。
// mysqlColumnTypes maps column names to column_type for one table.
func (db *DB) mysqlColumnTypes(table string) (map[string]string, error) {
	rows, err := db.DB.Query(`
		SELECT column_name, column_type FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[strings.ToLower(name)] = strings.ToLower(typ)
	}
	return out, rows.Err()
}

// mysqlIndexNames 读取一张表的索引名集合。
// mysqlIndexNames returns the set of index names for one table.
func (db *DB) mysqlIndexNames(table string) (map[string]bool, error) {
	rows, err := db.DB.Query(`
		SELECT DISTINCT index_name FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[strings.ToLower(name)] = true
	}
	return out, rows.Err()
}

// mysqlTypeCompatible 判断实际 column_type 是否满足期望类型片段。
// mysqlTypeCompatible reports whether an actual column_type matches the expected fragment.
func mysqlTypeCompatible(got, want string) bool {
	got = strings.ToLower(strings.TrimSpace(got))
	want = strings.ToLower(strings.TrimSpace(want))
	if got == want || strings.HasPrefix(got, want) {
		return true
	}
	// Agent: int(11) 与 int、bigint unsigned 等显示宽度差异视为兼容。
	if want == "int" && (strings.HasPrefix(got, "int(") || got == "int") {
		return true
	}
	if want == "bigint" && (strings.HasPrefix(got, "bigint") || strings.HasPrefix(got, "bigint(")) {
		return true
	}
	if want == "double" && (got == "double" || strings.HasPrefix(got, "double")) {
		return true
	}
	return false
}

// execIgnoreBatches 按行数和包大小拆开批量 INSERT。前缀用编号从 sql_manager 取。
// execIgnoreBatches splits a bulk INSERT by row count and packet size. The prefix comes from sql_manager by ID.
func (db *DB) execIgnoreBatches(tx *sql.Tx, prefixID SQLID, rowPH string, perRow int, args []any) error {
	if perRow <= 0 || (len(args) > 0 && len(args)%perRow != 0) {
		return fmt.Errorf("invalid row width")
	}
	prefix := db.SQL(prefixID)
	for len(args) > 0 {
		n := len(args) / perRow
		if n > multiRowSize {
			n = multiRowSize
		}
		// Agent: 直方图单行可能很大，超出包大小就减半，直到只剩一行。
		for n > 1 && estimateInsert(prefix, rowPH, n, args[:n*perRow]) > maxSQLPacketBytes {
			n /= 2
		}
		query := prefix
		for i := 0; i < n; i++ {
			if i > 0 {
				query += ","
			}
			query += rowPH
		}
		if _, err := tx.Exec(query, args[:n*perRow]...); err != nil {
			return err
		}
		args = args[n*perRow:]
	}
	return nil
}

// estimateInsert 估算一条批量插入的字节数，用来提前拆批。
// estimateInsert estimates the bytes of one bulk insert so it can be split first.
func estimateInsert(prefix, rowPH string, n int, args []any) int {
	size := len(prefix) + n*len(rowPH) + n
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			size += len(v)
		default:
			size += 8
		}
	}
	return size
}
