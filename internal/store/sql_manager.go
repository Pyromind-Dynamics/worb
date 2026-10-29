package store

import (
	"fmt"
	"strings"
)

// SQLID 是业务 SQL 的编号。调用方只传编号，不直接拼引擎差异。
// SQLID is the number of a business statement. Callers pass the ID instead of engine-specific text.
type SQLID int

// Agent: 编号按功能分段。1–99 建表，100–199 项目/run，200–299 指标，300–399 清理，400–499 拼装模板。
const (
	SQLCreateProjects SQLID = 1
	SQLCreateRuns     SQLID = 2
	SQLCreateScalars  SQLID = 3
	SQLCreateHists    SQLID = 4
	SQLCreateRunSteps SQLID = 5
	SQLCreateSysEv    SQLID = 6
	SQLCreateConsLog  SQLID = 7
	SQLCreateArts     SQLID = 8
	SQLCreateFiles    SQLID = 9
	SQLIdxSysEvRun    SQLID = 10
	SQLIdxConsLogRun  SQLID = 11
	SQLIdxRunsProject SQLID = 12
	SQLAlterRunDel    SQLID = 13
	SQLAlterProjDel   SQLID = 14
	SQLAlterRecvHist  SQLID = 15
	SQLAlterForkRun   SQLID = 16
	SQLAlterForkStep  SQLID = 17
	SQLCreateRunKeys  SQLID = 18
	SQLIdxScalarsStep SQLID = 19
	SQLIdxRunSteps    SQLID = 20

	SQLSelectProjectByEntityName SQLID = 100
	SQLInsertProject             SQLID = 101
	SQLSelectRunIDExists         SQLID = 102
	SQLSelectRunIDByName         SQLID = 103
	SQLSelectRunIDByDisplay      SQLID = 104
	SQLInsertRun                 SQLID = 105
	SQLFinishRun                 SQLID = 106
	SQLIncrReceivedHistory       SQLID = 107
	SQLSoftDeleteRun             SQLID = 108
	SQLSoftDeleteRunsByProject   SQLID = 109
	SQLSoftDeleteProject         SQLID = 110
	SQLUpdateRunSummary          SQLID = 111
	SQLSetHistoryLineCount       SQLID = 112
	SQLMarkRunFailed             SQLID = 113
	SQLUpdateFileSize            SQLID = 114
	SQLInsertFileIgnore          SQLID = 115
	SQLListFilesByRun            SQLID = 116
	SQLSelectProjectByID         SQLID = 117
	SQLSelectProjectByName       SQLID = 118
	SQLListAllProjects           SQLID = 119
	SQLListProjectsByEntity      SQLID = 120
	SQLCountRunsByName           SQLID = 121
	SQLCountRunsByDisplay        SQLID = 122
	SQLListRunsLite              SQLID = 123
	SQLUpdateForkedRun           SQLID = 124
	SQLInsertForkedRun           SQLID = 125
	SQLRunSelectByID             SQLID = 126
	SQLRunSelectByProject        SQLID = 127
	SQLRunSelectFiltered         SQLID = 128
	SQLRunSelectNone             SQLID = 129
	SQLUpdateRunBase             SQLID = 130
	SQLSetCounterColumn          SQLID = 131
	SQLInsertArtifact            SQLID = 132
	SQLSelectArtifact            SQLID = 133
	SQLCommitArtifact            SQLID = 134

	SQLInsertScalarsPrefix    SQLID = 200
	SQLInsertRunStepsPrefix   SQLID = 201
	SQLInsertRunKeysPrefix    SQLID = 202
	SQLInsertHistsPrefix      SQLID = 203
	SQLInsertSysEvent         SQLID = 204
	SQLInsertConsLog          SQLID = 205
	SQLSelectRunKeys          SQLID = 206
	SQLSelectHistoryLineCount SQLID = 207
	SQLCountRunSteps          SQLID = 208
	SQLBackfillRunSteps       SQLID = 209
	SQLCountDistinctSteps     SQLID = 210
	SQLSelectScalarsByKey     SQLID = 211
	SQLSelectScalarsBucket    SQLID = 212
	SQLSelectProjectRuns      SQLID = 213
	SQLMaxHistoryLineCount    SQLID = 214
	SQLSelectScalarsByRun     SQLID = 215
	SQLSelectScalarsBucketKey SQLID = 216
	SQLSelectHistogramsByRun  SQLID = 217
	SQLSelectConsoleLines     SQLID = 218
	SQLCopyRunKeys            SQLID = 219
	SQLSelectParentScalars    SQLID = 220
	SQLSelectParentHists      SQLID = 221
	SQLMaxParentStep          SQLID = 222
	SQLCountSysEvents         SQLID = 223
	SQLCountConsLogs          SQLID = 224
	SQLActiveRunsEmpty        SQLID = 225
	SQLActiveRunsIn           SQLID = 226
	SQLStorageUsage           SQLID = 227

	SQLSelectDeletedRun     SQLID = 300
	SQLSelectDeletedProject SQLID = 301
	SQLDeleteRunHard        SQLID = 302
	SQLDeleteProjectHard    SQLID = 303
	SQLSelectOrphanProject  SQLID = 304
	SQLDeleteScalarsBatch   SQLID = 305
	SQLDeleteHistsBatch     SQLID = 306
	SQLDeleteStepsBatch     SQLID = 307
	SQLDeleteKeysBatch      SQLID = 308
	SQLDeleteSysEvBatch     SQLID = 309
	SQLDeleteConsBatch      SQLID = 310
	SQLDeleteFilesBatch     SQLID = 311
	SQLDeleteArtsBatch      SQLID = 312
)

// Agent: SQLite 与 MySQL 各一份常量表。编号相同，文本按引擎语义分开写。
var sqliteSQLs = map[SQLID]string{}
var mysqlSQLs = map[SQLID]string{}

func init() {
	// Agent: 启动时只装一次映射，运行中按编号取值，不做字符串改写。
	loadSQLiteSQLs()
	loadMySQLSQLs()
}

// SQL 按 *DB 的引擎和编号返回对应语句。找不到编号时返回空串。
// SQL returns the statement for the DB engine and ID. An unknown ID yields an empty string.
func (db *DB) SQL(id SQLID) string {
	if db != nil && db.isMySQL() {
		if s, ok := mysqlSQLs[id]; ok {
			return s
		}
		return ""
	}
	if s, ok := sqliteSQLs[id]; ok {
		return s
	}
	return ""
}

// SQLByEngine 不依赖 *DB，按引擎名和编号取语句。供测试使用。
// SQLByEngine returns a statement by engine name and ID for tests.
func SQLByEngine(engine string, id SQLID) string {
	if engine == "mysql" {
		return mysqlSQLs[id]
	}
	return sqliteSQLs[id]
}

// schemaSQLIDs 返回建表语句编号，顺序固定。
// schemaSQLIDs returns create-table IDs in a fixed order.
func schemaSQLIDs() []SQLID {
	return []SQLID{
		SQLCreateProjects, SQLCreateRuns, SQLCreateScalars, SQLCreateHists, SQLCreateRunSteps,
		SQLCreateSysEv, SQLCreateConsLog, SQLCreateArts, SQLCreateFiles,
		SQLIdxSysEvRun, SQLIdxConsLogRun, SQLIdxRunsProject,
	}
}

// alterSQLIDs 返回补列和补表编号。
// alterSQLIDs returns alter and late-create IDs.
func alterSQLIDs() []SQLID {
	return []SQLID{
		SQLAlterRunDel, SQLAlterProjDel, SQLAlterRecvHist, SQLAlterForkRun, SQLAlterForkStep,
		SQLCreateRunKeys, SQLIdxScalarsStep, SQLIdxRunSteps,
	}
}

// SQLActiveRunIDs 按 run 数量拼 IN 列表。
// SQLActiveRunIDs builds an IN list sized to the number of runs.
func (db *DB) SQLActiveRunIDs(n int) string {
	if n <= 0 {
		return db.SQL(SQLActiveRunsEmpty)
	}
	return strings.Replace(db.SQL(SQLActiveRunsIn), "{in}", strings.Repeat("?,", n-1)+"?", 1)
}

// SQLUpdateRun 按本次请求里出现的列拼 UPDATE。列名只能来自白名单。
// SQLUpdateRun builds an UPDATE from the columns present on this request.
func (db *DB) SQLUpdateRun(extra []string) (string, error) {
	allowed := map[string]bool{
		"state = ?": true, "config = ?": true, "summary = ?": true, "display_name = ?": true,
		"tags = ?": true, "notes = ?": true, "host = ?": true, "program = ?": true,
	}
	sets := db.SQL(SQLUpdateRunBase)
	for _, item := range extra {
		if !allowed[item] {
			return "", fmt.Errorf("refuse run update %q", item)
		}
		sets += ", " + item
	}
	return sets + " WHERE id = ?", nil
}

// SQLSetRunColumn 更新某一个计数字段。
// SQLSetRunColumn updates one counter column.
func (db *DB) SQLSetRunColumn(column string) (string, error) {
	switch column {
	case "history_line_count", "events_line_count", "log_line_count":
		return strings.Replace(db.SQL(SQLSetCounterColumn), "{col}", column, 1), nil
	default:
		return "", fmt.Errorf("unknown counter column %s", column)
	}
}

// SQLRunSelect 返回 run 查询。kind 为 byID / byProject / filtered。
// SQLRunSelect returns a run SELECT. kind is byID, byProject, or filtered.
func (db *DB) SQLRunSelect(kind string) string {
	switch kind {
	case "byID":
		return db.SQL(SQLRunSelectByID)
	case "byProject":
		return db.SQL(SQLRunSelectByProject)
	case "filtered":
		return db.SQL(SQLRunSelectFiltered)
	default:
		return db.SQL(SQLRunSelectNone)
	}
}

// SQLDeleteBatch 按表名返回分批删除语句。
// SQLDeleteBatch returns a batched delete for a known table.
func (db *DB) SQLDeleteBatch(table, column string) (string, error) {
	if column != "run_id" {
		return "", fmt.Errorf("refuse delete column %s", column)
	}
	var id SQLID
	switch table {
	case "history_scalars":
		id = SQLDeleteScalarsBatch
	case "history_histograms":
		id = SQLDeleteHistsBatch
	case "run_steps":
		id = SQLDeleteStepsBatch
	case "run_keys":
		id = SQLDeleteKeysBatch
	case "system_events":
		id = SQLDeleteSysEvBatch
	case "console_logs":
		id = SQLDeleteConsBatch
	case "files":
		id = SQLDeleteFilesBatch
	case "artifacts":
		id = SQLDeleteArtsBatch
	default:
		return "", fmt.Errorf("refuse delete table %s", table)
	}
	return db.SQL(id), nil
}

// SQLInsertPrefix 返回批量 INSERT 的前缀。
// SQLInsertPrefix returns the bulk INSERT prefix for a statement ID.
func (db *DB) SQLInsertPrefix(id SQLID) string {
	return db.SQL(id)
}

// SQLScalarsBucket 拼降采样查询。bucketSize 写入整数除法位置。
// SQLScalarsBucket builds a downsampling query and inserts the bucket size.
func (db *DB) SQLScalarsBucket(bucketSize int) string {
	return strings.ReplaceAll(db.SQL(SQLSelectScalarsBucket), "{n}", fmt.Sprintf("%d", bucketSize))
}

// SQLProjectScalarsBucket 拼项目级降采样查询。
// SQLProjectScalarsBucket builds a project-level downsampling query.
func (db *DB) SQLProjectScalarsBucket(bucketSize int) string {
	return strings.ReplaceAll(db.SQL(SQLSelectScalarsBucketKey), "{n}", fmt.Sprintf("%d", bucketSize))
}

func loadSQLiteSQLs() {
	sqliteSQLs = map[SQLID]string{
		SQLCreateProjects: `CREATE TABLE IF NOT EXISTS projects (
			id TEXT PRIMARY KEY, entity TEXT NOT NULL DEFAULT 'local', name TEXT NOT NULL,
			created_at TEXT DEFAULT (datetime('now')), UNIQUE(entity, name))`,
		SQLCreateRuns: `CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), name TEXT NOT NULL,
			display_name TEXT, config TEXT, summary TEXT, state TEXT DEFAULT 'running', host TEXT, program TEXT,
			git_commit TEXT, tags TEXT DEFAULT '[]', notes TEXT, group_name TEXT, job_type TEXT, sweep_name TEXT,
			history_line_count INTEGER DEFAULT 0, events_line_count INTEGER DEFAULT 0, log_line_count INTEGER DEFAULT 0,
			created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')),
			heartbeat_at TEXT DEFAULT (datetime('now')))`,
		SQLCreateScalars: `CREATE TABLE IF NOT EXISTS history_scalars (
			run_id TEXT NOT NULL REFERENCES runs(id), step INTEGER NOT NULL, key TEXT NOT NULL,
			value REAL NOT NULL, x_step REAL, PRIMARY KEY (run_id, key, step)) WITHOUT ROWID`,
		SQLCreateHists: `CREATE TABLE IF NOT EXISTS history_histograms (
			run_id TEXT NOT NULL REFERENCES runs(id), step INTEGER NOT NULL, key TEXT NOT NULL,
			x_step REAL, bins TEXT NOT NULL, vals TEXT NOT NULL, PRIMARY KEY (run_id, key, step)) WITHOUT ROWID`,
		SQLCreateRunSteps: `CREATE TABLE IF NOT EXISTS run_steps (
			run_id TEXT NOT NULL REFERENCES runs(id), step INTEGER NOT NULL, PRIMARY KEY (run_id, step)) WITHOUT ROWID`,
		SQLCreateSysEv: `CREATE TABLE IF NOT EXISTS system_events (
			run_id TEXT NOT NULL REFERENCES runs(id), line_num INTEGER NOT NULL, data TEXT NOT NULL,
			timestamp TEXT DEFAULT (datetime('now')))`,
		SQLCreateConsLog: `CREATE TABLE IF NOT EXISTS console_logs (
			run_id TEXT NOT NULL REFERENCES runs(id), line_num INTEGER NOT NULL, line TEXT NOT NULL,
			stream TEXT DEFAULT 'stdout', timestamp TEXT DEFAULT (datetime('now')))`,
		SQLCreateArts: `CREATE TABLE IF NOT EXISTS artifacts (
			id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), type TEXT NOT NULL, name TEXT NOT NULL,
			digest TEXT, state TEXT DEFAULT 'pending', metadata TEXT, created_at TEXT DEFAULT (datetime('now')))`,
		SQLCreateFiles: `CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), name TEXT NOT NULL, url TEXT,
			size INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')))`,
		SQLIdxSysEvRun:               `CREATE INDEX IF NOT EXISTS idx_system_events_run_id ON system_events(run_id)`,
		SQLIdxConsLogRun:             `CREATE INDEX IF NOT EXISTS idx_console_logs_run_id ON console_logs(run_id)`,
		SQLIdxRunsProject:            `CREATE INDEX IF NOT EXISTS idx_runs_project_id ON runs(project_id)`,
		SQLAlterRunDel:               `ALTER TABLE runs ADD COLUMN deleted_at TEXT`,
		SQLAlterProjDel:              `ALTER TABLE projects ADD COLUMN deleted_at TEXT`,
		SQLAlterRecvHist:             `ALTER TABLE runs ADD COLUMN received_history_count INTEGER DEFAULT 0`,
		SQLAlterForkRun:              `ALTER TABLE runs ADD COLUMN fork_run_id TEXT`,
		SQLAlterForkStep:             `ALTER TABLE runs ADD COLUMN fork_step INTEGER`,
		SQLCreateRunKeys:             `CREATE TABLE IF NOT EXISTS run_keys (run_id TEXT NOT NULL REFERENCES runs(id), key TEXT NOT NULL, PRIMARY KEY (run_id, key)) WITHOUT ROWID`,
		SQLIdxScalarsStep:            `CREATE INDEX IF NOT EXISTS idx_history_scalars_run_step ON history_scalars(run_id, step)`,
		SQLIdxRunSteps:               `CREATE INDEX IF NOT EXISTS idx_run_steps_run_step ON run_steps(run_id, step)`,
		SQLSelectProjectByEntityName: `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL`,
		SQLInsertProject:             `INSERT INTO projects (id, entity, name) VALUES (?, ?, ?)`,
		SQLSelectRunIDExists:         `SELECT id FROM runs WHERE id = ? AND deleted_at IS NULL`,
		SQLSelectRunIDByName:         `SELECT id FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL`,
		SQLSelectRunIDByDisplay:      `SELECT id FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL`,
		SQLInsertRun: `INSERT INTO runs (id, project_id, name, display_name, config, summary, state, host, program, git_commit, tags, notes, group_name, job_type, sweep_name)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		SQLFinishRun:               `UPDATE runs SET state = 'finished', updated_at = current_timestamp WHERE id = ?`,
		SQLIncrReceivedHistory:     `UPDATE runs SET received_history_count = received_history_count + ? WHERE id = ?`,
		SQLSoftDeleteRun:           `UPDATE runs SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL`,
		SQLSoftDeleteRunsByProject: `UPDATE runs SET deleted_at = current_timestamp WHERE project_id = ? AND deleted_at IS NULL`,
		SQLSoftDeleteProject:       `UPDATE projects SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL`,
		SQLUpdateRunSummary:        `UPDATE runs SET summary = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLSetHistoryLineCount:     `UPDATE runs SET history_line_count = ? WHERE id = ?`,
		SQLMarkRunFailed:           `UPDATE runs SET state = 'failed', notes = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLUpdateFileSize:          `UPDATE files SET size = ? WHERE id = ?`,
		SQLInsertFileIgnore:        `INSERT OR IGNORE INTO files (id, run_id, name, url) VALUES (?, ?, ?, ?)`,
		SQLListFilesByRun:          `SELECT id, run_id, name, COALESCE(url, ''), COALESCE(size, 0), created_at FROM files WHERE run_id = ? ORDER BY name`,
		SQLSelectProjectByID:       `SELECT id, entity, name, created_at FROM projects WHERE id = ? AND deleted_at IS NULL`,
		SQLSelectProjectByName:     `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL`,
		SQLListAllProjects:         `SELECT id, entity, name, created_at FROM projects WHERE deleted_at IS NULL ORDER BY created_at DESC`,
		SQLListProjectsByEntity:    `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND deleted_at IS NULL ORDER BY created_at DESC`,
		SQLCountRunsByName:         `SELECT COUNT(*) FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL`,
		SQLCountRunsByDisplay:      `SELECT COUNT(*) FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL`,
		SQLListRunsLite: `SELECT r.id, r.project_id, r.name, r.display_name, r.state,
			r.history_line_count, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`,
		SQLUpdateForkedRun: `UPDATE runs SET config = ?, tags = ?, notes = ?, host = ?, program = ?,
			git_commit = ?, group_name = ?, job_type = ?, sweep_name = ?,
			fork_run_id = ?, fork_step = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLInsertForkedRun: `INSERT INTO runs (id, project_id, name, display_name, config, summary, state,
			host, program, git_commit, tags, notes, group_name, job_type, sweep_name, fork_run_id, fork_step)
			VALUES (?, ?, ?, ?, ?, '{}', 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		SQLRunSelectByID: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.id = ? AND r.deleted_at IS NULL`,
		SQLRunSelectByProject: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`,
		SQLRunSelectFiltered: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL`,
		SQLRunSelectNone:    `SELECT r.id FROM runs r WHERE 1 = 0`,
		SQLUpdateRunBase:    `UPDATE runs SET updated_at = current_timestamp, heartbeat_at = current_timestamp`,
		SQLSetCounterColumn: `UPDATE runs SET {col} = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLInsertArtifact:   `INSERT INTO artifacts (id, run_id, type, name, metadata) VALUES (?, ?, ?, ?, ?)`,
		SQLSelectArtifact:   `SELECT id, run_id, type, name, digest, state, metadata, created_at FROM artifacts WHERE id = ?`,
		SQLCommitArtifact:   `UPDATE artifacts SET state = 'committed', digest = ? WHERE id = ?`,

		SQLInsertScalarsPrefix:    `INSERT OR IGNORE INTO history_scalars (run_id, step, key, value, x_step) VALUES `,
		SQLInsertRunStepsPrefix:   `INSERT OR IGNORE INTO run_steps (run_id, step) VALUES `,
		SQLInsertRunKeysPrefix:    `INSERT OR IGNORE INTO run_keys (run_id, key) VALUES `,
		SQLInsertHistsPrefix:      `INSERT OR IGNORE INTO history_histograms (run_id, step, key, x_step, bins, vals) VALUES `,
		SQLInsertSysEvent:         `INSERT INTO system_events (run_id, line_num, data) VALUES (?, ?, ?)`,
		SQLInsertConsLog:          `INSERT INTO console_logs (run_id, line_num, line, stream) VALUES (?, ?, ?, 'stdout')`,
		SQLSelectRunKeys:          `SELECT key FROM run_keys WHERE run_id = ? ORDER BY key`,
		SQLSelectHistoryLineCount: `SELECT history_line_count FROM runs WHERE id = ?`,
		SQLCountRunSteps:          `SELECT COUNT(*) FROM run_steps WHERE run_id = ?`,
		SQLBackfillRunSteps: `INSERT OR IGNORE INTO run_steps (run_id, step)
			SELECT run_id, step FROM history_scalars WHERE run_id = ? GROUP BY run_id, step`,
		SQLCountDistinctSteps:  `SELECT COUNT(DISTINCT step) FROM history_scalars WHERE run_id = ?`,
		SQLSelectScalarsByKey:  `SELECT COALESCE(x_step, step), value, step FROM history_scalars WHERE run_id = ? AND key = ?%s ORDER BY step`,
		SQLSelectScalarsBucket: `SELECT AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value) FROM history_scalars WHERE run_id = ? AND key = ?%s GROUP BY step / {n} ORDER BY step / {n}`,
		SQLSelectProjectRuns: `SELECT r.id, COALESCE(NULLIF(r.display_name, ''), r.name)
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at`,
		SQLMaxHistoryLineCount:    `SELECT COALESCE(MAX(history_line_count), 0) FROM runs WHERE project_id = ? AND deleted_at IS NULL`,
		SQLSelectScalarsByRun:     `SELECT key, COALESCE(x_step, step) as x, value, step FROM history_scalars WHERE run_id = ? ORDER BY step, key`,
		SQLSelectScalarsBucketKey: `SELECT key, AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value) FROM history_scalars WHERE run_id = ? GROUP BY key, step / {n} ORDER BY step / {n}, key`,
		SQLSelectHistogramsByRun:  `SELECT step, key, x_step, bins, vals FROM history_histograms WHERE run_id = ? ORDER BY step`,
		SQLSelectConsoleLines:     `SELECT line FROM console_logs WHERE run_id = ? ORDER BY line_num`,
		SQLCopyRunKeys:            `INSERT OR IGNORE INTO run_keys (run_id, key) SELECT ?, key FROM run_keys WHERE run_id = ?`,
		SQLSelectParentScalars:    `SELECT step, key, value, x_step FROM history_scalars WHERE run_id = ? AND step <= ? ORDER BY step, key LIMIT ? OFFSET ?`,
		SQLSelectParentHists:      `SELECT step, key, x_step, bins, vals FROM history_histograms WHERE run_id = ? AND step <= ? ORDER BY step, key LIMIT ? OFFSET ?`,
		SQLMaxParentStep:          `SELECT COALESCE(MAX(step), -1) FROM run_steps WHERE run_id = ?`,
		SQLCountSysEvents:         `SELECT COUNT(*) FROM system_events WHERE run_id = ?`,
		SQLCountConsLogs:          `SELECT COUNT(*) FROM console_logs WHERE run_id = ?`,
		SQLActiveRunsEmpty:        `SELECT id FROM runs WHERE 1 = 0`,
		SQLActiveRunsIn:           `SELECT id FROM runs WHERE id IN ({in}) AND deleted_at IS NULL`,
		SQLStorageUsage:           storageUsageSQLSQLite,

		SQLSelectDeletedRun:     `SELECT id FROM runs WHERE deleted_at IS NOT NULL LIMIT 1`,
		SQLSelectDeletedProject: `SELECT id FROM projects WHERE deleted_at IS NOT NULL LIMIT 1`,
		SQLDeleteRunHard:        `DELETE FROM runs WHERE id = ?`,
		SQLDeleteProjectHard:    `DELETE FROM projects WHERE id = ?`,
		SQLSelectOrphanProject: `SELECT p.id FROM projects p WHERE p.deleted_at IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.project_id = p.id) LIMIT 1`,
		SQLDeleteScalarsBatch: `DELETE FROM history_scalars WHERE (run_id, key, step) IN (SELECT run_id, key, step FROM history_scalars WHERE run_id = ? LIMIT ?)`,
		SQLDeleteHistsBatch:   `DELETE FROM history_histograms WHERE (run_id, key, step) IN (SELECT run_id, key, step FROM history_histograms WHERE run_id = ? LIMIT ?)`,
		SQLDeleteStepsBatch:   `DELETE FROM run_steps WHERE (run_id, step) IN (SELECT run_id, step FROM run_steps WHERE run_id = ? LIMIT ?)`,
		SQLDeleteKeysBatch:    `DELETE FROM run_keys WHERE (run_id, key) IN (SELECT run_id, key FROM run_keys WHERE run_id = ? LIMIT ?)`,
		SQLDeleteSysEvBatch:   `DELETE FROM system_events WHERE rowid IN (SELECT rowid FROM system_events WHERE run_id = ? LIMIT ?)`,
		SQLDeleteConsBatch:    `DELETE FROM console_logs WHERE rowid IN (SELECT rowid FROM console_logs WHERE run_id = ? LIMIT ?)`,
		SQLDeleteFilesBatch:   `DELETE FROM files WHERE rowid IN (SELECT rowid FROM files WHERE run_id = ? LIMIT ?)`,
		SQLDeleteArtsBatch:    `DELETE FROM artifacts WHERE rowid IN (SELECT rowid FROM artifacts WHERE run_id = ? LIMIT ?)`,
	}
}

const mysqlEngineTail = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

func loadMySQLSQLs() {
	mysqlSQLs = map[SQLID]string{
		SQLCreateProjects: `CREATE TABLE IF NOT EXISTS projects (
			id VARCHAR(128) NOT NULL, entity VARCHAR(512) NOT NULL DEFAULT 'local', name VARCHAR(191) NOT NULL,
			created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP, deleted_at DATETIME NULL,
			PRIMARY KEY (id), UNIQUE KEY uq_projects_entity_name (entity, name))` + mysqlEngineTail,
		SQLCreateRuns: `CREATE TABLE IF NOT EXISTS runs (
			id VARCHAR(128) NOT NULL, project_id VARCHAR(128) NOT NULL, name VARCHAR(255) NOT NULL,
			display_name VARCHAR(255) NULL, config LONGTEXT, summary LONGTEXT, state VARCHAR(32) DEFAULT 'running',
			host VARCHAR(1024), program VARCHAR(1024), git_commit VARCHAR(128), tags LONGTEXT, notes LONGTEXT,
			group_name VARCHAR(255), job_type VARCHAR(255), sweep_name VARCHAR(255),
			history_line_count INT NOT NULL DEFAULT 0, events_line_count INT NOT NULL DEFAULT 0, log_line_count INT NOT NULL DEFAULT 0,
			received_history_count INT NOT NULL DEFAULT 0, fork_run_id VARCHAR(128) NULL, fork_step INT NULL,
			created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
			heartbeat_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP, deleted_at DATETIME NULL,
			PRIMARY KEY (id), KEY idx_runs_project_id (project_id),
			CONSTRAINT fk_runs_project FOREIGN KEY (project_id) REFERENCES projects (id))` + mysqlEngineTail,
		SQLCreateScalars: `CREATE TABLE IF NOT EXISTS history_scalars (
			run_id VARCHAR(128) NOT NULL, step INT NOT NULL, ` + "`key`" + ` VARCHAR(255) NOT NULL,
			value DOUBLE NOT NULL, x_step DOUBLE NULL, PRIMARY KEY (run_id, ` + "`key`" + `, step),
			KEY idx_history_scalars_run_step (run_id, step),
			CONSTRAINT fk_history_scalars_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateHists: `CREATE TABLE IF NOT EXISTS history_histograms (
			run_id VARCHAR(128) NOT NULL, step INT NOT NULL, ` + "`key`" + ` VARCHAR(255) NOT NULL,
			x_step DOUBLE NULL, bins LONGTEXT NOT NULL, vals LONGTEXT NOT NULL,
			PRIMARY KEY (run_id, ` + "`key`" + `, step),
			CONSTRAINT fk_history_histograms_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateRunSteps: `CREATE TABLE IF NOT EXISTS run_steps (
			run_id VARCHAR(128) NOT NULL, step INT NOT NULL, PRIMARY KEY (run_id, step),
			KEY idx_run_steps_run_step (run_id, step),
			CONSTRAINT fk_run_steps_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateSysEv: `CREATE TABLE IF NOT EXISTS system_events (
			id BIGINT NOT NULL AUTO_INCREMENT, run_id VARCHAR(128) NOT NULL, line_num INT NOT NULL,
			data LONGTEXT NOT NULL, timestamp DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id), KEY idx_system_events_run_id (run_id),
			CONSTRAINT fk_system_events_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateConsLog: `CREATE TABLE IF NOT EXISTS console_logs (
			id BIGINT NOT NULL AUTO_INCREMENT, run_id VARCHAR(128) NOT NULL, line_num INT NOT NULL,
			line LONGTEXT NOT NULL, stream VARCHAR(32) DEFAULT 'stdout',
			timestamp DATETIME NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id),
			KEY idx_console_logs_run_id (run_id),
			CONSTRAINT fk_console_logs_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateArts: `CREATE TABLE IF NOT EXISTS artifacts (
			id VARCHAR(128) NOT NULL, run_id VARCHAR(128) NOT NULL, ` + "`type`" + ` VARCHAR(255) NOT NULL,
			name VARCHAR(255) NOT NULL, digest VARCHAR(255) NULL, state VARCHAR(32) DEFAULT 'pending',
			metadata LONGTEXT, created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id),
			CONSTRAINT fk_artifacts_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLCreateFiles: `CREATE TABLE IF NOT EXISTS files (
			id VARCHAR(128) NOT NULL, run_id VARCHAR(128) NOT NULL, name VARCHAR(1024) NOT NULL,
			url LONGTEXT, size BIGINT NOT NULL DEFAULT 0, created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id), CONSTRAINT fk_files_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLIdxSysEvRun:    `SELECT 1`,
		SQLIdxConsLogRun:  `SELECT 1`,
		SQLIdxRunsProject: `SELECT 1`,
		SQLAlterRunDel:    `ALTER TABLE runs ADD COLUMN deleted_at DATETIME NULL`,
		SQLAlterProjDel:   `ALTER TABLE projects ADD COLUMN deleted_at DATETIME NULL`,
		SQLAlterRecvHist:  `ALTER TABLE runs ADD COLUMN received_history_count INT NOT NULL DEFAULT 0`,
		SQLAlterForkRun:   `ALTER TABLE runs ADD COLUMN fork_run_id VARCHAR(128) NULL`,
		SQLAlterForkStep:  `ALTER TABLE runs ADD COLUMN fork_step INT NULL`,
		SQLCreateRunKeys: `CREATE TABLE IF NOT EXISTS run_keys (
			run_id VARCHAR(128) NOT NULL, ` + "`key`" + ` VARCHAR(255) NOT NULL,
			PRIMARY KEY (run_id, ` + "`key`" + `),
			CONSTRAINT fk_run_keys_run FOREIGN KEY (run_id) REFERENCES runs (id))` + mysqlEngineTail,
		SQLIdxScalarsStep: `SELECT 1`,
		SQLIdxRunSteps:    `SELECT 1`,

		SQLSelectProjectByEntityName: `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL`,
		SQLInsertProject:             `INSERT INTO projects (id, entity, name) VALUES (?, ?, ?)`,
		SQLSelectRunIDExists:         `SELECT id FROM runs WHERE id = ? AND deleted_at IS NULL`,
		SQLSelectRunIDByName:         `SELECT id FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL`,
		SQLSelectRunIDByDisplay:      `SELECT id FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL`,
		SQLInsertRun: `INSERT INTO runs (id, project_id, name, display_name, config, summary, state, host, program, git_commit, tags, notes, group_name, job_type, sweep_name)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		SQLFinishRun:               `UPDATE runs SET state = 'finished', updated_at = current_timestamp WHERE id = ?`,
		SQLIncrReceivedHistory:     `UPDATE runs SET received_history_count = received_history_count + ? WHERE id = ?`,
		SQLSoftDeleteRun:           `UPDATE runs SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL`,
		SQLSoftDeleteRunsByProject: `UPDATE runs SET deleted_at = current_timestamp WHERE project_id = ? AND deleted_at IS NULL`,
		SQLSoftDeleteProject:       `UPDATE projects SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL`,
		SQLUpdateRunSummary:        `UPDATE runs SET summary = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLSetHistoryLineCount:     `UPDATE runs SET history_line_count = ? WHERE id = ?`,
		SQLMarkRunFailed:           `UPDATE runs SET state = 'failed', notes = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLUpdateFileSize:          `UPDATE files SET size = ? WHERE id = ?`,
		SQLInsertFileIgnore:        `INSERT IGNORE INTO files (id, run_id, name, url) VALUES (?, ?, ?, ?)`,
		SQLListFilesByRun:          `SELECT id, run_id, name, COALESCE(url, ''), COALESCE(size, 0), created_at FROM files WHERE run_id = ? ORDER BY name`,
		SQLSelectProjectByID:       `SELECT id, entity, name, created_at FROM projects WHERE id = ? AND deleted_at IS NULL`,
		SQLSelectProjectByName:     `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL`,
		SQLListAllProjects:         `SELECT id, entity, name, created_at FROM projects WHERE deleted_at IS NULL ORDER BY created_at DESC`,
		SQLListProjectsByEntity:    `SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND deleted_at IS NULL ORDER BY created_at DESC`,
		SQLCountRunsByName:         `SELECT COUNT(*) FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL`,
		SQLCountRunsByDisplay:      `SELECT COUNT(*) FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL`,
		SQLListRunsLite: `SELECT r.id, r.project_id, r.name, r.display_name, r.state,
			r.history_line_count, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`,
		SQLUpdateForkedRun: `UPDATE runs SET config = ?, tags = ?, notes = ?, host = ?, program = ?,
			git_commit = ?, group_name = ?, job_type = ?, sweep_name = ?,
			fork_run_id = ?, fork_step = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLInsertForkedRun: `INSERT INTO runs (id, project_id, name, display_name, config, summary, state,
			host, program, git_commit, tags, notes, group_name, job_type, sweep_name, fork_run_id, fork_step)
			VALUES (?, ?, ?, ?, ?, '{}', 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		SQLRunSelectByID: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.id = ? AND r.deleted_at IS NULL`,
		SQLRunSelectByProject: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`,
		SQLRunSelectFiltered: `SELECT r.id, r.project_id, r.name, r.display_name, r.config, r.summary, r.state,
			r.host, r.program, r.git_commit, r.tags, r.notes, r.group_name, r.job_type, r.sweep_name,
			r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
			r.fork_run_id, r.fork_step, r.created_at, r.updated_at, r.heartbeat_at
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL`,
		SQLRunSelectNone:    `SELECT r.id FROM runs r WHERE 1 = 0`,
		SQLUpdateRunBase:    `UPDATE runs SET updated_at = current_timestamp, heartbeat_at = current_timestamp`,
		SQLSetCounterColumn: `UPDATE runs SET {col} = ?, updated_at = current_timestamp WHERE id = ?`,
		SQLInsertArtifact:   `INSERT INTO artifacts (id, run_id, ` + "`type`" + `, name, metadata) VALUES (?, ?, ?, ?, ?)`,
		SQLSelectArtifact:   `SELECT id, run_id, ` + "`type`" + `, name, digest, state, metadata, created_at FROM artifacts WHERE id = ?`,
		SQLCommitArtifact:   `UPDATE artifacts SET state = 'committed', digest = ? WHERE id = ?`,

		SQLInsertScalarsPrefix:    `INSERT IGNORE INTO history_scalars (run_id, step, ` + "`key`" + `, value, x_step) VALUES `,
		SQLInsertRunStepsPrefix:   `INSERT IGNORE INTO run_steps (run_id, step) VALUES `,
		SQLInsertRunKeysPrefix:    `INSERT IGNORE INTO run_keys (run_id, ` + "`key`" + `) VALUES `,
		SQLInsertHistsPrefix:      `INSERT IGNORE INTO history_histograms (run_id, step, ` + "`key`" + `, x_step, bins, vals) VALUES `,
		SQLInsertSysEvent:         `INSERT INTO system_events (run_id, line_num, data) VALUES (?, ?, ?)`,
		SQLInsertConsLog:          `INSERT INTO console_logs (run_id, line_num, line, stream) VALUES (?, ?, ?, 'stdout')`,
		SQLSelectRunKeys:          `SELECT ` + "`key`" + ` FROM run_keys WHERE run_id = ? ORDER BY ` + "`key`",
		SQLSelectHistoryLineCount: `SELECT history_line_count FROM runs WHERE id = ?`,
		SQLCountRunSteps:          `SELECT COUNT(*) FROM run_steps WHERE run_id = ?`,
		SQLBackfillRunSteps: `INSERT IGNORE INTO run_steps (run_id, step)
			SELECT run_id, step FROM history_scalars WHERE run_id = ? GROUP BY run_id, step`,
		SQLCountDistinctSteps:  `SELECT COUNT(DISTINCT step) FROM history_scalars WHERE run_id = ?`,
		SQLSelectScalarsByKey:  `SELECT COALESCE(x_step, step), value, step FROM history_scalars WHERE run_id = ? AND ` + "`key`" + ` = ?%s ORDER BY step`,
		SQLSelectScalarsBucket: `SELECT AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value) FROM history_scalars WHERE run_id = ? AND ` + "`key`" + ` = ?%s GROUP BY step DIV {n} ORDER BY step DIV {n}`,
		SQLSelectProjectRuns: `SELECT r.id, COALESCE(NULLIF(r.display_name, ''), r.name)
			FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at`,
		SQLMaxHistoryLineCount:    `SELECT COALESCE(MAX(history_line_count), 0) FROM runs WHERE project_id = ? AND deleted_at IS NULL`,
		SQLSelectScalarsByRun:     `SELECT ` + "`key`" + `, COALESCE(x_step, step) as x, value, step FROM history_scalars WHERE run_id = ? ORDER BY step, ` + "`key`",
		SQLSelectScalarsBucketKey: `SELECT ` + "`key`" + `, AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value) FROM history_scalars WHERE run_id = ? GROUP BY ` + "`key`" + `, step DIV {n} ORDER BY step DIV {n}, ` + "`key`",
		SQLSelectHistogramsByRun:  `SELECT step, ` + "`key`" + `, x_step, bins, vals FROM history_histograms WHERE run_id = ? ORDER BY step`,
		SQLSelectConsoleLines:     `SELECT line FROM console_logs WHERE run_id = ? ORDER BY line_num`,
		SQLCopyRunKeys:            `INSERT IGNORE INTO run_keys (run_id, ` + "`key`" + `) SELECT ?, ` + "`key`" + ` FROM run_keys WHERE run_id = ?`,
		SQLSelectParentScalars:    `SELECT step, ` + "`key`" + `, value, x_step FROM history_scalars WHERE run_id = ? AND step <= ? ORDER BY step, ` + "`key`" + ` LIMIT ? OFFSET ?`,
		SQLSelectParentHists:      `SELECT step, ` + "`key`" + `, x_step, bins, vals FROM history_histograms WHERE run_id = ? AND step <= ? ORDER BY step, ` + "`key`" + ` LIMIT ? OFFSET ?`,
		SQLMaxParentStep:          `SELECT COALESCE(MAX(step), -1) FROM run_steps WHERE run_id = ?`,
		SQLCountSysEvents:         `SELECT COUNT(*) FROM system_events WHERE run_id = ?`,
		SQLCountConsLogs:          `SELECT COUNT(*) FROM console_logs WHERE run_id = ?`,
		SQLActiveRunsEmpty:        `SELECT id FROM runs WHERE 1 = 0`,
		SQLActiveRunsIn:           `SELECT id FROM runs WHERE id IN ({in}) AND deleted_at IS NULL`,
		SQLStorageUsage:           storageUsageSQLMySQL,

		SQLSelectDeletedRun:     `SELECT id FROM runs WHERE deleted_at IS NOT NULL LIMIT 1`,
		SQLSelectDeletedProject: `SELECT id FROM projects WHERE deleted_at IS NOT NULL LIMIT 1`,
		SQLDeleteRunHard:        `DELETE FROM runs WHERE id = ?`,
		SQLDeleteProjectHard:    `DELETE FROM projects WHERE id = ?`,
		SQLSelectOrphanProject: `SELECT p.id FROM projects p WHERE p.deleted_at IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.project_id = p.id) LIMIT 1`,
		SQLDeleteScalarsBatch: `DELETE FROM history_scalars WHERE run_id = ? LIMIT ?`,
		SQLDeleteHistsBatch:   `DELETE FROM history_histograms WHERE run_id = ? LIMIT ?`,
		SQLDeleteStepsBatch:   `DELETE FROM run_steps WHERE run_id = ? LIMIT ?`,
		SQLDeleteKeysBatch:    `DELETE FROM run_keys WHERE run_id = ? LIMIT ?`,
		SQLDeleteSysEvBatch:   `DELETE FROM system_events WHERE run_id = ? LIMIT ?`,
		SQLDeleteConsBatch:    `DELETE FROM console_logs WHERE run_id = ? LIMIT ?`,
		SQLDeleteFilesBatch:   `DELETE FROM files WHERE run_id = ? LIMIT ?`,
		SQLDeleteArtsBatch:    `DELETE FROM artifacts WHERE run_id = ? LIMIT ?`,
	}
}

// Agent: 存储用量两套语句分开写。MySQL 给 key/type 加反引号。
const storageUsageSQLSQLite = `
		SELECT
			r.id, r.project_id, p.name, COALESCE(NULLIF(r.display_name, ''), r.name),
			(
				LENGTH(COALESCE(r.id, '')) + LENGTH(COALESCE(r.project_id, '')) + LENGTH(COALESCE(r.name, '')) +
				LENGTH(COALESCE(r.display_name, '')) + LENGTH(COALESCE(r.config, '')) + LENGTH(COALESCE(r.summary, '')) +
				LENGTH(COALESCE(r.state, '')) + LENGTH(COALESCE(r.host, '')) + LENGTH(COALESCE(r.program, '')) +
				LENGTH(COALESCE(r.git_commit, '')) + LENGTH(COALESCE(r.tags, '')) + LENGTH(COALESCE(r.notes, '')) +
				LENGTH(COALESCE(r.group_name, '')) + LENGTH(COALESCE(r.job_type, '')) + LENGTH(COALESCE(r.sweep_name, '')) +
				64 + COALESCE(hs.bytes, 0) + COALESCE(hh.bytes, 0) + COALESCE(rs.bytes, 0) + COALESCE(se.bytes, 0) +
				COALESCE(cl.bytes, 0) + COALESCE(a.bytes, 0) + COALESCE(f.bytes, 0) + COALESCE(rk.bytes, 0)
			) AS total_bytes
		FROM runs r
		JOIN projects p ON p.id = r.project_id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + 40) AS bytes FROM history_scalars GROUP BY run_id) hs ON hs.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + LENGTH(COALESCE(bins, '')) + LENGTH(COALESCE(vals, '')) + 48) AS bytes FROM history_histograms GROUP BY run_id) hh ON hh.run_id = r.id
		LEFT JOIN (SELECT run_id, COUNT(*) * 24 AS bytes FROM run_steps GROUP BY run_id) rs ON rs.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(data, '')) + 24) AS bytes FROM system_events GROUP BY run_id) se ON se.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(line, '')) + LENGTH(COALESCE(stream, '')) + 24) AS bytes FROM console_logs GROUP BY run_id) cl ON cl.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(type, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(digest, '')) + LENGTH(COALESCE(state, '')) + LENGTH(COALESCE(metadata, '')) + 24) AS bytes FROM artifacts GROUP BY run_id) a ON a.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(url, '')) + 16) AS bytes FROM files GROUP BY run_id) f ON f.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + 16) AS bytes FROM run_keys GROUP BY run_id) rk ON rk.run_id = r.id
		WHERE r.deleted_at IS NULL AND p.deleted_at IS NULL`

const storageUsageSQLMySQL = `
		SELECT
			r.id, r.project_id, p.name, COALESCE(NULLIF(r.display_name, ''), r.name),
			(
				LENGTH(COALESCE(r.id, '')) + LENGTH(COALESCE(r.project_id, '')) + LENGTH(COALESCE(r.name, '')) +
				LENGTH(COALESCE(r.display_name, '')) + LENGTH(COALESCE(r.config, '')) + LENGTH(COALESCE(r.summary, '')) +
				LENGTH(COALESCE(r.state, '')) + LENGTH(COALESCE(r.host, '')) + LENGTH(COALESCE(r.program, '')) +
				LENGTH(COALESCE(r.git_commit, '')) + LENGTH(COALESCE(r.tags, '')) + LENGTH(COALESCE(r.notes, '')) +
				LENGTH(COALESCE(r.group_name, '')) + LENGTH(COALESCE(r.job_type, '')) + LENGTH(COALESCE(r.sweep_name, '')) +
				64 + COALESCE(hs.bytes, 0) + COALESCE(hh.bytes, 0) + COALESCE(rs.bytes, 0) + COALESCE(se.bytes, 0) +
				COALESCE(cl.bytes, 0) + COALESCE(a.bytes, 0) + COALESCE(f.bytes, 0) + COALESCE(rk.bytes, 0)
			) AS total_bytes
		FROM runs r
		JOIN projects p ON p.id = r.project_id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(` + "`key`" + `, '')) + 40) AS bytes FROM history_scalars GROUP BY run_id) hs ON hs.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(` + "`key`" + `, '')) + LENGTH(COALESCE(bins, '')) + LENGTH(COALESCE(vals, '')) + 48) AS bytes FROM history_histograms GROUP BY run_id) hh ON hh.run_id = r.id
		LEFT JOIN (SELECT run_id, COUNT(*) * 24 AS bytes FROM run_steps GROUP BY run_id) rs ON rs.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(data, '')) + 24) AS bytes FROM system_events GROUP BY run_id) se ON se.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(line, '')) + LENGTH(COALESCE(stream, '')) + 24) AS bytes FROM console_logs GROUP BY run_id) cl ON cl.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(` + "`type`" + `, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(digest, '')) + LENGTH(COALESCE(state, '')) + LENGTH(COALESCE(metadata, '')) + 24) AS bytes FROM artifacts GROUP BY run_id) a ON a.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(url, '')) + 16) AS bytes FROM files GROUP BY run_id) f ON f.run_id = r.id
		LEFT JOIN (SELECT run_id, SUM(LENGTH(COALESCE(` + "`key`" + `, '')) + 16) AS bytes FROM run_keys GROUP BY run_id) rk ON rk.run_id = r.id
		WHERE r.deleted_at IS NULL AND p.deleted_at IS NULL`
