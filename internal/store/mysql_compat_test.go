package store

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// localMySQLConfig 从环境变量读取本机 MySQL 连接参数。
// localMySQLConfig reads local MySQL settings from the environment.
func localMySQLConfig(t *testing.T) MySQLConfig {
	t.Helper()
	host := os.Getenv("WORB_MYSQL_HOST")
	if host == "" {
		t.Skip("WORB_MYSQL_HOST is not set")
	}
	return MySQLConfig{
		Host:     host,
		Port:     envOr("WORB_MYSQL_PORT", "3306"),
		User:     envOr("WORB_MYSQL_USER", "root"),
		Password: os.Getenv("WORB_MYSQL_PASSWD"),
		DBName:   envOr("WORB_MYSQL_DB", "worb"),
	}
}

// openLocalMySQL 打开本机 worb 库并做 schema 检查（不建表）。
// openLocalMySQL opens the local worb database and checks schema without creating tables.
func openLocalMySQL(t *testing.T) *DB {
	t.Helper()
	db, err := NewMySQL(t.TempDir(), localMySQLConfig(t))
	if err != nil {
		t.Fatalf("NewMySQL: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestMySQLSchemaCheck 确认启动检查能通过已提前建好的表、列类型和索引。
// TestMySQLSchemaCheck confirms the startup check passes for pre-created tables, types, and indexes.
func TestMySQLSchemaCheck(t *testing.T) {
	db := openLocalMySQL(t)
	// Agent: 第二次检查应同样通过，且过程不执行 CREATE。
	if err := db.checkMySQLSchema(); err != nil {
		t.Fatalf("checkMySQLSchema: %v", err)
	}
	want := []string{
		"projects", "runs", "history_scalars", "history_histograms",
		"run_steps", "run_keys", "system_events", "console_logs", "artifacts", "files",
	}
	for _, name := range want {
		var n int
		if err := db.DB.QueryRow(`
			SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("missing table %s", name)
		}
	}
	var entityType, keyType string
	if err := db.DB.QueryRow(`
		SELECT column_type FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'projects' AND column_name = 'entity'`).Scan(&entityType); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(entityType), "varchar(512)") {
		t.Fatalf("projects.entity = %s, want varchar(512)", entityType)
	}
	if err := db.DB.QueryRow(`
		SELECT column_type FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'history_scalars' AND column_name = 'key'`).Scan(&keyType); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(keyType), "varchar(255)") {
		t.Fatalf("history_scalars.key = %s, want varchar(255)", keyType)
	}
	var idx int
	if err := db.DB.QueryRow(`
		SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'history_scalars'
		  AND index_name = 'idx_history_scalars_run_step'`).Scan(&idx); err != nil {
		t.Fatal(err)
	}
	if idx == 0 {
		t.Fatal("missing index idx_history_scalars_run_step")
	}
}

// TestMySQLAllSQLStatements 对 sql_manager 中业务编号语句在 MySQL 上预编译。
// TestMySQLAllSQLStatements prepares every business numbered statement on MySQL.
func TestMySQLAllSQLStatements(t *testing.T) {
	db := openLocalMySQL(t)
	ids := []SQLID{
		SQLSelectProjectByEntityName, SQLInsertProject, SQLSelectRunIDExists, SQLSelectRunIDByName,
		SQLInsertRun, SQLFinishRun, SQLIncrReceivedHistory, SQLSoftDeleteRun, SQLSoftDeleteRunsByProject,
		SQLSoftDeleteProject, SQLUpdateRunSummary, SQLSetHistoryLineCount, SQLMarkRunFailed,
		SQLUpdateFileSize, SQLInsertFileIgnore, SQLListFilesByRun, SQLInsertArtifact, SQLSelectArtifact,
		SQLCommitArtifact, SQLInsertSysEvent, SQLInsertConsLog, SQLSelectRunKeys, SQLSelectHistoryLineCount,
		SQLCountRunSteps, SQLBackfillRunSteps, SQLCountDistinctSteps, SQLSelectHistogramsByRun,
		SQLSelectConsoleLines, SQLSelectDeletedRun, SQLSelectDeletedProject, SQLDeleteRunHard,
		SQLDeleteProjectHard, SQLSelectOrphanProject, SQLCopyRunKeys, SQLSelectParentScalars,
		SQLSelectParentHists, SQLMaxParentStep, SQLCountSysEvents, SQLCountConsLogs,
		SQLActiveRunsEmpty, SQLRunSelectByID, SQLRunSelectByProject, SQLRunSelectFiltered,
		SQLDeleteScalarsBatch, SQLDeleteHistsBatch, SQLDeleteStepsBatch, SQLDeleteKeysBatch,
		SQLDeleteSysEvBatch, SQLDeleteConsBatch, SQLDeleteFilesBatch, SQLDeleteArtsBatch,
		SQLStorageUsage, SQLListRunsLite, SQLUpdateForkedRun, SQLInsertForkedRun,
	}
	for _, id := range ids {
		query := db.SQL(id)
		if query == "" || query == "SELECT 1" {
			t.Fatalf("empty sql id=%d", id)
		}
		// Agent: 带占位符的模板先填成可 Prepare 的形式。
		query = strings.ReplaceAll(query, "{in}", "?,?,?")
		query = strings.ReplaceAll(query, "{n}", "3")
		query = strings.ReplaceAll(query, "{col}", "history_line_count")
		if strings.Contains(query, "%s") {
			query = fmt.Sprintf(query, "")
		}
		stmt, err := db.DB.Prepare(query)
		if err != nil {
			t.Fatalf("id=%d prepare failed sql=%s err=%v", id, query, err)
		}
		stmt.Close()
	}
	if _, err := db.SQLUpdateRun([]string{"state = ?"}); err != nil {
		t.Fatal(err)
	}
	if db.SQLActiveRunIDs(3) == "" {
		t.Fatal("active run ids empty")
	}
	if db.SQLScalarsBucket(3) == "" || db.SQLProjectScalarsBucket(3) == "" {
		t.Fatal("bucket sql empty")
	}
}

// TestMySQLRuntimeSQLCompat 在真实数据上跑写入、去重、分桶、删除、软删清理。
// TestMySQLRuntimeSQLCompat runs write, dedupe, bucket, delete, and soft-delete cleanup on real data.
func TestMySQLRuntimeSQLCompat(t *testing.T) {
	db := openLocalMySQL(t)
	suffix := fmt.Sprintf("compat-%d", os.Getpid())
	entity := "JDJiJDEyJHpHaGNGUlEzUWJsS2ZmOEtzbmo4Q2U="

	run, err := db.UpsertRun(UpsertRunParams{
		Entity: entity, Project: "mysql-" + suffix, Name: "run-" + suffix,
		DisplayName: "mysql compat", Config: json.RawMessage(`{"lr":0.1}`),
		Summary: json.RawMessage(`{"loss":1}`), Tags: json.RawMessage(`["a"]`), State: "running",
	})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}
	var storedEntity string
	if err := db.QueryRow("SELECT entity FROM projects WHERE id = ?", run.ProjectID).Scan(&storedEntity); err != nil {
		t.Fatal(err)
	}
	if storedEntity != entity {
		t.Fatalf("entity = %q, want raw wandb key", storedEntity)
	}

	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 0, 12)
	for step := 0; step < 10; step++ {
		batch = append(batch, struct {
			Step int
			Data json.RawMessage
		}{Step: step, Data: json.RawMessage(fmt.Sprintf(`{"loss":%d,"acc":0.%d}`, step, step))})
	}
	batch = append(batch,
		struct {
			Step int
			Data json.RawMessage
		}{Step: 1, Data: json.RawMessage(`{"loss":99}`)},
		struct {
			Step int
			Data json.RawMessage
		}{Step: 2, Data: json.RawMessage(`{"hist":{"_type":"histogram","bins":[0,1,2],"values":[3,4]}}`)},
	)
	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertSystemEvent(run.ID, 0, json.RawMessage(`{"cpu":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertConsoleLog(run.ID, 0, "hello", "stdout"); err != nil {
		t.Fatal(err)
	}
	db.Flush()
	if _, err := db.CreateArtifact(run.ID, "model", "ckpt", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertFile("file-"+suffix, run.ID, "out.log", "http://localhost/f"); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertFile("file-"+suffix, run.ID, "out.log", "http://localhost/f"); err != nil {
		t.Fatal(err)
	}

	var scalarCount, histCount, fileCount, eventCount, logCount int
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&scalarCount)
	db.QueryRow("SELECT COUNT(*) FROM history_histograms WHERE run_id = ?", run.ID).Scan(&histCount)
	db.QueryRow("SELECT COUNT(*) FROM files WHERE run_id = ?", run.ID).Scan(&fileCount)
	db.QueryRow("SELECT COUNT(*) FROM system_events WHERE run_id = ?", run.ID).Scan(&eventCount)
	db.QueryRow("SELECT COUNT(*) FROM console_logs WHERE run_id = ?", run.ID).Scan(&logCount)
	if scalarCount != 20 {
		t.Fatalf("scalars = %d, want 20", scalarCount)
	}
	if histCount != 1 || fileCount != 1 || eventCount != 1 || logCount != 1 {
		t.Fatalf("hist=%d file=%d event=%d log=%d", histCount, fileCount, eventCount, logCount)
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
		t.Fatalf("DIV buckets = %d, want 4", buckets)
	}

	gotPoints := 0
	if err := db.StreamHistoryScalars(run.ID, 3, []string{"loss"}, nil, nil, func(p ScalarPoint) error {
		gotPoints++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if gotPoints == 0 || gotPoints > 4 {
		t.Fatalf("streamed points = %d", gotPoints)
	}

	deleted, err := db.deleteBatch("history_scalars", "run_id", run.ID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 5 {
		t.Fatalf("deleted = %d, want 5", deleted)
	}
	got, err := db.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Config) == "" || got.CreatedAt.IsZero() {
		t.Fatalf("GetRun missing config/time: %+v", got)
	}
	if err := db.DeleteRun(run.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		more, err := db.purgeDeletedData(1000)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	var left int
	db.QueryRow("SELECT COUNT(*) FROM runs WHERE id = ?", run.ID).Scan(&left)
	if left != 0 {
		t.Fatalf("run still present after purge")
	}
}

// TestMySQLDeleteBatchAllTables 确认每个可分批删除的表都能在 MySQL 上执行。
// TestMySQLDeleteBatchAllTables checks that every batch-delete table works on MySQL.
func TestMySQLDeleteBatchAllTables(t *testing.T) {
	db := openLocalMySQL(t)
	tables := []string{"history_scalars", "history_histograms", "run_steps", "run_keys", "system_events", "console_logs", "files", "artifacts"}
	for _, table := range tables {
		q, err := db.SQLDeleteBatch(table, "run_id")
		if err != nil {
			t.Fatal(err)
		}
		if q != "DELETE FROM "+table+" WHERE run_id = ? LIMIT ?" {
			t.Fatalf("%s delete = %s", table, q)
		}
		stmt, err := db.DB.Prepare(q)
		if err != nil {
			t.Fatalf("%s prepare: %v", table, err)
		}
		stmt.Close()
	}
}
