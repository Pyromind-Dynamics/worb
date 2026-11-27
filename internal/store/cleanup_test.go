package store

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// setupTestDB 打开测试库。设置了 WORB_MYSQL_HOST 时用 MySQL，否则用 SQLite。
// setupTestDB opens the test database. It uses MySQL when WORB_MYSQL_HOST is set, otherwise SQLite.
func setupTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	// Agent: 有 MySQL 环境变量时整套功能测试都打到 MySQL。
	if host := os.Getenv("WORB_MYSQL_HOST"); host != "" {
		db, err := NewMySQL(dir, MySQLConfig{
			Host:     host,
			Port:     envOr("WORB_MYSQL_PORT", "3306"),
			User:     envOr("WORB_MYSQL_USER", "root"),
			Password: os.Getenv("WORB_MYSQL_PASSWD"),
			DBName:   envOr("WORB_MYSQL_DB", "worb"),
		})
		if err != nil {
			t.Fatalf("NewMySQL: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	db, err := New(dir, "sqlite")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPurgeDeletedRun(t *testing.T) {
	db := setupTestDB(t)

	// Create a project and run with 200 history rows
	proj, err := db.EnsureProject("local", "cleanup-test")
	if err != nil {
		t.Fatalf("EnsureProject: %v", err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "cleanup-test", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 200)
	for i := range batch {
		data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "loss": float64(i) * 0.1})
		batch[i] = struct {
			Step int
			Data json.RawMessage
		}{Step: i, Data: data}
	}
	db.InsertHistoryBatch(run.ID, batch)
	db.Flush()

	// Verify data is there
	var scalarCount int
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&scalarCount)
	if scalarCount != 200 {
		t.Fatalf("expected 200 scalars, got %d", scalarCount)
	}
	var stepCount int
	db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount)
	if stepCount != 200 {
		t.Fatalf("expected 200 run_steps, got %d", stepCount)
	}

	// Soft-delete the run
	if err := db.DeleteRun(run.ID); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}

	// Run should be hidden from queries
	runs, _ := db.ListRuns(proj.ID)
	if len(runs) != 0 {
		t.Errorf("expected 0 visible runs, got %d", len(runs))
	}

	// Purge with small batch size — should take multiple rounds
	batchSize := 50
	rounds := 0
	for {
		more, err := db.purgeDeletedData(batchSize)
		if err != nil {
			t.Fatalf("purgeDeletedData round %d: %v", rounds, err)
		}
		rounds++
		if !more {
			break
		}
		if rounds > 20 {
			t.Fatal("too many purge rounds")
		}
	}

	t.Logf("purged in %d rounds (batch=%d, scalars=%d)", rounds, batchSize, scalarCount)

	// Verify everything is gone
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&scalarCount)
	if scalarCount != 0 {
		t.Errorf("expected 0 scalars after purge, got %d", scalarCount)
	}
	db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount)
	if stepCount != 0 {
		t.Errorf("expected 0 run_steps after purge, got %d", stepCount)
	}

	var runCount int
	db.QueryRow("SELECT COUNT(*) FROM runs WHERE id = ?", run.ID).Scan(&runCount)
	if runCount != 0 {
		t.Errorf("expected run to be hard-deleted, got %d", runCount)
	}
}

func TestPurgeDeletedProject(t *testing.T) {
	db := setupTestDB(t)

	proj, err := db.EnsureProject("local", "proj-cleanup")
	if err != nil {
		t.Fatalf("EnsureProject: %v", err)
	}

	// Create two runs with data
	for _, name := range []string{"run-a", "run-b"} {
		run, _ := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "proj-cleanup", Name: name})
		batch := make([]struct {
			Step int
			Data json.RawMessage
		}, 100)
		for i := range batch {
			data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "acc": float64(i)})
			batch[i] = struct {
				Step int
				Data json.RawMessage
			}{Step: i, Data: data}
		}
		db.InsertHistoryBatch(run.ID, batch)
	}
	db.Flush()

	// Delete the project (soft-deletes project + all runs)
	if err := db.DeleteProject(proj.ID); err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	// Project should be hidden
	projects, _ := db.ListAllProjects()
	for _, p := range projects {
		if p.ID == proj.ID {
			t.Error("deleted project still visible in ListAllProjects")
		}
	}

	// Purge until done
	rounds := 0
	for {
		more, err := db.purgeDeletedData(50)
		if err != nil {
			t.Fatalf("purgeDeletedData round %d: %v", rounds, err)
		}
		rounds++
		if !more {
			break
		}
		if rounds > 50 {
			t.Fatal("too many purge rounds")
		}
	}

	t.Logf("project purged in %d rounds", rounds)

	// Verify everything is gone
	var runCount int
	db.QueryRow("SELECT COUNT(*) FROM runs WHERE project_id = ?", proj.ID).Scan(&runCount)
	if runCount != 0 {
		t.Errorf("expected 0 runs after purge, got %d", runCount)
	}

	var projCount int
	db.QueryRow("SELECT COUNT(*) FROM projects WHERE id = ?", proj.ID).Scan(&projCount)
	if projCount != 0 {
		t.Errorf("expected project to be hard-deleted, got %d", projCount)
	}
}

func TestPurgeBatchSizeRespected(t *testing.T) {
	db := setupTestDB(t)

	run, _ := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "batch-test", Name: "run1"})

	// Insert 500 scalar rows
	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 500)
	for i := range batch {
		data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "loss": float64(i)})
		batch[i] = struct {
			Step int
			Data json.RawMessage
		}{Step: i, Data: data}
	}
	db.InsertHistoryBatch(run.ID, batch)
	db.Flush()

	// Soft-delete
	db.DeleteRun(run.ID)

	// One purge call with batchSize=100 should delete exactly 100 scalars
	more, err := db.purgeDeletedData(100)
	if err != nil {
		t.Fatalf("purgeDeletedData: %v", err)
	}
	if !more {
		t.Error("expected more=true, there should be remaining data")
	}

	var remaining int
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&remaining)
	if remaining != 400 {
		t.Errorf("expected 400 remaining scalars after batch of 100, got %d", remaining)
	}
}

func TestPurgeNoDeletedData(t *testing.T) {
	db := setupTestDB(t)

	// Agent: MySQL 共用库可能残留前序用例的软删数据，先排空再断言空闲态。
	for i := 0; i < 200; i++ {
		more, err := db.purgeDeletedData(5000)
		if err != nil {
			t.Fatalf("purgeDeletedData drain: %v", err)
		}
		if !more {
			break
		}
		if i == 199 {
			t.Fatal("purge did not finish draining soft-deleted data")
		}
	}

	// No deleted runs/projects — purge should be a no-op
	more, err := db.purgeDeletedData(5000)
	if err != nil {
		t.Fatalf("purgeDeletedData: %v", err)
	}
	if more {
		t.Error("expected more=false when nothing is deleted")
	}
}

// TestPurgeDoesNotHardDeleteFiles 确认 DeleteRun 会软删文件并入队，purge 不硬删文件行。
// TestPurgeDoesNotHardDeleteFiles checks that DeleteRun soft-deletes files and purge leaves those rows in place.
func TestPurgeDoesNotHardDeleteFiles(t *testing.T) {
	db := setupTestDB(t)
	var queued []string
	// Agent: 回调代替删除队列，确认 id 是从 DeleteRun 送出的。
	db.SetFileDeleteNotify(func(ids []string) {
		queued = append(queued, ids...)
	})
	if _, err := db.EnsureProject("local", "purge-files"); err != nil {
		t.Fatal(err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "purge-files", Name: "run1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.InsertPendingRunFile("run-1", run.ID, "a.log", "/files/upload/run-1", "local"); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertFile("legacy", run.ID, "old.log", "/files/upload/legacy"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE files SET object_key=? WHERE id=?`, "obj", "run-1"); err != nil {
		t.Fatal(err)
	}
	visible, err := db.ListVisibleRunFiles(run.ID)
	if err != nil || len(visible) != 2 || visible[0].ObjectKey != "obj" && visible[1].ObjectKey != "obj" {
		t.Fatalf("visible %+v %v", visible, err)
	}
	if err := db.DeleteRun(run.ID); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range queued {
		got[id] = true
	}
	if len(queued) != 2 || !got["run-1"] || !got["legacy"] {
		t.Fatalf("queued %#v", queued)
	}
	visible, err = db.ListVisibleRunFiles(run.ID)
	if err != nil || len(visible) != 0 {
		t.Fatalf("soft-deleted still visible %+v %v", visible, err)
	}
	clock := time.Unix(1700000000, 0)
	db.cleanupNow = func() time.Time { return clock }
	// Agent: 文件行未清完时 run 被暂缓，purge 不再返回 true 让调用方反复重跑同一个 run。
	more, err := db.purgeDeletedData(100)
	if err != nil || more {
		t.Fatalf("purge while files remain %v %v", more, err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM files WHERE run_id=?`, run.ID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("file rows left %d %v", n, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE id=?`, run.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("run rows %d %v", n, err)
	}
	if err := db.HardDeleteStoredFile("run-1"); err != nil {
		t.Fatal(err)
	}
	if err := db.HardDeleteStoredFile("legacy"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(parkBase + time.Second)
	for i := 0; i < 5; i++ {
		more, err = db.purgeDeletedData(100)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE id=?`, run.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("run still present %d %v", n, err)
	}
}

func drainPurge(t *testing.T, db *DB, rounds int) {
	t.Helper()
	for i := 0; i < rounds; i++ {
		more, err := db.purgeDeletedData(100)
		if err != nil {
			t.Fatalf("purgeDeletedData: %v", err)
		}
		if !more {
			return
		}
	}
	t.Fatal("purge did not settle")
}

func countRows(t *testing.T, db *DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func newBlockedRun(t *testing.T, db *DB, project, runID, fileID string) {
	t.Helper()
	if _, err := db.UpsertRun(UpsertRunParams{ID: runID, Entity: "local", Project: project, Name: runID}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertPendingRunFile(fileID, runID, fileID+".log", "/files/upload/"+fileID, "local"); err != nil {
		t.Fatal(err)
	}
}

// TestPurgeBlockedRunDoesNotStarveOthers 复现队头阻塞：id 排在前面的 run 文件行永远删不掉，后面的 run 和孤儿项目仍要被清掉。
// TestPurgeBlockedRunDoesNotStarveOthers reproduces head-of-line blocking: a run that sorts first and never loses its file rows must not stop later runs or orphan projects from being purged.
func TestPurgeBlockedRunDoesNotStarveOthers(t *testing.T) {
	db := setupTestDB(t)
	clock := time.Unix(1700000000, 0)
	db.cleanupNow = func() time.Time { return clock }
	// Agent: 回调什么都不做，模拟入队丢失或对象存储持续失败。
	db.SetFileDeleteNotify(func(ids []string) {})

	newBlockedRun(t, db, "hol-a", "0000-blocked", "0000-file")
	if _, err := db.UpsertRun(UpsertRunParams{ID: "zzzz-free", Entity: "local", Project: "hol-b", Name: "free"}); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]interface{}{"_step": float64(0), "loss": 1.0})
	db.InsertHistoryBatch("zzzz-free", []struct {
		Step int
		Data json.RawMessage
	}{{Step: 0, Data: data}})
	db.Flush()
	orphan, err := db.EnsureProject("local", "hol-orphan")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteRun("0000-blocked"); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteRun("zzzz-free"); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteProject(orphan.ID); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		drainPurge(t, db, 50)
		clock = clock.Add(time.Second)
	}

	if n := countRows(t, db, `SELECT COUNT(*) FROM runs WHERE id=?`, "zzzz-free"); n != 0 {
		t.Fatalf("free run starved by blocked run, still present %d", n)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM history_scalars WHERE run_id=?`, "zzzz-free"); n != 0 {
		t.Fatalf("free run metrics left %d", n)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM projects WHERE id=?`, orphan.ID); n != 0 {
		t.Fatalf("orphan project starved by blocked run, still present %d", n)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM runs WHERE id=?`, "0000-blocked"); n != 1 {
		t.Fatalf("blocked run must stay until its files are hard-deleted, got %d", n)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM files WHERE run_id=?`, "0000-blocked"); n != 1 {
		t.Fatalf("blocked run file rows %d", n)
	}

	if err := db.HardDeleteStoredFile("0000-file"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(parkMax + time.Second)
	drainPurge(t, db, 50)
	if n := countRows(t, db, `SELECT COUNT(*) FROM runs WHERE id=?`, "0000-blocked"); n != 0 {
		t.Fatalf("blocked run not purged after its files were deleted, got %d", n)
	}
}

// TestPurgeParkedRunIsRequeued 确认暂缓的 run 回访时会把已软删的文件 id 重新入队，并按退避间隔执行。
// TestPurgeParkedRunIsRequeued checks that a parked run re-queues its soft-deleted file ids on revisit, with backoff.
func TestPurgeParkedRunIsRequeued(t *testing.T) {
	db := setupTestDB(t)
	clock := time.Unix(1700000000, 0)
	db.cleanupNow = func() time.Time { return clock }
	var calls [][]string
	db.SetFileDeleteNotify(func(ids []string) {
		calls = append(calls, append([]string(nil), ids...))
	})

	newBlockedRun(t, db, "requeue", "requeue-run", "requeue-file")
	if err := db.DeleteRun("requeue-run"); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("DeleteRun notify calls %d", len(calls))
	}

	drainPurge(t, db, 10)
	if len(calls) != 1 {
		t.Fatalf("first visit must not re-queue, calls %d", len(calls))
	}
	clock = clock.Add(time.Second)
	drainPurge(t, db, 10)
	if len(calls) != 1 {
		t.Fatalf("parked run must not be revisited early, calls %d", len(calls))
	}

	clock = clock.Add(parkBase)
	drainPurge(t, db, 10)
	if len(calls) != 2 || len(calls[1]) != 1 || calls[1][0] != "requeue-file" {
		t.Fatalf("revisit must re-queue the soft-deleted id, calls %#v", calls)
	}
	db.cleanupMu.Lock()
	p := db.cleanupParked["requeue-run"]
	db.cleanupMu.Unlock()
	if p.delay != 2*parkBase {
		t.Fatalf("no progress must double the delay, got %v", p.delay)
	}

	clock = clock.Add(parkBase + time.Second)
	drainPurge(t, db, 10)
	if len(calls) != 2 {
		t.Fatalf("backoff not applied, calls %d", len(calls))
	}

	for i := 0; i < 10; i++ {
		clock = clock.Add(parkMax)
		drainPurge(t, db, 10)
	}
	db.cleanupMu.Lock()
	p = db.cleanupParked["requeue-run"]
	db.cleanupMu.Unlock()
	if p.delay != parkMax {
		t.Fatalf("delay must be capped at parkMax, got %v", p.delay)
	}
}

// TestPurgeParkedProgressKeepsShortDelay 文件行在减少说明异步删除在推进，退避间隔应保持在最小值。
// TestPurgeParkedProgressKeepsShortDelay verifies that shrinking file rows keep the short retry delay.
func TestPurgeParkedProgressKeepsShortDelay(t *testing.T) {
	db := setupTestDB(t)
	clock := time.Unix(1700000000, 0)
	db.cleanupNow = func() time.Time { return clock }
	db.SetFileDeleteNotify(func(ids []string) {})

	newBlockedRun(t, db, "progress", "progress-run", "progress-f1")
	if err := db.InsertPendingRunFile("progress-f2", "progress-run", "f2.log", "/files/upload/progress-f2", "local"); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteRun("progress-run"); err != nil {
		t.Fatal(err)
	}
	drainPurge(t, db, 10)
	if err := db.HardDeleteStoredFile("progress-f1"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(parkBase + time.Second)
	drainPurge(t, db, 10)
	db.cleanupMu.Lock()
	p := db.cleanupParked["progress-run"]
	db.cleanupMu.Unlock()
	if p.delay != parkBase || p.waiting != 1 {
		t.Fatalf("progress must keep short delay, got %+v", p)
	}
}
