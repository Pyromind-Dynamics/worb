package store

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileKindAndEntityKey(t *testing.T) {
	if FileKindOf("run-abc") != FileKindRun || FileKindOf("art-abc") != FileKindArtifact || FileKindOf("abc") != FileKindUnknown {
		t.Fatal("prefix routing")
	}
	if EntityKey("local") == "" || len(EntityKey("a=b")) == 0 {
		t.Fatal("entity key")
	}
	if _, err := CleanObjectName("../x"); err == nil {
		t.Fatal("traversal accepted")
	}
	key := ObjectKey(FileKindRun, "ZXg", "run1", "a.log")
	if key != "ZXg/stand-runs/run1/a.log" {
		t.Fatal(key)
	}
}

func TestUploadBackoff(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	if !uploadDue(0, "", now) || uploadDue(1, "2026-11-11 00:00:00", now) {
		t.Fatal("immediate window")
	}
	if !uploadDue(1, "2026-11-10 23:58:00", now) || uploadDue(2, "2026-11-10 23:56:00", now) {
		t.Fatal("backoff window")
	}
	if !uploadDue(2, "2026-11-10 23:50:00", now) {
		t.Fatal("five minute backoff")
	}
}

func TestPendingFileHiddenAfterSoftDelete(t *testing.T) {
	db := setupTestDB(t)
	if _, err := db.EnsureProject("local", "files"); err != nil {
		t.Fatal(err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "files", Name: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	id := "run-1"
	if err := db.InsertPendingRunFile(id, run.ID, "a.log", "/files/upload/"+id, "local"); err != nil {
		t.Fatal(err)
	}
	ok, err := db.MarkFileLocal(id, filepath.Join(t.TempDir(), id), 3)
	if err != nil || !ok {
		t.Fatalf("mark local %v %v", ok, err)
	}
	listed, err := db.ListFiles(run.ID)
	if err != nil || len(listed) != 1 {
		t.Fatalf("list %+v %v", listed, err)
	}
	ids, err := db.SoftDeleteFilesForRun(run.ID)
	if err != nil || len(ids) != 1 {
		t.Fatalf("soft %v %v", ids, err)
	}
	listed, err = db.ListFiles(run.ID)
	if err != nil || len(listed) != 0 {
		t.Fatalf("still visible %+v %v", listed, err)
	}
	file, err := db.GetStoredFile(id)
	if err != nil || file.DeletedAt == "" {
		t.Fatalf("stored %+v %v", file, err)
	}
}

func TestSoftDeleteStoredFilesByID(t *testing.T) {
	db := setupTestDB(t)
	if _, err := db.EnsureProject("local", "files"); err != nil {
		t.Fatal(err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "files", Name: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	id := "run-9"
	if err := db.InsertPendingRunFile(id, run.ID, "test.csv", "/files/upload/"+id, "local"); err != nil {
		t.Fatal(err)
	}
	var queued []string
	db.SetFileDeleteNotify(func(ids []string) { queued = append(queued, ids...) })
	changed, err := db.SoftDeleteStoredFiles([]string{id, "run-missing"})
	if err != nil || len(changed) != 1 || changed[0] != id || len(queued) != 1 {
		t.Fatalf("changed %v queued %v err %v", changed, queued, err)
	}
	listed, err := db.ListVisibleRunFiles(run.ID)
	if err != nil || len(listed) != 0 {
		t.Fatalf("visible %+v %v", listed, err)
	}
}

func TestVisibleRunFilePageByName(t *testing.T) {
	db := setupTestDB(t)
	if _, err := db.EnsureProject("local", "pages"); err != nil {
		t.Fatal(err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "pages", Name: "page"})
	if err != nil {
		t.Fatal(err)
	}
	rows := []struct{ id, name string }{
		{"run-a", "a.log"},
		{"run-b", "wandb-history.jsonl"},
		{"run-c", "test.csv"},
	}
	for _, row := range rows {
		if err := db.InsertPendingRunFile(row.id, run.ID, row.name, "/files/upload/"+row.id, "local"); err != nil {
			t.Fatal(err)
		}
	}
	named, more, err := db.ListVisibleRunFilePage(run.ID, VisibleFilePage{Names: []string{"wandb-history.jsonl"}, Limit: 50})
	if err != nil || more || len(named) != 1 || named[0].ID != "run-b" {
		t.Fatalf("named %+v more %v err %v", named, more, err)
	}
	csv, _, err := db.ListVisibleRunFilePage(run.ID, VisibleFilePage{Pattern: "%.csv", Limit: 50})
	if err != nil || len(csv) != 1 || csv[0].Name != "test.csv" {
		t.Fatalf("pattern %+v %v", csv, err)
	}
	first, more, err := db.ListVisibleRunFilePage(run.ID, VisibleFilePage{Limit: 1})
	if err != nil || !more || len(first) != 1 || first[0].ID != "run-a" {
		t.Fatalf("first %+v more %v err %v", first, more, err)
	}
	second, more, err := db.ListVisibleRunFilePage(run.ID, VisibleFilePage{Limit: 1, AfterID: first[0].ID})
	if err != nil || !more || len(second) != 1 || second[0].ID != "run-b" {
		t.Fatalf("second %+v more %v err %v", second, more, err)
	}
	n, err := db.CountVisibleRunFiles(run.ID)
	if err != nil || n != 3 {
		t.Fatalf("count %d %v", n, err)
	}
	if _, err := db.SoftDeleteStoredFiles([]string{"run-a"}); err != nil {
		t.Fatal(err)
	}
	n, err = db.CountVisibleRunFiles(run.ID)
	if err != nil || n != 2 {
		t.Fatalf("count after delete %d %v", n, err)
	}
	gone, _, err := db.ListVisibleRunFilePage(run.ID, VisibleFilePage{Names: []string{"a.log"}, Limit: 50})
	if err != nil || len(gone) != 0 {
		t.Fatalf("deleted still visible %+v %v", gone, err)
	}
	if db.Engine == "mysql" {
		return
	}
	plan := sqlitePlan(t, db, `SELECT id, name, COALESCE(object_key,'') FROM files WHERE run_id=? AND deleted_at IS NULL AND name IN (?) ORDER BY name LIMIT ?`, run.ID, "wandb-history.jsonl", 51)
	if !strings.Contains(plan, "idx_files_run_name") {
		t.Fatalf("name lookup plan %s", plan)
	}
	plan = sqlitePlan(t, db, `SELECT id, name, COALESCE(object_key,'') FROM files WHERE run_id=? AND deleted_at IS NULL AND id > ? ORDER BY id LIMIT ?`, run.ID, "run-a", 51)
	if !strings.Contains(plan, "idx_files_run_id") {
		t.Fatalf("page plan %s", plan)
	}
}

func sqlitePlan(t *testing.T, db *DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
