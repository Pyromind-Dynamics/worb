package store

import (
	"path/filepath"
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
