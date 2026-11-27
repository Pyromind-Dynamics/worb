package store

import (
	"encoding/json"
	"testing"
)

// TestCoreReads 确认 run、指标和文件能从库里读回。设置了 WORB_MYSQL_HOST 时走 MySQL，否则走 SQLite。
// TestCoreReads checks that a run, its metrics, and its files can be read back. It uses MySQL when WORB_MYSQL_HOST is set, otherwise SQLite.
func TestCoreReads(t *testing.T) {
	db := setupTestDB(t)
	if _, err := db.EnsureProject("10000003", "reads"); err != nil {
		t.Fatal(err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "10000003", Project: "reads", Name: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.GetRun(run.ID)
	if err != nil || got.Name != "r1" {
		t.Fatalf("get run %+v %v", got, err)
	}
	byName, err := db.GetRunByName(run.ProjectID, "r1")
	if err != nil || byName.ID != run.ID {
		t.Fatalf("get by name %+v %v", byName, err)
	}

	data, _ := json.Marshal(map[string]any{"loss": 0.5})
	if err := db.InsertHistoryBatch(run.ID, []struct {
		Step int
		Data json.RawMessage
	}{{Step: 1, Data: data}}); err != nil {
		t.Fatal(err)
	}
	// Agent: 指标先进入 WAL，Flush 之后读取才看得到。
	db.Flush()
	keys, err := db.GetHistoryKeys(run.ID)
	if err != nil || len(keys) != 1 || keys[0] != "loss" {
		t.Fatalf("keys %+v %v", keys, err)
	}

	runFile := "run-read"
	if err := db.InsertPendingRunFile(runFile, run.ID, "a.log", "/files/upload/"+runFile, "10000003"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE files SET object_key=? WHERE id=?`, "obj-run", runFile); err != nil {
		t.Fatal(err)
	}
	artifact, err := db.CreateArtifact(run.ID, "model", "m", nil)
	if err != nil {
		t.Fatal(err)
	}
	artFile := "art-read"
	if err := db.InsertPendingArtifactFile(artFile, run.ID, artifact.ID, "w.bin", "/files/upload/"+artFile, "10000003"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE artifact_files SET object_key=? WHERE id=?`, "obj-art", artFile); err != nil {
		t.Fatal(err)
	}

	stored, err := db.GetStoredFile(runFile)
	if err != nil || stored.ObjectKey != "obj-run" || stored.Name != "a.log" {
		t.Fatalf("stored run file %+v %v", stored, err)
	}
	storedArt, err := db.GetStoredFile(artFile)
	if err != nil || storedArt.ObjectKey != "obj-art" {
		t.Fatalf("stored art file %+v %v", storedArt, err)
	}
	visible, err := db.ListVisibleRunFiles(run.ID)
	if err != nil || len(visible) != 1 || visible[0].ID != runFile || visible[0].ObjectKey != "obj-run" {
		t.Fatalf("visible run %+v %v", visible, err)
	}
	visibleArt, err := db.ListVisibleArtifactFiles(artifact.ID)
	if err != nil || len(visibleArt) != 1 || visibleArt[0].ObjectKey != "obj-art" {
		t.Fatalf("visible art %+v %v", visibleArt, err)
	}
	listed, err := db.ListFiles(run.ID)
	if err != nil || len(listed) != 1 || listed[0].ID != runFile {
		t.Fatalf("list %+v %v", listed, err)
	}

	if _, err := db.softDeleteLiveRunFiles(run.ID); err != nil {
		t.Fatal(err)
	}
	visible, err = db.ListVisibleRunFiles(run.ID)
	if err != nil || len(visible) != 0 {
		t.Fatalf("soft-deleted run files still visible %+v %v", visible, err)
	}
	visibleArt, err = db.ListVisibleArtifactFiles(artifact.ID)
	if err != nil || len(visibleArt) != 0 {
		t.Fatalf("soft-deleted artifact files still visible %+v %v", visibleArt, err)
	}
	listed, err = db.ListFiles(run.ID)
	if err != nil || len(listed) != 0 {
		t.Fatalf("soft-deleted list %+v %v", listed, err)
	}
}
