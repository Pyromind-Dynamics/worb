package store

import (
	"encoding/json"
	"testing"
)

func TestFlushScalarsMaintainsRunSteps(t *testing.T) {
	db := setupTestDB(t)

	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-steps", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 1, Data: mustJSON(t, map[string]any{"loss": 1.0, "acc": 0.1})},
		{Step: 2, Data: mustJSON(t, map[string]any{"loss": 2.0, "acc": 0.2})},
		{Step: 2, Data: mustJSON(t, map[string]any{"loss": 2.1, "acc": 0.3})},
		{Step: 4, Data: mustJSON(t, map[string]any{"loss": 4.0})},
	}

	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	var stepCount, historyLineCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount); err != nil {
		t.Fatalf("run_steps count: %v", err)
	}
	if err := db.QueryRow("SELECT history_line_count FROM runs WHERE id = ?", run.ID).Scan(&historyLineCount); err != nil {
		t.Fatalf("history_line_count: %v", err)
	}
	if stepCount != 3 {
		t.Fatalf("expected 3 unique steps, got %d", stepCount)
	}
	if historyLineCount != 3 {
		t.Fatalf("expected history_line_count=3, got %d", historyLineCount)
	}
}

func TestStreamHistoryScalarsBackfillsRunSteps(t *testing.T) {
	db := setupTestDB(t)

	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-backfill", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 10, Data: mustJSON(t, map[string]any{"loss": 10.0})},
		{Step: 12, Data: mustJSON(t, map[string]any{"loss": 12.0})},
		{Step: 15, Data: mustJSON(t, map[string]any{"loss": 15.0})},
	}

	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	if _, err := db.Exec("DELETE FROM run_steps WHERE run_id = ?", run.ID); err != nil {
		t.Fatalf("clear run_steps: %v", err)
	}
	if _, err := db.Exec("UPDATE runs SET history_line_count = 3 WHERE id = ?", run.ID); err != nil {
		t.Fatalf("restore history_line_count: %v", err)
	}

	xMin := 11.0
	xMax := 15.0
	var points []ScalarPoint
	err = db.StreamHistoryScalars(run.ID, 2, []string{"loss"}, &xMin, &xMax, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamHistoryScalars: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("expected 2 points after exact backfill count, got %d", len(points))
	}

	var stepCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount); err != nil {
		t.Fatalf("run_steps count: %v", err)
	}
	if stepCount != 3 {
		t.Fatalf("expected 3 backfilled steps, got %d", stepCount)
	}
}

func TestHistoryUsesWandbStepNotFileOffset(t *testing.T) {
	db := setupTestDB(t)

	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-wandb-step", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	// Agent: file_stream 行号从 0 起，JSON 里的 `_step` 从 20 起，图表应显示 20。
	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 0, Data: mustJSON(t, map[string]any{"_step": 20.0, "train/loss": 1.0})},
		{Step: 1, Data: mustJSON(t, map[string]any{"_step": 21.0, "train/loss": 0.9})},
		{Step: 2, Data: mustJSON(t, map[string]any{"_step": 29.0, "train/loss": 0.5})},
		{Step: 3, Data: mustJSON(t, map[string]any{"loss": 0.1})},
	}

	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	var points []ScalarPoint
	err = db.StreamHistoryScalars(run.ID, 0, []string{"train/loss", "loss"}, nil, nil, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamHistoryScalars: %v", err)
	}

	got := map[string][]float64{}
	for _, p := range points {
		got[p.Key] = append(got[p.Key], p.Step)
	}
	if len(got["train/loss"]) != 3 || got["train/loss"][0] != 20 || got["train/loss"][1] != 21 || got["train/loss"][2] != 29 {
		t.Fatalf("train/loss steps = %v, want 20,21,29", got["train/loss"])
	}
	if len(got["loss"]) != 1 || got["loss"][0] != 3 {
		t.Fatalf("fallback loss step = %v, want file offset 3", got["loss"])
	}

	var minStep, maxStep int
	if err := db.QueryRow("SELECT MIN(step), MAX(step) FROM history_scalars WHERE run_id = ? AND `key` = ?", run.ID, "train/loss").Scan(&minStep, &maxStep); err != nil {
		t.Fatalf("min/max step: %v", err)
	}
	if minStep != 20 || maxStep != 29 {
		t.Fatalf("stored train/loss steps min=%d max=%d, want 20..29", minStep, maxStep)
	}
}

func mustJSON(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}
