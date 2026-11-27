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

func historyObj(t *testing.T, v map[string]any) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(mustJSON(t, v), &obj); err != nil {
		t.Fatalf("historyObj: %v", err)
	}
	return obj
}

// TestWandbHistoryStepFields `_step` 是 wandb 标准；没有时才认 `step`；都没有用行号。
// TestWandbHistoryStepFields uses wandb `_step` first, then `step`, then the file_stream offset.
func TestWandbHistoryStepFields(t *testing.T) {
	cases := []struct {
		name     string
		obj      map[string]any
		fallback int
		wantStep int
		wantX    bool
		wantXVal float64
	}{
		{name: "wandb _step", obj: map[string]any{"_step": 20.0, "loss": 1.0}, fallback: 0, wantStep: 20, wantX: true, wantXVal: 20},
		{name: "plain step", obj: map[string]any{"step": 15.0, "loss": 1.0}, fallback: 0, wantStep: 15, wantX: true, wantXVal: 15},
		// Agent: 同时有 `_step` 和 `step` 时必须用 wandb 标准 `_step`。
		{name: "prefer _step", obj: map[string]any{"_step": 20.0, "step": 99.0, "loss": 1.0}, fallback: 0, wantStep: 20, wantX: true, wantXVal: 20},
		{name: "invalid _step uses step", obj: map[string]any{"_step": "x", "step": 7.0, "loss": 1.0}, fallback: 0, wantStep: 7, wantX: true, wantXVal: 7},
		{name: "offset fallback", obj: map[string]any{"loss": 1.0}, fallback: 3, wantStep: 3, wantX: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, x := wandbHistoryStep(historyObj(t, tc.obj), tc.fallback)
			if step != tc.wantStep {
				t.Fatalf("step=%d want %d", step, tc.wantStep)
			}
			if tc.wantX {
				if x == nil || *x != tc.wantXVal {
					t.Fatalf("xStep=%v want %v", x, tc.wantXVal)
				}
				return
			}
			if x != nil {
				t.Fatalf("xStep=%v want nil", *x)
			}
		})
	}
}

// TestParseHistoryRowsStepNotAMetric `_step`/`step` 只当坐标，不会当成一条叫 step 的指标。
// TestParseHistoryRowsStepNotAMetric keeps `_step`/`step` as the axis, not as a plotted metric.
func TestParseHistoryRowsStepNotAMetric(t *testing.T) {
	scalars, _ := parseHistoryRows([]struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 0, Data: mustJSON(t, map[string]any{"_step": 20.0, "step": 99.0, "loss": 1.5})},
	})
	if len(scalars) != 1 {
		t.Fatalf("got %d scalars, want 1", len(scalars))
	}
	if scalars[0].key != "loss" || scalars[0].step != 20 {
		t.Fatalf("key=%s step=%d", scalars[0].key, scalars[0].step)
	}
	if scalars[0].xStep == nil || *scalars[0].xStep != 20 {
		t.Fatalf("xStep=%v want 20", scalars[0].xStep)
	}
}

// TestHistoryPlainStepWithoutUnderscore 只有 JSON `step` 时主键和图表 x 也用它，兼容旧解析和迁移数据。
// TestHistoryPlainStepWithoutUnderscore uses JSON `step` for PK and chart x when `_step` is absent.
func TestHistoryPlainStepWithoutUnderscore(t *testing.T) {
	db := setupTestDB(t)
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-plain-step", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}
	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 0, Data: mustJSON(t, map[string]any{"step": 40.0, "acc": 0.2})},
	}
	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	var points []ScalarPoint
	err = db.StreamHistoryScalars(run.ID, 0, []string{"acc"}, nil, nil, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamHistoryScalars: %v", err)
	}
	if len(points) != 1 || points[0].Step != 40 {
		t.Fatalf("points=%v want step 40", points)
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
