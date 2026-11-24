package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sarna/worb/internal/auth"
	"github.com/sarna/worb/internal/store"
)

type stepChartLookup struct{}

func (stepChartLookup) Lookup(ctx context.Context, token string) (string, int) {
	return "1", 0
}

func mustJSON(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}

func TestRunChartHistoryStartsAtLoggedStep(t *testing.T) {
	auth.SetLookup(stepChartLookup{})
	dir := t.TempDir()
	db, err := store.New(dir, "sqlite")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	run, err := db.UpsertRun(store.UpsertRunParams{
		Entity:      "local",
		Project:     "step-from-20",
		Name:        "run-step20",
		DisplayName: "step-from-20",
	})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 10)
	for i := 0; i < 10; i++ {
		step := 20 + i
		batch[i] = struct {
			Step int
			Data json.RawMessage
		}{
			Step: i,
			Data: mustJSON(t, map[string]any{
				"_step":        float64(step),
				"train/loss":   1.0 - float64(i)*0.05,
				"val/accuracy": 0.5 + float64(i)*0.04,
			}),
		}
	}
	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	s := &Server{
		config: Config{Host: "127.0.0.1", Port: 18080, DataDir: dir, DBEngine: "sqlite"},
		store:  db,
	}
	s.setupRoutes()
	ts := httptest.NewServer(s.router)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/api/runs/" + run.ID + "/history?keys=train%2Floss")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history status %d", resp.StatusCode)
	}

	var steps []float64
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var p store.ScalarPoint
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("ndjson: %v", err)
		}
		steps = append(steps, p.Step)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(steps) != 10 || steps[0] != 20 || steps[len(steps)-1] != 29 {
		t.Fatalf("history steps = %v, want 20..29", steps)
	}

	if os.Getenv("WORB_KEEP_UI") == "1" {
		url := ts.URL + "/runs/" + run.ID
		t.Logf("WORB_UI_URL=%s", url)
		if err := os.WriteFile("/tmp/worb-ui-url.txt", []byte(url+"\n"), 0644); err != nil {
			t.Fatalf("write url: %v", err)
		}
		time.Sleep(90 * time.Second)
	}
}
