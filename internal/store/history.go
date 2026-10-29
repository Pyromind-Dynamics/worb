package store

import (
	"encoding/json"
	"fmt"
)

func (db *DB) InsertHistory(runID string, step int, data json.RawMessage) error {
	return db.InsertHistoryBatch(runID, []struct {
		Step int
		Data json.RawMessage
	}{{Step: step, Data: data}})
}

func (db *DB) InsertHistoryBatch(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	entry := walEntry{RunID: runID}
	for _, r := range rows {
		entry.History = append(entry.History, walHistoryRow{Step: r.Step, Data: r.Data})
	}
	return db.wal.Append(entry)
}

type parsedScalar struct {
	step  int
	key   string
	value float64
	xStep *float64
}

type parsedHistogram struct {
	step  int
	key   string
	xStep *float64
	bins  string
	vals  string
}

func parseHistoryRows(rows []struct {
	Step int
	Data json.RawMessage
}) ([]parsedScalar, []parsedHistogram) {
	var scalars []parsedScalar
	var histograms []parsedHistogram

	for _, r := range rows {
		var obj map[string]json.RawMessage
		if json.Unmarshal(r.Data, &obj) != nil {
			continue
		}

		var xStep *float64
		if raw, ok := obj["step"]; ok {
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				xStep = &v
			}
		}

		for key, raw := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				scalars = append(scalars, parsedScalar{step: r.Step, key: key, value: v, xStep: xStep})
				continue
			}
			var histObj struct {
				Type   string    `json:"_type"`
				Bins   []float64 `json:"bins"`
				Values []float64 `json:"values"`
			}
			if json.Unmarshal(raw, &histObj) == nil && histObj.Type == "histogram" && len(histObj.Bins) >= 2 && len(histObj.Values) > 0 {
				binsJSON, _ := json.Marshal(histObj.Bins)
				valsJSON, _ := json.Marshal(histObj.Values)
				histograms = append(histograms, parsedHistogram{step: r.Step, key: key, xStep: xStep, bins: string(binsJSON), vals: string(valsJSON)})
			}
		}
	}
	return scalars, histograms
}

const (
	multiRowSize = 100
	txChunkSize  = 100000 // max rows per transaction
)

func (db *DB) flushScalars(runID string, scalars []parsedScalar) error {
	keySet := map[string]struct{}{}

	for txStart := 0; txStart < len(scalars); txStart += txChunkSize {
		txEnd := txStart + txChunkSize
		if txEnd > len(scalars) {
			txEnd = len(scalars)
		}
		txSlice := scalars[txStart:txEnd]

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		txStepSet := map[int]struct{}{}
		// Agent: 批量插入按 100 行和包大小拆开，语句由 sql_manager 按引擎改写。
		args := make([]any, 0, len(txSlice)*5)
		for _, s := range txSlice {
			args = append(args, runID, s.step, s.key, s.value, s.xStep)
			keySet[s.key] = struct{}{}
			txStepSet[s.step] = struct{}{}
		}
		if err := db.execIgnoreBatches(tx, SQLInsertScalarsPrefix, "(?,?,?,?,?)", 5, args); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert scalars: %w", err)
		}

		if len(txStepSet) > 0 {
			stepArgs := make([]any, 0, len(txStepSet)*2)
			for step := range txStepSet {
				stepArgs = append(stepArgs, runID, step)
			}
			if err := db.execIgnoreBatches(tx, SQLInsertRunStepsPrefix, "(?,?)", 2, stepArgs); err != nil {
				tx.Rollback()
				return fmt.Errorf("insert run_steps: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit scalars: %w", err)
		}

	}

	// Insert new keys into run_keys (small, separate tx)
	if len(keySet) > 0 {
		// Agent: 指标名批量写入，超包时由 execIgnoreBatches 拆开。
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin keys tx: %w", err)
		}
		args := make([]any, 0, len(keySet)*2)
		for k := range keySet {
			args = append(args, runID, k)
		}
		if err := db.execIgnoreBatches(tx, SQLInsertRunKeysPrefix, "(?,?)", 2, args); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert run_keys: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit run_keys: %w", err)
		}
	}

	return nil
}

func (db *DB) flushHistograms(runID string, histograms []parsedHistogram) error {
	for txStart := 0; txStart < len(histograms); txStart += txChunkSize {
		txEnd := txStart + txChunkSize
		if txEnd > len(histograms) {
			txEnd = len(histograms)
		}
		txSlice := histograms[txStart:txEnd]

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		// Agent: 先按 100 行切开，execIgnoreBatches 再按包大小拆小。
		for i := 0; i < len(txSlice); i += multiRowSize {
			end := i + multiRowSize
			if end > len(txSlice) {
				end = len(txSlice)
			}
			chunk := txSlice[i:end]
			args := make([]any, 0, len(chunk)*6)
			for _, hh := range chunk {
				args = append(args, runID, hh.step, hh.key, hh.xStep, hh.bins, hh.vals)
			}
			if err := db.execIgnoreBatches(tx, SQLInsertHistsPrefix, "(?,?,?,?,?,?)", 6, args); err != nil {
				tx.Rollback()
				return fmt.Errorf("insert histograms: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit histograms: %w", err)
		}

	}
	return nil
}

func (db *DB) flushEvents(runID string, events []eventItem) error {
	for txStart := 0; txStart < len(events); txStart += txChunkSize {
		txEnd := txStart + txChunkSize
		if txEnd > len(events) {
			txEnd = len(events)
		}
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		// Agent: 准备语句也走当前引擎的写法。
		stmt, err := tx.Prepare(db.SQL(SQLInsertSysEvent))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare events: %w", err)
		}
		for _, e := range events[txStart:txEnd] {
			if _, err := stmt.Exec(runID, e.lineNum, e.data); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("insert event %d: %w", e.lineNum, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit events: %w", err)
		}

	}
	return nil
}

func (db *DB) flushLogs(runID string, logs []logItem) error {
	for txStart := 0; txStart < len(logs); txStart += txChunkSize {
		txEnd := txStart + txChunkSize
		if txEnd > len(logs) {
			txEnd = len(logs)
		}
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		// Agent: 准备语句也走当前引擎的写法。
		stmt, err := tx.Prepare(db.SQL(SQLInsertConsLog))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare logs: %w", err)
		}
		for _, l := range logs[txStart:txEnd] {
			if _, err := stmt.Exec(runID, l.lineNum, l.line); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("insert log %d: %w", l.lineNum, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit logs: %w", err)
		}
	}
	return nil
}

func (db *DB) GetHistoryKeys(runID string) ([]string, error) {
	rows, err := db.Query(db.SQL(SQLSelectRunKeys), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := []string{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// isSQLiteBusy 同时识别 SQLite 锁和 MySQL 的死锁、锁等待超时。
// isSQLiteBusy reports SQLite locks and MySQL deadlock or lock-wait timeout.
func isSQLiteBusy(err error) bool {
	// Agent: MySQL 1213/1205 与 SQLITE_BUSY 一样走回退查询。
	return isBusyLock(err)
}

func (db *DB) ensureRunSteps(runID string) error {
	var historyLineCount int
	if err := db.QueryRow(db.SQL(SQLSelectHistoryLineCount), runID).Scan(&historyLineCount); err != nil {
		return err
	}

	var stepCount int
	if err := db.QueryRow(db.SQL(SQLCountRunSteps), runID).Scan(&stepCount); err != nil {
		return err
	}
	if stepCount > 0 || historyLineCount == 0 {
		return nil
	}

	if _, err := db.Exec(db.SQL(SQLBackfillRunSteps), runID); err != nil {
		if isSQLiteBusy(err) {
			return err
		}
		return fmt.Errorf("backfill run_steps: %w", err)
	}

	var count int
	if err := db.QueryRow(db.SQL(SQLCountRunSteps), runID).Scan(&count); err != nil {
		return err
	}
	if _, err := db.Exec(db.SQL(SQLSetHistoryLineCount), count, runID); err != nil {
		return err
	}
	return nil
}

func (db *DB) countRunSteps(runID string, xMin, xMax *float64) (int, error) {
	if err := db.ensureRunSteps(runID); err != nil {
		if !isSQLiteBusy(err) {
			return 0, err
		}
		query := db.SQL(SQLCountDistinctSteps)
		args := []interface{}{runID}
		if xMin != nil {
			query += " AND step >= ?"
			args = append(args, int(*xMin))
		}
		if xMax != nil {
			query += " AND step <= ?"
			args = append(args, int(*xMax))
		}
		var count int
		if err := db.QueryRow(query, args...).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}
	if xMin == nil && xMax == nil {
		var count int
		if err := db.QueryRow(db.SQL(SQLSelectHistoryLineCount), runID).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}

	query := db.SQL(SQLCountRunSteps)
	args := []interface{}{runID}
	if xMin != nil {
		query += " AND step >= ?"
		args = append(args, int(*xMin))
	}
	if xMax != nil {
		query += " AND step <= ?"
		args = append(args, int(*xMax))
	}
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

type ScalarPoint struct {
	Key   string   `json:"k"`
	Step  float64  `json:"s"`
	Value float64  `json:"v"`
	Min   *float64 `json:"min,omitempty"`
	Max   *float64 `json:"max,omitempty"`
	Index int      `json:"i"`
}

func (db *DB) StreamHistoryScalars(runID string, maxPoints int, filterKeys []string, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	if len(filterKeys) == 0 {
		return nil
	}

	needsBucket := false
	bucketSize := 1
	if maxPoints > 0 {
		totalRows, err := db.countRunSteps(runID, xMin, xMax)
		if err != nil {
			return err
		}
		if totalRows > maxPoints {
			needsBucket = true
			bucketSize = totalRows / maxPoints
			if bucketSize == 0 {
				bucketSize = 1
			}
		}
	}

	for _, key := range filterKeys {
		if err := db.streamKeyScalars(runID, key, needsBucket, bucketSize, xMin, xMax, emit); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) streamKeyScalars(runID, key string, needsBucket bool, bucketSize int, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	args := []interface{}{runID, key}
	stepFilter := ""
	if xMin != nil {
		stepFilter += " AND step >= ?"
		args = append(args, int(*xMin))
	}
	if xMax != nil {
		stepFilter += " AND step <= ?"
		args = append(args, int(*xMax))
	}

	if !needsBucket {
		// Agent: 按编号取语句，再填入 step 过滤。
		query := fmt.Sprintf(db.SQL(SQLSelectScalarsByKey), stepFilter)
		rows, err := db.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var x, value float64
			var step int
			if err := rows.Scan(&x, &value, &step); err != nil {
				return err
			}
			if err := emit(ScalarPoint{Key: key, Step: x, Value: value, Index: step}); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	// Agent: 分桶大小写入 DIV/除法模板。
	query := fmt.Sprintf(db.SQLScalarsBucket(bucketSize), stepFilter)
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var x, avg, mn, mx float64
		if err := rows.Scan(&x, &avg, &mn, &mx); err != nil {
			return err
		}
		mnCopy, mxCopy := mn, mx
		if err := emit(ScalarPoint{Key: key, Step: x, Value: avg, Min: &mnCopy, Max: &mxCopy, Index: int(x)}); err != nil {
			return err
		}
	}
	return rows.Err()
}

type ProjectScalarPoint struct {
	RunID   string   `json:"r"`
	RunName string   `json:"n"`
	Key     string   `json:"k"`
	Step    float64  `json:"s"`
	Value   float64  `json:"v"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Index   int      `json:"i"`
}

func (db *DB) StreamProjectHistoryScalars(projectID string, maxPoints int, emit func(ProjectScalarPoint) error) error {
	type runInfo struct {
		id   string
		name string
	}
	infoRows, err := db.Query(db.SQL(SQLSelectProjectRuns), projectID)
	if err != nil {
		return err
	}
	var runs []runInfo
	for infoRows.Next() {
		var ri runInfo
		if err := infoRows.Scan(&ri.id, &ri.name); err != nil {
			infoRows.Close()
			return err
		}
		runs = append(runs, ri)
	}
	infoRows.Close()

	needsBucket := false
	bucketSize := 1
	if maxPoints > 0 {
		var maxRunRows int
		db.QueryRow(db.SQL(SQLMaxHistoryLineCount), projectID).Scan(&maxRunRows)
		if maxRunRows > maxPoints {
			needsBucket = true
			bucketSize = maxRunRows / maxPoints
		}
	}

	for _, ri := range runs {
		if !needsBucket {
			rows, err := db.Query(db.SQL(SQLSelectScalarsByRun), ri.id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var key string
				var x, value float64
				var step int
				if err := rows.Scan(&key, &x, &value, &step); err != nil {
					rows.Close()
					return err
				}
				if err := emit(ProjectScalarPoint{RunID: ri.id, RunName: ri.name, Key: key, Step: x, Value: value, Index: step}); err != nil {
					rows.Close()
					return err
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		} else {
			rows, err := db.Query(db.SQLProjectScalarsBucket(bucketSize), ri.id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var key string
				var x, avg, mn, mx float64
				if err := rows.Scan(&key, &x, &avg, &mn, &mx); err != nil {
					rows.Close()
					return err
				}
				mnCopy, mxCopy := mn, mx
				if err := emit(ProjectScalarPoint{RunID: ri.id, RunName: ri.name, Key: key, Step: x, Value: avg, Min: &mnCopy, Max: &mxCopy, Index: int(x)}); err != nil {
					rows.Close()
					return err
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
	}
	return nil
}

type HistogramPoint struct {
	Key    string    `json:"k"`
	Step   float64   `json:"s"`
	Bins   []float64 `json:"b"`
	Values []float64 `json:"v"`
}

func (db *DB) StreamHistoryHistograms(runID string, emit func(HistogramPoint) error) error {
	rows, err := db.Query(db.SQL(SQLSelectHistogramsByRun), runID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var step int
		var key, binsStr, valsStr string
		var xStep *float64
		if err := rows.Scan(&step, &key, &xStep, &binsStr, &valsStr); err != nil {
			return err
		}
		x := float64(step)
		if xStep != nil {
			x = *xStep
		}
		var bins, vals []float64
		if json.Unmarshal([]byte(binsStr), &bins) != nil {
			continue
		}
		if json.Unmarshal([]byte(valsStr), &vals) != nil {
			continue
		}
		if err := emit(HistogramPoint{Key: key, Step: x, Bins: bins, Values: vals}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (db *DB) UpdateRunSummary(runID string, summary json.RawMessage) error {
	_, err := db.Exec(db.SQL(SQLUpdateRunSummary), string(summary), runID)
	return err
}

func (db *DB) InsertSystemEvent(runID string, lineNum int, data json.RawMessage) error {
	db.InsertSystemEventBatch(runID, []struct {
		LineNum int
		Data    json.RawMessage
	}{{LineNum: lineNum, Data: data}})
	return nil
}

func (db *DB) InsertSystemEventBatch(runID string, rows []struct {
	LineNum int
	Data    json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	entry := walEntry{RunID: runID}
	for _, r := range rows {
		entry.Events = append(entry.Events, walEventRow{LineNum: r.LineNum, Data: r.Data})
	}
	return db.wal.Append(entry)
}

func (db *DB) InsertConsoleLog(runID string, lineNum int, line, stream string) error {
	db.InsertConsoleLogBatch(runID, []struct {
		LineNum int
		Line    string
	}{{LineNum: lineNum, Line: line}})
	return nil
}

func (db *DB) InsertConsoleLogBatch(runID string, rows []struct {
	LineNum int
	Line    string
}) error {
	if len(rows) == 0 {
		return nil
	}
	entry := walEntry{RunID: runID}
	for _, r := range rows {
		entry.Logs = append(entry.Logs, walLogRow{LineNum: r.LineNum, Line: r.Line})
	}
	return db.wal.Append(entry)
}

func (db *DB) GetConsoleLogs(runID string) ([]string, error) {
	rows, err := db.Query(db.SQL(SQLSelectConsoleLines), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}
