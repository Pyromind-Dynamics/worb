package store

import (
	"fmt"
	"log"
)

type scalarItem struct {
	runID string
	parsedScalar
}

type histogramItem struct {
	runID string
	parsedHistogram
}

type eventItem struct {
	runID   string
	lineNum int
	data    string
}

type logItem struct {
	runID   string
	lineNum int
	line    string
}

type counterDelta struct {
	runID  string
	column string
	delta  int
}

func collectRunIDs[T any](items []T, getRunID func(T) string) []string {
	seen := map[string]bool{}
	var ids []string
	for _, item := range items {
		id := getRunID(item)
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func (db *DB) processScalars(items []scalarItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i scalarItem) string { return i.runID }))
	byRun := map[string][]parsedScalar{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item.parsedScalar)
	}
	total := 0
	for runID, scalars := range byRun {
		if err := db.flushScalars(runID, scalars); err != nil {
			log.Printf("[flush] ERROR scalars run=%s: %v", runID[:8], err)
		}
		total += len(scalars)
	}
	if total > 0 {
		log.Printf("[flush] OK scalars runs=%d count=%d", len(byRun), total)
	}
	return nil
}

func (db *DB) processHistograms(items []histogramItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i histogramItem) string { return i.runID }))
	byRun := map[string][]parsedHistogram{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item.parsedHistogram)
	}
	total := 0
	for runID, histograms := range byRun {
		if err := db.flushHistograms(runID, histograms); err != nil {
			log.Printf("[flush] ERROR histograms run=%s: %v", runID[:8], err)
		}
		total += len(histograms)
	}
	if total > 0 {
		log.Printf("[flush] OK histograms runs=%d count=%d", len(byRun), total)
	}
	return nil
}

func (db *DB) processEvents(items []eventItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i eventItem) string { return i.runID }))
	byRun := map[string][]eventItem{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item)
	}
	for runID, events := range byRun {
		if err := db.flushEvents(runID, events); err != nil {
			log.Printf("[flush] ERROR events run=%s: %v", runID[:8], err)
		}
	}
	return nil
}

func (db *DB) processLogs(items []logItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i logItem) string { return i.runID }))
	byRun := map[string][]logItem{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item)
	}
	for runID, logs := range byRun {
		if err := db.flushLogs(runID, logs); err != nil {
			log.Printf("[flush] ERROR logs run=%s: %v", runID[:8], err)
		}
	}
	return nil
}

// counterQueryIDs 把计数列映射到 sql_manager 编号。用绝对计数，重复迁移也安全。
// counterQueryIDs maps counter columns to sql_manager IDs. Absolute counts keep remigration safe.
var counterQueryIDs = map[string]SQLID{
	"history_line_count": SQLCountRunSteps,
	"events_line_count":  SQLCountSysEvents,
	"log_line_count":     SQLCountConsLogs,
}

func (db *DB) processCounters(items []counterDelta) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i counterDelta) string { return i.runID }))
	// Collect unique (runID, column) pairs that need updating.
	type key struct {
		runID  string
		column string
	}
	seen := map[key]bool{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		seen[key{item.runID, item.column}] = true
	}
	for k := range seen {
		id, ok := counterQueryIDs[k.column]
		if !ok {
			log.Printf("[flush] WARN unknown counter column %q", k.column)
			continue
		}
		var count int
		if err := db.QueryRow(db.SQL(id), k.runID).Scan(&count); err != nil {
			log.Printf("[flush] ERROR count %s run=%s: %v", k.column, k.runID[:8], err)
			continue
		}
		// Agent: 计数字段更新只允许三个已知列，语句按编号取。
		updateSQL, err := db.SQLSetRunColumn(k.column)
		if err != nil {
			log.Printf("[flush] ERROR %s run=%s: %v", k.column, k.runID[:8], err)
			continue
		}
		if _, err := db.Exec(updateSQL, count, k.runID); err != nil {
			log.Printf("[flush] ERROR %s run=%s: %v", k.column, k.runID[:8], err)
		}
	}
	return nil
}

func (db *DB) processForkOp(targetRunID string, op walForkOp) error {
	log.Printf("[fork] starting copy run=%s parent=%s maxStep=%d", targetRunID[:8], op.ParentRunID[:8], op.MaxStep)

	var maxStep int
	if err := db.QueryRow(db.SQL(SQLMaxParentStep), op.ParentRunID).Scan(&maxStep); err != nil {
		return fmt.Errorf("query parent max step: %w", err)
	}
	if op.MaxStep > maxStep {
		msg := fmt.Sprintf("invalid fork_step %d: parent run has max step %d", op.MaxStep, maxStep)
		_, _ = db.Exec(db.SQL(SQLMarkRunFailed), msg, targetRunID)
		return fmt.Errorf("%s", msg)
	}

	// Copy scalars in batches
	offset := 0
	totalScalars := 0
	for {
		// Agent: 父 run 标量拷贝语句按编号取。
		rows, err := db.Query(
			db.SQL(SQLSelectParentScalars),
			op.ParentRunID, op.MaxStep, txChunkSize, offset,
		)
		if err != nil {
			return fmt.Errorf("read parent scalars: %w", err)
		}

		var batch []parsedScalar
		for rows.Next() {
			var s parsedScalar
			if err := rows.Scan(&s.step, &s.key, &s.value, &s.xStep); err != nil {
				rows.Close()
				return fmt.Errorf("scan parent scalar: %w", err)
			}
			batch = append(batch, s)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		if err := db.flushScalars(targetRunID, batch); err != nil {
			return fmt.Errorf("insert fork scalars: %w", err)
		}
		totalScalars += len(batch)
		offset += len(batch)

		if len(batch) < txChunkSize {
			break
		}
	}

	// Copy histograms in batches
	offset = 0
	totalHistograms := 0
	for {
		rows, err := db.Query(
			db.SQL(SQLSelectParentHists),
			op.ParentRunID, op.MaxStep, txChunkSize, offset,
		)
		if err != nil {
			return fmt.Errorf("read parent histograms: %w", err)
		}

		var batch []parsedHistogram
		for rows.Next() {
			var h parsedHistogram
			if err := rows.Scan(&h.step, &h.key, &h.xStep, &h.bins, &h.vals); err != nil {
				rows.Close()
				return fmt.Errorf("scan parent histogram: %w", err)
			}
			batch = append(batch, h)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		if err := db.flushHistograms(targetRunID, batch); err != nil {
			return fmt.Errorf("insert fork histograms: %w", err)
		}
		totalHistograms += len(batch)
		offset += len(batch)

		if len(batch) < txChunkSize {
			break
		}
	}

	// Copy run_keys (small, single operation)
	if _, err := db.Exec(db.SQL(SQLCopyRunKeys), targetRunID, op.ParentRunID); err != nil {
		return fmt.Errorf("copy run_keys: %w", err)
	}

	// Update history_line_count
	var count int
	if err := db.QueryRow(db.SQL(SQLCountRunSteps), targetRunID).Scan(&count); err != nil {
		return fmt.Errorf("count run_steps: %w", err)
	}
	setCount, err := db.SQLSetRunColumn("history_line_count")
	if err != nil {
		return err
	}
	if _, err := db.Exec(setCount, count, targetRunID); err != nil {
		return fmt.Errorf("update history_line_count: %w", err)
	}

	log.Printf("[fork] complete run=%s scalars=%d histograms=%d steps=%d", targetRunID[:8], totalScalars, totalHistograms, count)
	return nil
}
