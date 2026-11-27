package store

import (
	"database/sql"
	"log"
	"time"
)

// StartCleanup launches a background goroutine that periodically purges
// data belonging to soft-deleted runs and projects.
func (db *DB) StartCleanup() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if !db.hasDeletedData() {
				continue
			}
			_, err := db.purgeDeletedData(5000)
			if err != nil {
				log.Printf("cleanup error: %v", err)
			}
		}
	}()
}

func (db *DB) hasDeletedData() bool {
	var id string
	err := db.QueryRow(db.SQL(SQLSelectDeletedRun)).Scan(&id)
	if err == nil {
		return true
	}
	err = db.QueryRow(db.SQL(SQLSelectDeletedProject)).Scan(&id)
	return err == nil
}

const (
	// Agent: 文件行删不掉的 run 先暂缓 5 秒，之后每次仍无进展就翻倍，最长 5 分钟。有进展则回到 5 秒。
	parkBase = 5 * time.Second
	parkMax  = 5 * time.Minute
)

// parkedRun 记录一个因文件行未清完而暂缓的 run。
// parkedRun records a run that is parked because file rows are still waiting for the async deleter.
type parkedRun struct {
	until   time.Time
	waiting int
	delay   time.Duration
}

// cleanupTime 返回清理用的当前时间，测试可以替换。
// cleanupTime returns the clock used by cleanup; tests may replace it.
func (db *DB) cleanupTime() time.Time {
	if db.cleanupNow != nil {
		return db.cleanupNow()
	}
	return time.Now()
}

// parkedCount 先清掉早已过期的记录，再返回仍在暂缓期内的 run 数。
// parkedCount drops long-expired entries and returns how many runs are still parked.
func (db *DB) parkedCount(now time.Time) int {
	db.cleanupMu.Lock()
	defer db.cleanupMu.Unlock()
	n := 0
	for id, p := range db.cleanupParked {
		// Agent: 过期超过 parkMax 仍没人回访的记录多半是 run 已被别处删掉，丢弃以免一直占着候选名额。
		if now.After(p.until.Add(parkMax)) {
			delete(db.cleanupParked, id)
			continue
		}
		if now.Before(p.until) {
			n++
		}
	}
	return n
}

func (db *DB) isParked(runID string, now time.Time) bool {
	db.cleanupMu.Lock()
	defer db.cleanupMu.Unlock()
	p, ok := db.cleanupParked[runID]
	return ok && now.Before(p.until)
}

// parkRun 登记或续期暂缓，并返回这次之前是否已经暂缓过。
// parkRun records or extends the parking and reports whether the run had been parked before.
func (db *DB) parkRun(runID string, waiting int, now time.Time) bool {
	db.cleanupMu.Lock()
	defer db.cleanupMu.Unlock()
	if db.cleanupParked == nil {
		db.cleanupParked = make(map[string]parkedRun)
	}
	prev, seen := db.cleanupParked[runID]
	delay := parkBase
	// Agent: 行数减少说明异步删除在推进，保持短间隔；没减少就翻倍，避免对删不动的 run 反复查询。
	if seen && waiting >= prev.waiting {
		delay = prev.delay * 2
		if delay > parkMax {
			delay = parkMax
		}
	}
	db.cleanupParked[runID] = parkedRun{until: now.Add(delay), waiting: waiting, delay: delay}
	return seen
}

func (db *DB) unparkRun(runID string) {
	db.cleanupMu.Lock()
	delete(db.cleanupParked, runID)
	db.cleanupMu.Unlock()
}

// deletedRunCandidates 按 id 取软删 run，数量是暂缓中的 run 数加 1。
// deletedRunCandidates lists soft-deleted runs by id, as many as the parked runs plus one.
func (db *DB) deletedRunCandidates(limit int) ([]string, error) {
	rows, err := db.Query(db.SQL(SQLSelectDeletedRuns), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// purgeDeletedData 分批清掉软删 run 的指标和日志。文件行只补软删并入队，等异步硬删后再删 run。
// 文件行迟迟删不掉的 run 会暂缓并让位给其它 run，不会挡住别的 run 和项目清理。
// purgeDeletedData purges metrics and logs for a soft-deleted run. File rows are only soft-deleted and queued.
// A run whose file rows are slow to disappear is parked so it never blocks other runs or project cleanup.
func (db *DB) purgeDeletedData(batchSize int) (bool, error) {
	now := db.cleanupTime()
	// Agent: 候选数 = 暂缓数 + 1，其中至少有一个不在暂缓里，不会因为队头全是暂缓 run 而选不到别的 run。
	candidates, err := db.deletedRunCandidates(db.parkedCount(now) + 1)
	if err != nil {
		return false, err
	}
	for _, runID := range candidates {
		if db.isParked(runID, now) {
			continue
		}
		more, parked, err := db.purgeOneRun(runID, batchSize, now)
		if err != nil {
			return false, err
		}
		// Agent: 这个 run 暂缓了，本次继续看下一个，不返回 true 让调用方一直重跑它。
		if parked {
			continue
		}
		return more, nil
	}
	// Agent: 没有可处理的软删 run（没有，或全部在暂缓）时才清孤儿项目。
	return db.purgeDeletedProjects()
}

// purgeOneRun 处理一个软删 run。返回 parked=true 表示文件行还没清完，已暂缓。
// purgeOneRun processes one soft-deleted run. parked=true means file rows remain and the run has been parked.
func (db *DB) purgeOneRun(runID string, batchSize int, now time.Time) (more bool, parked bool, err error) {
	// Agent: 指标和日志仍直接硬删。文件行里带前缀的要等异步清理。
	dataTables := []string{"history_scalars", "history_histograms", "run_steps", "run_keys", "system_events", "console_logs"}
	for _, table := range dataTables {
		deleted, err := db.deleteBatch(table, "run_id", runID, batchSize)
		if err != nil {
			return false, false, err
		}
		if deleted > 0 {
			return true, false, nil // more work to do
		}
	}

	// Agent: 尚未软删的文件行在这里补软删并入队。files 和 artifact_files 都不在 purge 里硬删。
	ids, err := db.softDeleteLiveRunFiles(runID)
	if err != nil {
		return false, false, err
	}
	db.notifyFileDelete(ids)
	waiting, err := db.CountAllFileRows(runID)
	if err != nil {
		return false, false, err
	}
	if waiting > 0 {
		revisit := db.parkRun(runID, waiting, now)
		// Agent: 第二次及以后回访仍有残留，说明之前入队的 id 可能丢了（队列满、进程重启）。把已软删的 id 再送一次。
		// 第一次不重送，DeleteRun 刚入过队，队列还在处理。
		if revisit {
			again, err := db.softFileIDs(SQLListSoftRunFileIDsByRun, SQLListSoftArtFileIDsByRun, runID)
			if err != nil {
				return false, false, err
			}
			db.notifyFileDelete(again)
		}
		return false, true, nil
	}
	db.unparkRun(runID)

	for _, table := range []string{"artifacts"} {
		deleted, err := db.deleteBatch(table, "run_id", runID, batchSize)
		if err != nil {
			return false, false, err
		}
		if deleted > 0 {
			return true, false, nil
		}
	}

	if _, err := db.Exec(db.SQL(SQLDeleteRunHard), runID); err != nil {
		return false, false, err
	}
	return true, false, nil // check for more deleted runs
}

func (db *DB) deleteBatch(table, column, id string, batchSize int) (int64, error) {
	// Agent: MySQL 使用 DELETE ... LIMIT，SQLite 仍按主键分批删除。
	// Agent: 分批删除语句按编号从 sql_manager 取。
	query, err := db.SQLDeleteBatch(table, column)
	if err != nil {
		return 0, err
	}
	res, err := db.Exec(query, id, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// purgeDeletedProjects hard-deletes projects that are soft-deleted and have no remaining runs.
func (db *DB) purgeDeletedProjects() (bool, error) {
	var projectID string
	err := db.QueryRow(db.SQL(SQLSelectOrphanProject)).Scan(&projectID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := db.Exec(db.SQL(SQLDeleteProjectHard), projectID); err != nil {
		return false, err
	}
	return true, nil
}
