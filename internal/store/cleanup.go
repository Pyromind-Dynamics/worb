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

// purgeDeletedData 分批清掉软删 run 的指标和日志。文件行只补软删并入队，等异步硬删后再删 run。
// purgeDeletedData purges metrics and logs for a soft-deleted run. File rows are only soft-deleted and queued.
func (db *DB) purgeDeletedData(batchSize int) (bool, error) {
	// Find one soft-deleted run
	var runID string
	err := db.QueryRow(db.SQL(SQLSelectDeletedRun)).Scan(&runID)
	if err == sql.ErrNoRows {
		// No deleted runs — check for orphaned projects
		return db.purgeDeletedProjects()
	}
	if err != nil {
		return false, err
	}

	// Agent: 指标和日志仍直接硬删。文件行里带前缀的要等异步清理。
	dataTables := []string{"history_scalars", "history_histograms", "run_steps", "run_keys", "system_events", "console_logs"}
	for _, table := range dataTables {
		deleted, err := db.deleteBatch(table, "run_id", runID, batchSize)
		if err != nil {
			return false, err
		}
		if deleted > 0 {
			return true, nil // more work to do
		}
	}

	// Agent: 尚未软删的文件行在这里补软删并入队。files 和 artifact_files 都不在 purge 里硬删。
	ids, err := db.softDeleteLiveRunFiles(runID)
	if err != nil {
		return false, err
	}
	db.notifyFileDelete(ids)
	waiting, err := db.CountAllFileRows(runID)
	if err != nil {
		return false, err
	}
	if waiting > 0 {
		return true, nil
	}

	for _, table := range []string{"artifacts"} {
		deleted, err := db.deleteBatch(table, "run_id", runID, batchSize)
		if err != nil {
			return false, err
		}
		if deleted > 0 {
			return true, nil
		}
	}

	if _, err := db.Exec(db.SQL(SQLDeleteRunHard), runID); err != nil {
		return false, err
	}
	return true, nil // check for more deleted runs
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
