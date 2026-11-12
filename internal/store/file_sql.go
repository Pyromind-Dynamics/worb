package store

// Agent: 文件上云的语句按编号写入 SQLite / MySQL 两套映射。MySQL 启动不执行这些 DDL。

func registerFileCloudSQL() {
	// Agent: SQLite 可重复启动：建表 IF NOT EXISTS，补列失败由 migrate 忽略。
	sqliteSQLs[SQLCreateArtifactFiles] = `CREATE TABLE IF NOT EXISTS artifact_files (
		id TEXT PRIMARY KEY, run_id TEXT NOT NULL, artifact_id TEXT NOT NULL, name TEXT NOT NULL, url TEXT,
		size INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
		entity TEXT, entity_key TEXT, local_path TEXT, local_uploaded_at TEXT, status TEXT, storage_type TEXT,
		object_key TEXT, cloud_uploaded_at TEXT, cloud_etag TEXT, error_message TEXT,
		retry_count INTEGER NOT NULL DEFAULT 0, local_deleted_at TEXT, deleted_at TEXT, updated_at TEXT)`
	sqliteSQLs[SQLAlterFileEntity] = `ALTER TABLE files ADD COLUMN entity TEXT`
	sqliteSQLs[SQLAlterFileEntityKey] = `ALTER TABLE files ADD COLUMN entity_key TEXT`
	sqliteSQLs[SQLAlterFileLocalPath] = `ALTER TABLE files ADD COLUMN local_path TEXT`
	sqliteSQLs[SQLAlterFileLocalAt] = `ALTER TABLE files ADD COLUMN local_uploaded_at TEXT`
	sqliteSQLs[SQLAlterFileStatus] = `ALTER TABLE files ADD COLUMN status TEXT`
	sqliteSQLs[SQLAlterFileStorage] = `ALTER TABLE files ADD COLUMN storage_type TEXT`
	sqliteSQLs[SQLAlterFileObjectKey] = `ALTER TABLE files ADD COLUMN object_key TEXT`
	sqliteSQLs[SQLAlterFileCloudAt] = `ALTER TABLE files ADD COLUMN cloud_uploaded_at TEXT`
	sqliteSQLs[SQLAlterFileETag] = `ALTER TABLE files ADD COLUMN cloud_etag TEXT`
	sqliteSQLs[SQLAlterFileError] = `ALTER TABLE files ADD COLUMN error_message TEXT`
	sqliteSQLs[SQLAlterFileRetry] = `ALTER TABLE files ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0`
	sqliteSQLs[SQLAlterFileLocalDel] = `ALTER TABLE files ADD COLUMN local_deleted_at TEXT`
	sqliteSQLs[SQLAlterFileDeleted] = `ALTER TABLE files ADD COLUMN deleted_at TEXT`
	sqliteSQLs[SQLAlterFileUpdated] = `ALTER TABLE files ADD COLUMN updated_at TEXT`
	sqliteSQLs[SQLIdxFileEntity] = `CREATE INDEX IF NOT EXISTS idx_files_entity ON files(entity)`
	sqliteSQLs[SQLIdxFileEntityKey] = `CREATE INDEX IF NOT EXISTS idx_files_entity_key ON files(entity_key)`
	sqliteSQLs[SQLIdxFileStatus] = `CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)`
	sqliteSQLs[SQLIdxFileDeleted] = `CREATE INDEX IF NOT EXISTS idx_files_deleted_at ON files(deleted_at)`

	// Agent: MySQL DDL 只给 schema 对照，checkMySQLSchema 不执行它。
	mysqlSQLs[SQLCreateArtifactFiles] = `CREATE TABLE artifact_files (
		id VARCHAR(128) NOT NULL, run_id VARCHAR(128) NOT NULL, artifact_id VARCHAR(128) NOT NULL,
		name VARCHAR(1024) NOT NULL, url LONGTEXT, size BIGINT NOT NULL DEFAULT 0,
		created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
		entity VARCHAR(512) NULL, entity_key VARCHAR(512) NULL, local_path VARCHAR(2048) NULL,
		local_uploaded_at DATETIME NULL, status VARCHAR(32) NULL, storage_type VARCHAR(32) NULL,
		object_key VARCHAR(2048) NULL, cloud_uploaded_at DATETIME NULL, cloud_etag VARCHAR(255) NULL,
		error_message VARCHAR(1024) NULL, retry_count INT NOT NULL DEFAULT 0,
		local_deleted_at DATETIME NULL, deleted_at DATETIME NULL,
		updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		KEY idx_artifact_files_entity (entity),
		KEY idx_artifact_files_entity_key (entity_key),
		KEY idx_artifact_files_status (status),
		KEY idx_artifact_files_deleted_at (deleted_at),
		KEY idx_artifact_files_artifact_id (artifact_id),
		CONSTRAINT fk_artifact_files_artifact FOREIGN KEY (artifact_id) REFERENCES artifacts (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`
	for _, id := range []SQLID{
		SQLAlterFileEntity, SQLAlterFileEntityKey, SQLAlterFileLocalPath, SQLAlterFileLocalAt,
		SQLAlterFileStatus, SQLAlterFileStorage, SQLAlterFileObjectKey, SQLAlterFileCloudAt,
		SQLAlterFileETag, SQLAlterFileError, SQLAlterFileRetry, SQLAlterFileLocalDel,
		SQLAlterFileDeleted, SQLAlterFileUpdated, SQLIdxFileEntity, SQLIdxFileEntityKey,
		SQLIdxFileStatus, SQLIdxFileDeleted,
	} {
		// Agent: MySQL 禁止应用改表，这些编号给成空操作，避免误执行。
		mysqlSQLs[id] = `SELECT 1`
	}

	registerFileDML(sqliteSQLs, false)
	registerFileDML(mysqlSQLs, true)
}

func registerFileDML(dst map[SQLID]string, mysql bool) {
	now := "datetime('now')"
	if mysql {
		now = "CURRENT_TIMESTAMP"
	}
	// Agent: 两张表各一套语句，禁止运行时拼表名。
	dst[SQLInsertPendingFile] = `INSERT INTO files (id, run_id, name, url, entity, entity_key, status, retry_count) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0)`
	dst[SQLInsertPendingArtifactFile] = `INSERT INTO artifact_files (id, run_id, artifact_id, name, url, entity, entity_key, status, retry_count) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0)`
	dst[SQLMarkRunFileLocal] = `UPDATE files SET status='local', local_path=?, size=?, local_uploaded_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status IN ('pending','local','failed')`
	dst[SQLMarkArtFileLocal] = `UPDATE artifact_files SET status='local', local_path=?, size=?, local_uploaded_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status IN ('pending','local','failed')`
	dst[SQLClaimRunUpload] = `UPDATE files SET status='uploading', retry_count=retry_count+1, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status IN ('local','failed') AND retry_count < 3`
	dst[SQLClaimArtUpload] = `UPDATE artifact_files SET status='uploading', retry_count=retry_count+1, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status IN ('local','failed') AND retry_count < 3`
	dst[SQLSelectRunFile] = `SELECT id, run_id, name, COALESCE(entity,''), COALESCE(entity_key,''), COALESCE(local_path,''), COALESCE(status,''), COALESCE(storage_type,''), COALESCE(object_key,''), COALESCE(retry_count,0), COALESCE(deleted_at,''), COALESCE(local_deleted_at,'') FROM files WHERE id=?`
	dst[SQLSelectArtFile] = `SELECT id, run_id, name, COALESCE(entity,''), COALESCE(entity_key,''), COALESCE(local_path,''), COALESCE(status,''), COALESCE(storage_type,''), COALESCE(object_key,''), COALESCE(retry_count,0), COALESCE(deleted_at,''), COALESCE(local_deleted_at,'') FROM artifact_files WHERE id=?`
	dst[SQLFinishRunCloud] = `UPDATE files SET status='cloud', storage_type=?, object_key=?, cloud_etag=?, cloud_uploaded_at=` + now + `, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL`
	dst[SQLFinishArtCloud] = `UPDATE artifact_files SET status='cloud', storage_type=?, object_key=?, cloud_etag=?, cloud_uploaded_at=` + now + `, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL`
	dst[SQLFailRunUpload] = `UPDATE files SET status='failed', error_message=?, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status='uploading'`
	dst[SQLFailArtUpload] = `UPDATE artifact_files SET status='failed', error_message=?, updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND status='uploading'`
	// Agent: 只让已经软删的行离开 uploading，避免把仍在上传的活行打回 local 再被扫表重传。
	dst[SQLLeaveRunUploading] = `UPDATE files SET status='local', updated_at=` + now + ` WHERE id=? AND status='uploading' AND deleted_at IS NOT NULL`
	dst[SQLLeaveArtUploading] = `UPDATE artifact_files SET status='local', updated_at=` + now + ` WHERE id=? AND status='uploading' AND deleted_at IS NOT NULL`
	dst[SQLSetRunLocalDeleted] = `UPDATE files SET local_deleted_at=` + now + ` WHERE id=? AND local_deleted_at IS NULL`
	dst[SQLSetArtLocalDeleted] = `UPDATE artifact_files SET local_deleted_at=` + now + ` WHERE id=? AND local_deleted_at IS NULL`
	dst[SQLSoftDeleteRunFiles] = `UPDATE files SET deleted_at=` + now + ` WHERE run_id=? AND deleted_at IS NULL`
	dst[SQLSoftDeleteArtFiles] = `UPDATE artifact_files SET deleted_at=` + now + ` WHERE run_id=? AND deleted_at IS NULL`
	dst[SQLSoftDeleteProjectFiles] = `UPDATE files SET deleted_at=` + now + ` WHERE deleted_at IS NULL AND run_id IN (SELECT id FROM runs WHERE project_id=?)`
	dst[SQLSoftDeleteProjectArtFiles] = `UPDATE artifact_files SET deleted_at=` + now + ` WHERE deleted_at IS NULL AND run_id IN (SELECT id FROM runs WHERE project_id=?)`
	dst[SQLListRunIDsToUpload] = `SELECT id, COALESCE(retry_count,0), COALESCE(updated_at, created_at, '') FROM files WHERE deleted_at IS NULL AND status IN ('local','failed') AND retry_count < 3`
	dst[SQLListArtIDsToUpload] = `SELECT id, COALESCE(retry_count,0), COALESCE(updated_at, created_at, '') FROM artifact_files WHERE deleted_at IS NULL AND status IN ('local','failed') AND retry_count < 3`
	dst[SQLListRunIDsToUnlink] = `SELECT id FROM files WHERE status='cloud' AND local_deleted_at IS NULL AND COALESCE(local_path,'') <> ''`
	dst[SQLListArtIDsToUnlink] = `SELECT id FROM artifact_files WHERE status='cloud' AND local_deleted_at IS NULL AND COALESCE(local_path,'') <> ''`
	dst[SQLCountFilesByRun] = `SELECT COUNT(*) FROM files WHERE run_id=?`
	dst[SQLCountArtFilesByRun] = `SELECT COUNT(*) FROM artifact_files WHERE run_id=?`
	dst[SQLHardDeleteRunFile] = `DELETE FROM files WHERE id=?`
	dst[SQLHardDeleteArtFile] = `DELETE FROM artifact_files WHERE id=?`
	dst[SQLListSoftRunFileIDs] = `SELECT id FROM files WHERE deleted_at IS NOT NULL`
	dst[SQLListSoftArtFileIDs] = `SELECT id FROM artifact_files WHERE deleted_at IS NOT NULL`
	dst[SQLListSoftRunFileIDsByRun] = `SELECT id FROM files WHERE run_id=? AND deleted_at IS NOT NULL`
	dst[SQLListSoftArtFileIDsByRun] = `SELECT id FROM artifact_files WHERE run_id=? AND deleted_at IS NOT NULL`
	dst[SQLCountLiveObjectKey] = `SELECT (SELECT COUNT(*) FROM files WHERE object_key=? AND deleted_at IS NULL) + (SELECT COUNT(*) FROM artifact_files WHERE object_key=? AND deleted_at IS NULL)`
	dst[SQLReapRunUploading] = `UPDATE files SET status=CASE WHEN retry_count>=3 THEN 'failed' ELSE 'local' END, updated_at=` + now + ` WHERE id=? AND status='uploading' AND updated_at <= ?`
	dst[SQLReapArtUploading] = `UPDATE artifact_files SET status=CASE WHEN retry_count>=3 THEN 'failed' ELSE 'local' END, updated_at=` + now + ` WHERE id=? AND status='uploading' AND updated_at <= ?`
	dst[SQLListStaleRunUploading] = `SELECT id FROM files WHERE status='uploading' AND updated_at <= ?`
	dst[SQLListStaleArtUploading] = `SELECT id FROM artifact_files WHERE status='uploading' AND updated_at <= ?`
	dst[SQLListRetryDeleteRun] = `SELECT id FROM files WHERE deleted_at IS NOT NULL AND deleted_at <= ? AND deleted_at > ?`
	dst[SQLListRetryDeleteArt] = `SELECT id FROM artifact_files WHERE deleted_at IS NOT NULL AND deleted_at <= ? AND deleted_at > ?`
	dst[SQLCountPermanentFails] = `SELECT COUNT(*) FROM files WHERE status='failed' AND retry_count>=3 AND updated_at>=? AND updated_at<?`
	dst[SQLCountArtPermanentFails] = `SELECT COUNT(*) FROM artifact_files WHERE status='failed' AND retry_count>=3 AND updated_at>=? AND updated_at<?`
	dst[SQLCountPrefixedFilesByRun] = `SELECT (SELECT COUNT(*) FROM files WHERE run_id=? AND id LIKE 'run-%') + (SELECT COUNT(*) FROM artifact_files WHERE run_id=?)`
	dst[SQLListRunIDsByProject] = `SELECT id FROM runs WHERE project_id=?`
	dst[SQLStopRunUpload] = `UPDATE files SET status='failed', updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND retry_count>=3 AND status IN ('local','failed')`
	dst[SQLStopArtUpload] = `UPDATE artifact_files SET status='failed', updated_at=` + now + ` WHERE id=? AND deleted_at IS NULL AND retry_count>=3 AND status IN ('local','failed')`
	// Agent: 先查出未软删的 id，再按 id 更新，避免更新窗口里新插入的行被标软删却没有入队。
	dst[SQLListLiveRunFileIDs] = `SELECT id FROM files WHERE run_id=? AND deleted_at IS NULL`
	dst[SQLListLiveArtFileIDs] = `SELECT id FROM artifact_files WHERE run_id=? AND deleted_at IS NULL`
	dst[SQLListLiveProjectRunFileIDs] = `SELECT id FROM files WHERE deleted_at IS NULL AND run_id IN (SELECT id FROM runs WHERE project_id=?)`
	dst[SQLListLiveProjectArtFileIDs] = `SELECT id FROM artifact_files WHERE deleted_at IS NULL AND run_id IN (SELECT id FROM runs WHERE project_id=?)`
	dst[SQLSoftDeleteFileByID] = `UPDATE files SET deleted_at=` + now + ` WHERE id=? AND deleted_at IS NULL`
	dst[SQLSoftDeleteArtFileByID] = `UPDATE artifact_files SET deleted_at=` + now + ` WHERE id=? AND deleted_at IS NULL`
	// Agent: 两张表的行都还在时，run 不能硬删。这里统计全部行，不只统计带前缀的行。
	dst[SQLCountAllFilesByRun] = `SELECT (SELECT COUNT(*) FROM files WHERE run_id=?) + (SELECT COUNT(*) FROM artifact_files WHERE run_id=?)`
	// Agent: 查询只返回未软删的行，并带上 object_key 供现签。
	dst[SQLListVisibleRunFiles] = `SELECT id, name, COALESCE(object_key,'') FROM files WHERE run_id=? AND deleted_at IS NULL ORDER BY name`
	dst[SQLListVisibleArtFiles] = `SELECT id, name, COALESCE(object_key,'') FROM artifact_files WHERE artifact_id=? AND deleted_at IS NULL ORDER BY name`
	// Agent: SQLite 不执行这句。MySQL 用会话锁让 Q39 一轮只有一个副本跑。
	if mysql {
		dst[SQLJobLock] = `SELECT GET_LOCK(?, 0)`
	} else {
		dst[SQLJobLock] = `SELECT 1`
	}
}
