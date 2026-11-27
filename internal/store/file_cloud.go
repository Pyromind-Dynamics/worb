package store

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// FileKind 区分 run 文件和 artifact 文件，用来选表。
// FileKind selects the files table or the artifact_files table.
type FileKind int

const (
	// FileKindUnknown 表示 id 没有 run- 或 art- 前缀。
	// FileKindUnknown means the id has neither the run- nor the art- prefix.
	FileKindUnknown FileKind = iota
	// FileKindRun 对应 files 表。
	// FileKindRun is a row in files.
	FileKindRun
	// FileKindArtifact 对应 artifact_files 表。
	// FileKindArtifact is a row in artifact_files.
	FileKindArtifact
)

// StoredFile 是上云和删除共用的文件行。
// StoredFile is one file row used by upload and delete.
type StoredFile struct {
	ID             string
	RunID          string
	Name           string
	Entity         string
	EntityKey      string
	LocalPath      string
	Status         string
	StorageType    string
	ObjectKey      string
	RetryCount     int
	DeletedAt      string
	LocalDeletedAt string
	Kind           FileKind
}

// FileKindOf 按 id 前缀选表。其它前缀不查库。
// FileKindOf chooses the table from the id prefix and does not query the database.
func FileKindOf(id string) FileKind {
	// Agent: run- 与 art- 是唯一选表依据。
	switch {
	case strings.HasPrefix(id, "run-"):
		return FileKindRun
	case strings.HasPrefix(id, "art-"):
		return FileKindArtifact
	default:
		return FileKindUnknown
	}
}

// EntityKey 把 entity 原文编成无填充的 Base64URL，用作对象键第一段。
// EntityKey encodes the raw entity as unpadded Base64URL for the object key prefix.
func EntityKey(entity string) string {
	// Agent: RawURLEncoding 不输出 '='。
	return base64.RawURLEncoding.EncodeToString([]byte(entity))
}

// CleanObjectName 拒绝路径穿越，并把反斜杠换成斜杠。
// CleanObjectName rejects path traversal and normalizes slashes.
func CleanObjectName(name string) (string, error) {
	// Agent: 对象键后半段只接受相对路径。
	name = strings.ReplaceAll(name, "\\", "/")
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "..") {
		return "", errBadFileName
	}
	return name, nil
}

var errBadFileName = errString("invalid file name")

type errString string

func (e errString) Error() string { return string(e) }

// ObjectKey 拼出云端对象键。kind 决定 stand-runs 或 stand-artifacts。
// ObjectKey builds the cloud object key. kind picks stand-runs or stand-artifacts.
func ObjectKey(kind FileKind, entityKey, runID, name string) string {
	// Agent: 类型目录紧跟 entity_key，避免和 run_id 混排。
	segment := "stand-runs"
	if kind == FileKindArtifact {
		segment = "stand-artifacts"
	}
	return entityKey + "/" + segment + "/" + runID + "/" + name
}

// InsertPendingRunFile 登记一个待上传的 run 文件，状态为 pending。
// InsertPendingRunFile records a run file that is waiting for PUT.
func (db *DB) InsertPendingRunFile(id, runID, name, url, entity string) error {
	// Agent: entity_key 与原文一起在 Create 时写好。
	_, err := db.Exec(db.SQL(SQLInsertPendingFile), id, runID, name, url, entity, EntityKey(entity))
	return err
}

// InsertPendingArtifactFile 登记一个 artifact 文件。artifact 必须已存在。
// InsertPendingArtifactFile records an artifact file. The artifact must already exist.
func (db *DB) InsertPendingArtifactFile(id, runID, artifactID, name, url, entity string) error {
	_, err := db.Exec(db.SQL(SQLInsertPendingArtifactFile), id, runID, artifactID, name, url, entity, EntityKey(entity))
	return err
}

// MarkFileLocal 在本地写盘成功后把行改成 local。影响 0 行表示已软删或不在可写状态。
// MarkFileLocal marks a row local after the bytes are on disk. Zero rows means the row cannot be updated.
func (db *DB) MarkFileLocal(id, localPath string, size int64) (bool, error) {
	kind := FileKindOf(id)
	var q string
	switch kind {
	case FileKindRun:
		q = db.SQL(SQLMarkRunFileLocal)
	case FileKindArtifact:
		q = db.SQL(SQLMarkArtFileLocal)
	default:
		return false, errBadFileName
	}
	res, err := db.Exec(q, localPath, size, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// GetStoredFile 按前缀只查一张表。没有前缀时只查 files。
// GetStoredFile loads one row from the table selected by the id prefix. An id without a prefix is read from files.
func (db *DB) GetStoredFile(id string) (*StoredFile, error) {
	kind := FileKindOf(id)
	var q string
	switch kind {
	case FileKindArtifact:
		q = db.SQL(SQLSelectArtFile)
	default:
		// Agent: 无前缀的旧行只在 files。这里只查这一张表，不去查 artifact_files。
		kind = FileKindRun
		q = db.SQL(SQLSelectRunFile)
	}
	f := &StoredFile{Kind: kind}
	// Agent: MySQL DATETIME 在 parseTime 下是 time.Time，SQLite 是文本，两边都收成同一字符串。
	var deleted, localDel dbTime
	err := db.QueryRow(q, id).Scan(&f.ID, &f.RunID, &f.Name, &f.Entity, &f.EntityKey, &f.LocalPath, &f.Status, &f.StorageType, &f.ObjectKey, &f.RetryCount, &deleted, &localDel)
	if err != nil {
		return nil, err
	}
	f.DeletedAt = deleted.s
	f.LocalDeletedAt = localDel.s
	return f, nil
}

// ClaimUpload 把 local/failed 抢成 uploading，并给 retry_count 加一。
// ClaimUpload moves local/failed to uploading and increments retry_count.
func (db *DB) ClaimUpload(id string) (bool, error) {
	kind := FileKindOf(id)
	var q string
	switch kind {
	case FileKindRun:
		q = db.SQL(SQLClaimRunUpload)
	case FileKindArtifact:
		q = db.SQL(SQLClaimArtUpload)
	default:
		return false, nil
	}
	res, err := db.Exec(q, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// FinishCloud 在行仍未软删时写入云端字段。返回 false 表示期间已被软删。
// FinishCloud writes cloud fields when the row is not soft-deleted. False means it was deleted during the put.
func (db *DB) FinishCloud(id, storageType, objectKey, etag string) (bool, error) {
	var q string
	switch FileKindOf(id) {
	case FileKindRun:
		q = db.SQL(SQLFinishRunCloud)
	case FileKindArtifact:
		q = db.SQL(SQLFinishArtCloud)
	default:
		return false, nil
	}
	res, err := db.Exec(q, storageType, objectKey, etag, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// FailUpload 把仍在上传且未软删的行标成 failed。
// FailUpload marks an in-flight row failed when it is not soft-deleted.
func (db *DB) FailUpload(id, message string) error {
	var q string
	switch FileKindOf(id) {
	case FileKindRun:
		q = db.SQL(SQLFailRunUpload)
	case FileKindArtifact:
		q = db.SQL(SQLFailArtUpload)
	default:
		return nil
	}
	if len(message) > 1000 {
		message = message[:1000]
	}
	_, err := db.Exec(q, message, id)
	return err
}

// LeaveUploading 在软删赢得竞争后离开 uploading，不再进入上云队列。
// LeaveUploading exits uploading after a soft-delete wins, without queueing another upload.
func (db *DB) LeaveUploading(id string) error {
	var q string
	switch FileKindOf(id) {
	case FileKindRun:
		q = db.SQL(SQLLeaveRunUploading)
	case FileKindArtifact:
		q = db.SQL(SQLLeaveArtUploading)
	default:
		return nil
	}
	_, err := db.Exec(q, id)
	return err
}

// MarkLocalDeleted 记录本地文件已经删除。
// MarkLocalDeleted records that the local bytes are gone.
func (db *DB) MarkLocalDeleted(id string) error {
	var q string
	switch FileKindOf(id) {
	case FileKindArtifact:
		q = db.SQL(SQLSetArtLocalDeleted)
	default:
		// Agent: 无前缀旧行的本地删除标记写在 files。
		q = db.SQL(SQLSetRunLocalDeleted)
	}
	_, err := db.Exec(q, id)
	return err
}

// HardDeleteStoredFile 在云端和本地都处理完之后删掉行。
// HardDeleteStoredFile removes the row after cloud and local cleanup succeed.
func (db *DB) HardDeleteStoredFile(id string) error {
	var q string
	switch FileKindOf(id) {
	case FileKindArtifact:
		q = db.SQL(SQLHardDeleteArtFile)
	default:
		// Agent: 无前缀旧行从 files 硬删。purge 不再直接删这张表。
		q = db.SQL(SQLHardDeleteRunFile)
	}
	_, err := db.Exec(q, id)
	return err
}

// LiveObjectKeyCount 统计同 key 仍未软删的行。files 与 artifact_files 的键空间不同，仍分开计数后相加。
// LiveObjectKeyCount counts rows with this key that are not soft-deleted.
func (db *DB) LiveObjectKeyCount(key string) (int, error) {
	var n int
	err := db.QueryRow(db.SQL(SQLCountLiveObjectKey), key, key).Scan(&n)
	return n, err
}

// SoftDeleteFilesForRun 把一个 run 下两张文件表都标成软删，并返回这些 id。
// SoftDeleteFilesForRun soft-deletes both file tables for a run and returns the ids.
func (db *DB) SoftDeleteFilesForRun(runID string) ([]string, error) {
	if _, err := db.Exec(db.SQL(SQLSoftDeleteRunFiles), runID); err != nil {
		return nil, err
	}
	if _, err := db.Exec(db.SQL(SQLSoftDeleteArtFiles), runID); err != nil {
		return nil, err
	}
	return db.softFileIDs(SQLListSoftRunFileIDsByRun, SQLListSoftArtFileIDsByRun, runID)
}

// SoftDeleteFilesForProject 把项目下所有 run 的文件行标成软删。
// SoftDeleteFilesForProject soft-deletes file rows for every run in a project.
func (db *DB) SoftDeleteFilesForProject(projectID string) ([]string, error) {
	if _, err := db.Exec(db.SQL(SQLSoftDeleteProjectFiles), projectID); err != nil {
		return nil, err
	}
	if _, err := db.Exec(db.SQL(SQLSoftDeleteProjectArtFiles), projectID); err != nil {
		return nil, err
	}
	runIDs, err := db.runIDsByProject(projectID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, runID := range runIDs {
		part, err := db.softFileIDs(SQLListSoftRunFileIDsByRun, SQLListSoftArtFileIDsByRun, runID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, part...)
	}
	return ids, nil
}

func (db *DB) runIDsByProject(projectID string) ([]string, error) {
	// Agent: 项目删除时文件行按 run 收集，语句仍走已有 run 查询的过滤条件之外的软删行。
	rows, err := db.Query(db.SQL(SQLListRunIDsByProject), projectID)
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

func (db *DB) softFileIDs(runSQL, artSQL SQLID, runID string) ([]string, error) {
	var ids []string
	for _, id := range []SQLID{runSQL, artSQL} {
		rows, err := db.Query(db.SQL(id), runID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var fileID string
			if err := rows.Scan(&fileID); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, fileID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return ids, nil
}

// SetFileDeleteNotify 装上删除队列回调。装上之前记下的 id 会立刻送出。
// SetFileDeleteNotify installs the delete-queue callback and flushes ids saved before it was set.
func (db *DB) SetFileDeleteNotify(fn func([]string)) {
	// Agent: 回调只在启动时安装一次。锁内只交换函数和积压 id，不在锁里调用队列。
	db.fileDeleteMu.Lock()
	db.fileDeleteFn = fn
	pending := db.fileDeletePending
	db.fileDeletePending = nil
	db.fileDeleteMu.Unlock()
	if fn != nil && len(pending) > 0 {
		fn(pending)
	}
}

// notifyFileDelete 把刚软删的 id 送进删除队列。回调还没装上时先留在内存里。
// notifyFileDelete sends newly soft-deleted ids to the delete queue, or keeps them until the callback exists.
func (db *DB) notifyFileDelete(ids []string) {
	if len(ids) == 0 {
		return
	}
	db.fileDeleteMu.Lock()
	fn := db.fileDeleteFn
	if fn == nil {
		// Agent: 启动清理可能早于队列安装，先积压，避免这些行再也进不了队列。
		db.fileDeletePending = append(db.fileDeletePending, ids...)
		db.fileDeleteMu.Unlock()
		return
	}
	db.fileDeleteMu.Unlock()
	fn(ids)
}

// softDeleteLiveRunFiles 把一个 run 下尚未软删的文件行标成软删，并返回真正更新到的 id。
// softDeleteLiveRunFiles soft-deletes file rows for one run that are still live and returns the ids that changed.
func (db *DB) softDeleteLiveRunFiles(runID string) ([]string, error) {
	runIDs, err := db.queryIDs(SQLListLiveRunFileIDs, runID)
	if err != nil {
		return nil, err
	}
	artIDs, err := db.queryIDs(SQLListLiveArtFileIDs, runID)
	if err != nil {
		return nil, err
	}
	changed, err := db.softDeleteIDs(runIDs, false)
	if err != nil {
		return nil, err
	}
	artChanged, err := db.softDeleteIDs(artIDs, true)
	if err != nil {
		return nil, err
	}
	return append(changed, artChanged...), nil
}

// softDeleteLiveProjectFiles 把项目下尚未软删的文件行标成软删，并返回真正更新到的 id。
// softDeleteLiveProjectFiles soft-deletes live file rows for a project and returns the ids that changed.
func (db *DB) softDeleteLiveProjectFiles(projectID string) ([]string, error) {
	runIDs, err := db.queryIDs(SQLListLiveProjectRunFileIDs, projectID)
	if err != nil {
		return nil, err
	}
	artIDs, err := db.queryIDs(SQLListLiveProjectArtFileIDs, projectID)
	if err != nil {
		return nil, err
	}
	changed, err := db.softDeleteIDs(runIDs, false)
	if err != nil {
		return nil, err
	}
	artChanged, err := db.softDeleteIDs(artIDs, true)
	if err != nil {
		return nil, err
	}
	return append(changed, artChanged...), nil
}

func (db *DB) queryIDs(id SQLID, arg string) ([]string, error) {
	rows, err := db.Query(db.SQL(id), arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var fileID string
		if err := rows.Scan(&fileID); err != nil {
			return nil, err
		}
		ids = append(ids, fileID)
	}
	return ids, rows.Err()
}

func (db *DB) softDeleteIDs(ids []string, artifact bool) ([]string, error) {
	q := db.SQL(SQLSoftDeleteFileByID)
	if artifact {
		q = db.SQL(SQLSoftDeleteArtFileByID)
	}
	var changed []string
	for _, id := range ids {
		// Agent: 按先前查到的 id 更新。影响 0 行说明已被别的调用软删，不再重复入队。
		res, err := db.Exec(q, id)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			changed = append(changed, id)
		}
	}
	return changed, nil
}

// SoftDeleteStoredFiles 按 id 软删仍可见的文件行，并把真正更新到的 id 送进删除队列。
// SoftDeleteStoredFiles soft-deletes live rows by id and sends the changed ids to the delete queue.
func (db *DB) SoftDeleteStoredFiles(ids []string) ([]string, error) {
	var runIDs, artIDs []string
	for _, id := range ids {
		// Agent: art- 前缀进 artifact_files，其余进 files。已经软删的行更新不到，不会再次入队。
		if FileKindOf(id) == FileKindArtifact {
			artIDs = append(artIDs, id)
			continue
		}
		runIDs = append(runIDs, id)
	}
	changed, err := db.softDeleteIDs(runIDs, false)
	if err != nil {
		return nil, err
	}
	artChanged, err := db.softDeleteIDs(artIDs, true)
	if err != nil {
		return nil, err
	}
	changed = append(changed, artChanged...)
	db.notifyFileDelete(changed)
	return changed, nil
}

// CountAllFileRows 返回这个 run 在两张文件表里还没被异步硬删的行数。
// CountAllFileRows counts file rows for a run that the async deleter has not removed yet.
func (db *DB) CountAllFileRows(runID string) (int, error) {
	var n int
	err := db.QueryRow(db.SQL(SQLCountAllFilesByRun), runID, runID).Scan(&n)
	return n, err
}

// VisibleFile 是查询已有文件时需要的字段。软删行不会出现在这里。
// VisibleFile is one existing file returned to a query. Soft-deleted rows are excluded.
type VisibleFile struct {
	ID        string
	Name      string
	ObjectKey string
}

// ListVisibleRunFiles 列出一个 run 尚未软删的 run 文件。
// ListVisibleRunFiles lists run files that are not soft-deleted.
func (db *DB) ListVisibleRunFiles(runID string) ([]VisibleFile, error) {
	return db.listVisibleFiles(SQLListVisibleRunFiles, runID)
}

// VisibleFilePage 是按入参取一页未软删 run 文件的条件。
// VisibleFilePage is the filter for one page of run files that are not soft-deleted.
type VisibleFilePage struct {
	Names   []string
	Pattern string
	AfterID string
	Limit   int
}

// CountVisibleRunFiles 统计一个 run 尚未软删的文件数。
// CountVisibleRunFiles counts run files that are not soft-deleted.
func (db *DB) CountVisibleRunFiles(runID string) (int, error) {
	// Agent: COUNT 走 run_id 索引，不把 name 和 object_key 读进进程。
	var n int
	err := db.QueryRow(db.SQL(SQLCountVisibleRunFiles), runID).Scan(&n)
	return n, err
}

// ListVisibleRunFilePage 按文件名、LIKE 和游标直接取一页。
// ListVisibleRunFilePage loads one page by name, LIKE pattern, and cursor.
func (db *DB) ListVisibleRunFilePage(runID string, page VisibleFilePage) ([]VisibleFile, bool, error) {
	limit := page.Limit
	// Agent: 缺省与 GraphQL 的 first=50 一致。单次最多 200 行，避免一次请求读完整张表。
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var extra strings.Builder
	args := make([]any, 0, 4+len(page.Names))
	args = append(args, runID)
	// Agent: 有文件名时只按 name 排序。带上 id 会让 SQLite 改走 run_id 索引，把这个 run 扫一遍。
	order := " ORDER BY id"
	if len(page.Names) > 0 {
		order = " ORDER BY name"
		extra.WriteString(" AND name IN (")
		for i, name := range page.Names {
			if i > 0 {
				extra.WriteByte(',')
			}
			extra.WriteByte('?')
			args = append(args, name)
		}
		extra.WriteByte(')')
		if page.AfterID != "" {
			var cursorName string
			err := db.QueryRow(db.SQL(SQLSelectFileNameByID), page.AfterID, runID).Scan(&cursorName)
			if err == sql.ErrNoRows {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
			// Agent: 名字索引上的下一页。游标 id 只在同名文件里区分先后。
			extra.WriteString(" AND (name > ? OR (name = ? AND id > ?))")
			args = append(args, cursorName, cursorName, page.AfterID)
		}
	} else if page.AfterID != "" {
		// Agent: 没有文件名时按 (run_id, id) 做范围，读完一页就停。
		extra.WriteString(" AND id > ?")
		args = append(args, page.AfterID)
	}
	if page.Pattern != "" {
		// Agent: pattern 是客户端自己的 SQL LIKE，只绑定参数，不拼进 SQL 文本。
		extra.WriteString(" AND name LIKE ?")
		args = append(args, page.Pattern)
	}
	args = append(args, limit+1)
	query := strings.Replace(db.SQL(SQLListVisibleRunFilePage), "{extra}", extra.String(), 1)
	query = strings.Replace(query, "{order}", order, 1)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	files := make([]VisibleFile, 0, limit)
	for rows.Next() {
		var f VisibleFile
		if err := rows.Scan(&f.ID, &f.Name, &f.ObjectKey); err != nil {
			return nil, false, err
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	// Agent: 多取一行只用来判断还有没有下一页，这一行不返回给客户端。
	more := len(files) > limit
	if more {
		files = files[:limit]
	}
	return files, more, nil
}

// ListVisibleArtifactFiles 列出一个 artifact 尚未软删的文件。
// ListVisibleArtifactFiles lists artifact files that are not soft-deleted.
func (db *DB) ListVisibleArtifactFiles(artifactID string) ([]VisibleFile, error) {
	return db.listVisibleFiles(SQLListVisibleArtFiles, artifactID)
}

func (db *DB) listVisibleFiles(id SQLID, arg string) ([]VisibleFile, error) {
	rows, err := db.Query(db.SQL(id), arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []VisibleFile
	for rows.Next() {
		var f VisibleFile
		if err := rows.Scan(&f.ID, &f.Name, &f.ObjectKey); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// StopExhaustedUpload 在已达 3 次时直接标失败，不再开传。
// StopExhaustedUpload marks a row failed when it has already started three uploads.
func (db *DB) StopExhaustedUpload(id string) error {
	var q string
	switch FileKindOf(id) {
	case FileKindRun:
		q = db.SQL(SQLStopRunUpload)
	case FileKindArtifact:
		q = db.SQL(SQLStopArtUpload)
	default:
		return nil
	}
	_, err := db.Exec(q, id)
	return err
}

// DueUploadIDs 返回已到退避时间、且未软删的待上云 id。
// DueUploadIDs returns ids that are waiting to upload and have reached their backoff.
func (db *DB) DueUploadIDs(now time.Time) ([]string, error) {
	var ids []string
	for _, q := range []string{db.SQL(SQLListRunIDsToUpload), db.SQL(SQLListArtIDsToUpload)} {
		rows, err := db.Query(q)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var retry int
			var updated dbTime
			if err := rows.Scan(&id, &retry, &updated); err != nil {
				rows.Close()
				return nil, err
			}
			if uploadDue(retry, updated.s, now) {
				ids = append(ids, id)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return ids, nil
}

func uploadDue(retry int, updated string, now time.Time) bool {
	// Agent: retry_count 0 立刻可传；1 等 1 分钟；2 等 5 分钟。
	wait := time.Duration(0)
	switch retry {
	case 1:
		wait = time.Minute
	case 2:
		wait = 5 * time.Minute
	}
	if wait == 0 {
		return true
	}
	t, ok := parseDBTime(updated)
	if !ok {
		return true
	}
	return !now.Before(t.Add(wait))
}

func parseDBTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339, "2006-01-02T15:04:05Z"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// IDsNeedingUnlink 返回已上云但本地删除时间仍空的 id。
// IDsNeedingUnlink returns cloud rows whose local file is not yet recorded as removed.
func (db *DB) IDsNeedingUnlink() ([]string, error) {
	return db.collectIDs(db.SQL(SQLListRunIDsToUnlink), db.SQL(SQLListArtIDsToUnlink))
}

// StaleUploadingIDs 返回 updated_at 早于 cutoff 的 uploading 行。
// StaleUploadingIDs returns uploading rows whose updated_at is before cutoff.
func (db *DB) StaleUploadingIDs(cutoff time.Time) ([]string, error) {
	stamp := cutoff.UTC().Format("2006-01-02 15:04:05")
	var ids []string
	for _, q := range []string{db.SQL(SQLListStaleRunUploading), db.SQL(SQLListStaleArtUploading)} {
		rows, err := db.Query(q, stamp)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		rows.Close()
	}
	return ids, nil
}

// ReapUploading 按行把超时的 uploading 改回 local 或 failed。RowsAffected=0 表示别的副本已处理。
// ReapUploading resets one stale uploading row to local or failed. Zero rows means another replica took it.
func (db *DB) ReapUploading(id string, cutoff time.Time) (bool, error) {
	stamp := cutoff.UTC().Format("2006-01-02 15:04:05")
	var q string
	switch FileKindOf(id) {
	case FileKindRun:
		q = db.SQL(SQLReapRunUploading)
	case FileKindArtifact:
		q = db.SQL(SQLReapArtUploading)
	default:
		return false, nil
	}
	res, err := db.Exec(q, id, stamp)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// RetryDeleteIDs 返回软删时间落在 (older, newer] 的文件 id。
// RetryDeleteIDs returns soft-deleted ids whose deleted_at is inside (older, newer].
func (db *DB) RetryDeleteIDs(older, newer time.Time) ([]string, error) {
	a := older.UTC().Format("2006-01-02 15:04:05")
	b := newer.UTC().Format("2006-01-02 15:04:05")
	var ids []string
	for _, q := range []string{db.SQL(SQLListRetryDeleteRun), db.SQL(SQLListRetryDeleteArt)} {
		rows, err := db.Query(q, b, a)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		rows.Close()
	}
	return ids, nil
}

// PermanentFailCounts 按北京时间日历日统计两表永久失败条数。
// PermanentFailCounts counts permanent failures in both tables for each Shanghai day.
func (db *DB) PermanentFailCounts(days []time.Time) (map[string]int, error) {
	out := map[string]int{}
	for _, day := range days {
		start := day.UTC().Format("2006-01-02 15:04:05")
		end := day.Add(24 * time.Hour).UTC().Format("2006-01-02 15:04:05")
		var a, b int
		if err := db.QueryRow(db.SQL(SQLCountPermanentFails), start, end).Scan(&a); err != nil {
			return nil, err
		}
		if err := db.QueryRow(db.SQL(SQLCountArtPermanentFails), start, end).Scan(&b); err != nil {
			return nil, err
		}
		out[day.In(time.FixedZone("CST", 8*3600)).Format("2006-01-02")] = a + b
	}
	return out, nil
}

// TryJobLock 在 MySQL 上抢一把会话锁。SQLite 单进程直接返回 true。
// TryJobLock takes a session lock on MySQL. SQLite returns true.
func (db *DB) TryJobLock(name string) bool {
	if !db.isMySQL() {
		return true
	}
	var n sql.NullInt64
	// Agent: GET_LOCK 返回 1 才执行本轮定时任务。
	// Agent: 锁语句走编号，避免在业务里写裸 SQL。
	if err := db.QueryRow(db.SQL(SQLJobLock), name).Scan(&n); err != nil {
		return false
	}
	return n.Valid && n.Int64 == 1
}

// dbTime 把 SQLite 文本和 MySQL time.Time 都收成 UTC 文本。空值保持空串。
// dbTime stores SQLite text and MySQL time.Time as one UTC string. Empty stays empty.
type dbTime struct {
	s string
}

// Scan 接受 nil、文本、[]byte 和 time.Time。
// Scan accepts nil, text, []byte, and time.Time.
func (t *dbTime) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		t.s = ""
	case string:
		t.s = strings.TrimSpace(v)
	case []byte:
		t.s = strings.TrimSpace(string(v))
	case time.Time:
		if v.IsZero() {
			t.s = ""
			return nil
		}
		t.s = v.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Errorf("dbTime: %T", value)
	}
	return nil
}

func (db *DB) collectIDs(queries ...string) ([]string, error) {
	var ids []string
	for _, q := range queries {
		rows, err := db.Query(q)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return ids, nil
}
