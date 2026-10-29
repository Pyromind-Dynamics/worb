package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type Project struct {
	ID        string
	Entity    string
	Name      string
	CreatedAt time.Time
}

type Run struct {
	ID                   string
	ProjectID            string
	Name                 string
	DisplayName          string
	Config               json.RawMessage
	Summary              json.RawMessage
	State                string
	Host                 string
	Program              string
	GitCommit            string
	Tags                 json.RawMessage
	Notes                string
	GroupName            string
	JobType              string
	SweepName            string
	HistoryLineCount     int
	EventsLineCount      int
	LogLineCount         int
	ReceivedHistoryCount int
	ForkRunID            *string
	ForkStep             *int
	CreatedAt            time.Time
	UpdatedAt            time.Time
	HeartbeatAt          time.Time
	Project              *Project
}

func (db *DB) EnsureProject(entity, name string) (*Project, error) {
	if entity == "" {
		entity = "local"
	}

	var p Project
	var createdAt flexTime
	err := db.QueryRow(db.SQL(SQLSelectProjectByEntityName), entity, name).
		Scan(&p.ID, &p.Entity, &p.Name, &createdAt)
	p.CreatedAt = createdAt.Time
	if err == nil {
		return &p, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	p = Project{
		ID:     uuid.New().String(),
		Entity: entity,
		Name:   name,
	}
	_, err = db.Exec(db.SQL(SQLInsertProject), p.ID, p.Entity, p.Name)
	if err != nil {
		return nil, fmt.Errorf("insert project: %w", err)
	}
	return &p, nil
}

type UpsertRunParams struct {
	ID          string
	Entity      string
	Project     string
	Name        string
	DisplayName string
	Config      json.RawMessage
	Summary     json.RawMessage
	State       string
	Host        string
	Program     string
	GitCommit   string
	Tags        json.RawMessage
	Notes       string
	GroupName   string
	JobType     string
	SweepName   string
}

func (db *DB) UpsertRun(p UpsertRunParams) (*Run, error) {
	clientSentID := p.ID != ""
	if p.ID == "" {
		p.ID = uuid.New().String()
	}
	if p.Name == "" {
		if len(p.ID) >= 8 {
			p.Name = p.ID[:8]
		} else {
			p.Name = p.ID
		}
	}

	var existing string
	err := db.QueryRow(db.SQL(SQLSelectRunIDExists), p.ID).Scan(&existing)
	if err == sql.ErrNoRows && !clientSentID && p.Name != "" {
		proj, projErr := db.EnsureProject(p.Entity, p.Project)
		if projErr == nil {
			lookupErr := db.QueryRow(db.SQL(SQLSelectRunIDByName), proj.ID, p.Name).Scan(&existing)
			if lookupErr == nil {
				p.ID = existing
				err = nil
			}
		}
	}
	if err == sql.ErrNoRows {
		proj, projErr := db.EnsureProject(p.Entity, p.Project)
		if projErr != nil {
			return nil, fmt.Errorf("ensure project: %w", projErr)
		}
		if p.Config == nil {
			p.Config = json.RawMessage("{}")
		}
		if p.Summary == nil {
			p.Summary = json.RawMessage("{}")
		}
		if p.Tags == nil {
			p.Tags = json.RawMessage("[]")
		}
		if p.State == "" {
			p.State = "running"
		}
		_, err = db.Exec(db.SQL(SQLInsertRun),
			p.ID, proj.ID, p.Name, p.DisplayName, string(p.Config), string(p.Summary), p.State,
			p.Host, p.Program, p.GitCommit, string(p.Tags), p.Notes, p.GroupName, p.JobType, p.SweepName)
		if err != nil {
			return nil, fmt.Errorf("insert run: %w", err)
		}
	} else if err == nil {
		// Agent: SET 子句按本次有值的字段拼装，列名来自白名单。
		extra := []string{}
		if p.State != "" {
			extra = append(extra, "state = ?")
		}
		if p.Config != nil {
			extra = append(extra, "config = ?")
		}
		if p.Summary != nil {
			extra = append(extra, "summary = ?")
		}
		if p.DisplayName != "" {
			extra = append(extra, "display_name = ?")
		}
		if p.Tags != nil {
			extra = append(extra, "tags = ?")
		}
		if p.Notes != "" {
			extra = append(extra, "notes = ?")
		}
		if p.Host != "" {
			extra = append(extra, "host = ?")
		}
		if p.Program != "" {
			extra = append(extra, "program = ?")
		}
		query, qErr := db.SQLUpdateRun(extra)
		if qErr != nil {
			return nil, qErr
		}
		args := []any{}
		if p.State != "" {
			args = append(args, p.State)
		}
		if p.Config != nil {
			args = append(args, string(p.Config))
		}
		if p.Summary != nil {
			args = append(args, string(p.Summary))
		}
		if p.DisplayName != "" {
			args = append(args, p.DisplayName)
		}
		if p.Tags != nil {
			args = append(args, string(p.Tags))
		}
		if p.Notes != "" {
			args = append(args, p.Notes)
		}
		if p.Host != "" {
			args = append(args, p.Host)
		}
		if p.Program != "" {
			args = append(args, p.Program)
		}
		args = append(args, p.ID)
		_, err = db.DB.Exec(query, args...)
		if err != nil {
			return nil, fmt.Errorf("update run: %w", err)
		}
	} else {
		return nil, err
	}

	return db.GetRun(p.ID)
}

func (db *DB) FinishRun(id string) error {
	_, err := db.Exec(db.SQL(SQLFinishRun), id)
	return err
}

func (db *DB) IncrReceivedHistoryCount(id string, delta int) {
	_, err := db.Exec(db.SQL(SQLIncrReceivedHistory), delta, id)
	if err != nil {
		log.Printf("[store] IncrReceivedHistoryCount(%s, %d): %v", id, delta, err)
	}
}

func (db *DB) GetRun(id string) (*Run, error) {
	r := &Run{}
	var config, summary, tags sql.NullString
	var displayName, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
	var forkRunID sql.NullString
	var forkStep sql.NullInt64
	var createdAt, updatedAt, heartbeatAt flexTime
	err := db.QueryRow(db.SQLRunSelect("byID"), id).
		Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt)
	r.CreatedAt = createdAt.Time
	r.UpdatedAt = updatedAt.Time
	r.HeartbeatAt = heartbeatAt.Time
	if err != nil {
		return nil, err
	}
	r.DisplayName = displayName.String
	r.Host = host.String
	r.Program = program.String
	r.GitCommit = gitCommit.String
	r.Notes = notes.String
	r.GroupName = groupName.String
	r.JobType = jobType.String
	r.SweepName = sweepName.String
	if forkRunID.Valid {
		r.ForkRunID = &forkRunID.String
	}
	if forkStep.Valid {
		v := int(forkStep.Int64)
		r.ForkStep = &v
	}
	if config.Valid {
		r.Config = json.RawMessage(config.String)
	}
	if summary.Valid {
		r.Summary = json.RawMessage(summary.String)
	}
	if tags.Valid {
		r.Tags = json.RawMessage(tags.String)
	}
	return r, nil
}

func (db *DB) GetRunByName(projectID, name string) (*Run, error) {
	var id string
	err := db.QueryRow(db.SQL(SQLSelectRunIDByName), projectID, name).Scan(&id)
	if err == sql.ErrNoRows {
		// Fallback: try matching by display_name
		err = db.QueryRow(db.SQL(SQLSelectRunIDByDisplay), projectID, name).Scan(&id)
	}
	if err != nil {
		return nil, err
	}
	return db.GetRun(id)
}

func (db *DB) ResolveRunRef(projectID, ref string) (*Run, error) {
	run, err := db.GetRun(ref)
	if err == nil {
		if run.ProjectID != projectID {
			return nil, sql.ErrNoRows
		}
		return run, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	var nameCount int
	err = db.QueryRow(db.SQL(SQLCountRunsByName), projectID, ref).Scan(&nameCount)
	if err != nil {
		return nil, err
	}
	if nameCount == 1 {
		return db.GetRunByName(projectID, ref)
	}
	if nameCount > 1 {
		return nil, fmt.Errorf("ambiguous run reference %q: matched %d runs by name", ref, nameCount)
	}

	var displayNameCount int
	err = db.QueryRow(db.SQL(SQLCountRunsByDisplay), projectID, ref).Scan(&displayNameCount)
	if err != nil {
		return nil, err
	}
	if displayNameCount == 1 {
		var id string
		err = db.QueryRow(db.SQL(SQLSelectRunIDByDisplay), projectID, ref).Scan(&id)
		if err != nil {
			return nil, err
		}
		return db.GetRun(id)
	}
	if displayNameCount > 1 {
		return nil, fmt.Errorf("ambiguous run reference %q: matched %d runs by display_name", ref, displayNameCount)
	}

	return nil, sql.ErrNoRows
}

func (db *DB) ListRuns(projectID string) ([]*Run, error) {
	rows, err := db.Query(db.SQLRunSelect("byProject"), projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var config, summary, tags sql.NullString
		var displayName, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
		var forkRunID sql.NullString
		var forkStep sql.NullInt64
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = displayName.String
		r.Host = host.String
		r.Program = program.String
		r.GitCommit = gitCommit.String
		r.Notes = notes.String
		r.GroupName = groupName.String
		r.JobType = jobType.String
		r.SweepName = sweepName.String
		if forkRunID.Valid {
			r.ForkRunID = &forkRunID.String
		}
		if forkStep.Valid {
			v := int(forkStep.Int64)
			r.ForkStep = &v
		}
		if config.Valid {
			r.Config = json.RawMessage(config.String)
		}
		if summary.Valid {
			r.Summary = json.RawMessage(summary.String)
		}
		if tags.Valid {
			r.Tags = json.RawMessage(tags.String)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (db *DB) ListRunsLite(projectID string) ([]*Run, error) {
	rows, err := db.Query(db.SQL(SQLListRunsLite), projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var displayName sql.NullString
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &r.State,
			&r.HistoryLineCount, &createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = displayName.String
		runs = append(runs, r)
	}
	return runs, nil
}

func (db *DB) DeleteRun(runID string) error {
	res, err := db.Exec(db.SQL(SQLSoftDeleteRun), runID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("run not found: %s", runID)
	}
	return nil
}

func (db *DB) DeleteProject(projectID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(db.SQL(SQLSoftDeleteRunsByProject), projectID); err != nil {
		return fmt.Errorf("soft-delete runs: %w", err)
	}
	res, err := tx.Exec(db.SQL(SQLSoftDeleteProject), projectID)
	if err != nil {
		return fmt.Errorf("soft-delete project: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("project not found: %s", projectID)
	}
	return tx.Commit()
}

func (db *DB) ListRunsFiltered(projectID string, displayName string) ([]*Run, error) {
	query := db.SQLRunSelect("filtered")
	args := []any{projectID}
	if displayName != "" {
		query += ` AND r.display_name = ?`
		args = append(args, displayName)
	}
	query += ` ORDER BY r.created_at DESC`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var config, summary, tags sql.NullString
		var dn, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
		var forkRunID sql.NullString
		var forkStep sql.NullInt64
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &dn, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = dn.String
		r.Host = host.String
		r.Program = program.String
		r.GitCommit = gitCommit.String
		r.Notes = notes.String
		r.GroupName = groupName.String
		r.JobType = jobType.String
		r.SweepName = sweepName.String
		if forkRunID.Valid {
			r.ForkRunID = &forkRunID.String
		}
		if forkStep.Valid {
			v := int(forkStep.Int64)
			r.ForkStep = &v
		}
		if config.Valid {
			r.Config = json.RawMessage(config.String)
		}
		if summary.Valid {
			r.Summary = json.RawMessage(summary.String)
		}
		if tags.Valid {
			r.Tags = json.RawMessage(tags.String)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

type ForkRunParams struct {
	ParentRunID     string
	ForkStep        int
	NewRunID        string          // if empty, generates a new UUID
	DisplayName     string          // optional
	ConfigOverrides json.RawMessage // merged on top of parent config
}

func (db *DB) ForkRun(p ForkRunParams) (*Run, error) {
	parent, err := db.GetRun(p.ParentRunID)
	if err != nil {
		return nil, fmt.Errorf("parent run not found: %s", p.ParentRunID)
	}

	if p.ForkStep < 0 {
		return nil, fmt.Errorf("invalid fork_step: must be >= 0")
	}

	newID := p.NewRunID
	if newID == "" {
		newID = uuid.New().String()
	}
	name := newID
	if len(name) >= 8 {
		name = name[:8]
	}

	// Merge config: parent base + overrides
	mergedConfig := parent.Config
	if p.ConfigOverrides != nil {
		mergedConfig = mergeConfigJSON(parent.Config, p.ConfigOverrides)
	}

	displayName := p.DisplayName
	if displayName == "" && parent.DisplayName != "" {
		displayName = fmt.Sprintf("fork of %s @%d", parent.DisplayName, p.ForkStep)
	}

	configStr := "{}"
	if mergedConfig != nil {
		configStr = string(mergedConfig)
	}
	tagsStr := "[]"
	if parent.Tags != nil {
		tagsStr = string(parent.Tags)
	}

	if p.NewRunID != "" {
		// Run already exists (created by UpsertRun) — update it with fork metadata
		_, err = db.Exec(db.SQL(SQLUpdateForkedRun),
			configStr, tagsStr, parent.Notes, parent.Host, parent.Program,
			parent.GitCommit, parent.GroupName, parent.JobType, parent.SweepName,
			p.ParentRunID, p.ForkStep, newID)
		if err != nil {
			return nil, fmt.Errorf("update forked run: %w", err)
		}
	} else {
		// Create a brand new run record
		_, err = db.Exec(db.SQL(SQLInsertForkedRun),
			newID, parent.ProjectID, name, displayName, configStr,
			parent.Host, parent.Program, parent.GitCommit, tagsStr, parent.Notes,
			parent.GroupName, parent.JobType, parent.SweepName, p.ParentRunID, p.ForkStep)
		if err != nil {
			return nil, fmt.Errorf("insert forked run: %w", err)
		}
	}

	_, err = db.Exec(db.SQL(SQLSetHistoryLineCount), p.ForkStep+1, newID)
	if err != nil {
		return nil, fmt.Errorf("set history_line_count: %w", err)
	}

	err = db.wal.Append(walEntry{
		RunID:    newID,
		ForkFrom: &walForkOp{ParentRunID: p.ParentRunID, MaxStep: p.ForkStep},
	})
	if err != nil {
		return nil, fmt.Errorf("enqueue fork WAL entry: %w", err)
	}

	return db.GetRun(newID)
}

// mergeConfigJSON overlays overrides on top of base config JSON.
// Both are expected to be JSON objects. Keys in overrides replace keys in base.
func mergeConfigJSON(base, overrides json.RawMessage) json.RawMessage {
	var baseMap map[string]json.RawMessage
	var overMap map[string]json.RawMessage

	if json.Unmarshal(base, &baseMap) != nil {
		baseMap = map[string]json.RawMessage{}
	}
	if json.Unmarshal(overrides, &overMap) != nil {
		return base
	}

	for k, v := range overMap {
		baseMap[k] = v
	}

	merged, err := json.Marshal(baseMap)
	if err != nil {
		return base
	}
	return merged
}

func (db *DB) ListAllProjects() ([]*Project, error) {
	return db.ListProjects("")
}

func (db *DB) ListProjects(entity string) ([]*Project, error) {
	var rows *sql.Rows
	var err error
	if entity == "" {
		rows, err = db.Query(db.SQL(SQLListAllProjects))
	} else {
		rows, err = db.Query(db.SQL(SQLListProjectsByEntity), entity)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*Project
	for rows.Next() {
		p := &Project{}
		var ca flexTime
		if err := rows.Scan(&p.ID, &p.Entity, &p.Name, &ca); err != nil {
			return nil, err
		}
		p.CreatedAt = ca.Time
		projects = append(projects, p)
	}
	return projects, nil
}

func (db *DB) GetProject(id string) (*Project, error) {
	p := &Project{}
	var ca flexTime
	err := db.QueryRow(db.SQL(SQLSelectProjectByID), id).
		Scan(&p.ID, &p.Entity, &p.Name, &ca)
	if err != nil {
		return nil, err
	}
	p.CreatedAt = ca.Time
	return p, nil
}

type File struct {
	ID        string    `json:"id"`
	RunID     string    `json:"run_id"`
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

func (db *DB) InsertFile(id, runID, name, url string) error {
	_, err := db.Exec(db.SQL(SQLInsertFileIgnore), id, runID, name, url)
	return err
}

func (db *DB) UpdateFileSize(fileID string, size int64) {
	db.Exec(db.SQL(SQLUpdateFileSize), size, fileID)
}

func (db *DB) ListFiles(runID string) ([]*File, error) {
	rows, err := db.Query(db.SQL(SQLListFilesByRun), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*File
	for rows.Next() {
		f := &File{}
		var ca flexTime
		if err := rows.Scan(&f.ID, &f.RunID, &f.Name, &f.URL, &f.Size, &ca); err != nil {
			return nil, err
		}
		f.CreatedAt = ca.Time
		files = append(files, f)
	}
	return files, nil
}

func (db *DB) GetProjectByName(entity, name string) (*Project, error) {
	p := &Project{}
	var ca flexTime
	err := db.QueryRow(db.SQL(SQLSelectProjectByEntityName), entity, name).
		Scan(&p.ID, &p.Entity, &p.Name, &ca)
	if err != nil {
		return nil, err
	}
	p.CreatedAt = ca.Time
	return p, nil
}
