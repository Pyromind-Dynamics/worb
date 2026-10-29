package store

import (
	"sort"
	"sync"
	"time"
)

type StorageUsage struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ProjectID   string `json:"projectId,omitempty"`
	ProjectName string `json:"projectName,omitempty"`
	Bytes       int64  `json:"bytes"`
}

type StorageUsageSnapshot struct {
	TopProjects []StorageUsage `json:"topProjects"`
	TopRuns     []StorageUsage `json:"topRuns"`
}

type storageUsageCache struct {
	mu       sync.Mutex
	snapshot StorageUsageSnapshot
	refresh  chan struct{}
}

const storageUsageRefreshEvery = time.Hour

func newStorageUsageCache() *storageUsageCache {
	return &storageUsageCache{}
}

func (db *DB) CachedStorageUsageSnapshot() StorageUsageSnapshot {
	if db.storageCache == nil {
		return StorageUsageSnapshot{}
	}
	db.storageCache.mu.Lock()
	defer db.storageCache.mu.Unlock()
	return cloneStorageUsageSnapshot(db.storageCache.snapshot)
}

func (db *DB) FreshStorageUsageSnapshot() StorageUsageSnapshot {
	if db.storageCache == nil {
		return db.computeStorageUsageSnapshot()
	}

	db.storageCache.mu.Lock()
	if ch := db.storageCache.refresh; ch != nil {
		db.storageCache.mu.Unlock()
		<-ch
		return db.CachedStorageUsageSnapshot()
	}
	ch := make(chan struct{})
	db.storageCache.refresh = ch
	db.storageCache.mu.Unlock()

	snapshot := db.computeStorageUsageSnapshot()

	db.storageCache.mu.Lock()
	db.storageCache.snapshot = cloneStorageUsageSnapshot(snapshot)
	close(ch)
	db.storageCache.refresh = nil
	cached := cloneStorageUsageSnapshot(db.storageCache.snapshot)
	db.storageCache.mu.Unlock()

	return cached
}

func (db *DB) StartStorageUsageRefresh() {
	if db.storageCache == nil {
		return
	}
	go func() {
		db.FreshStorageUsageSnapshot()

		ticker := time.NewTicker(storageUsageRefreshEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				db.FreshStorageUsageSnapshot()
			case <-db.ctx.Done():
				return
			}
		}
	}()
}

func (db *DB) computeStorageUsageSnapshot() StorageUsageSnapshot {
	topProjects, topRuns := db.topStorageUsage()
	return StorageUsageSnapshot{
		TopProjects: topProjects,
		TopRuns:     topRuns,
	}
}

func (db *DB) topStorageUsage() ([]StorageUsage, []StorageUsage) {
	rows, err := db.Query(db.SQL(SQLStorageUsage))
	if err != nil {
		return nil, nil
	}
	defer rows.Close()
	snapshot := storageUsageSnapshotFromRows(rows)
	return snapshot.TopProjects, snapshot.TopRuns
}

func storageUsageSnapshotFromRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) StorageUsageSnapshot {
	var runs []StorageUsage
	projectsByID := map[string]StorageUsage{}
	for rows.Next() {
		var item StorageUsage
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.ProjectName, &item.Name, &item.Bytes); err != nil {
			return StorageUsageSnapshot{}
		}
		runs = append(runs, item)
		project := projectsByID[item.ProjectID]
		project.ID = item.ProjectID
		project.Name = item.ProjectName
		project.Bytes += item.Bytes
		projectsByID[item.ProjectID] = project
	}
	if err := rows.Err(); err != nil {
		return StorageUsageSnapshot{}
	}

	projects := make([]StorageUsage, 0, len(projectsByID))
	for _, project := range projectsByID {
		projects = append(projects, project)
	}

	sort.Slice(runs, func(i, j int) bool {
		if runs[i].Bytes == runs[j].Bytes {
			if runs[i].ProjectName == runs[j].ProjectName {
				return runs[i].Name < runs[j].Name
			}
			return runs[i].ProjectName < runs[j].ProjectName
		}
		return runs[i].Bytes > runs[j].Bytes
	})
	sort.Slice(projects, func(i, j int) bool {
		if projects[i].Bytes == projects[j].Bytes {
			return projects[i].Name < projects[j].Name
		}
		return projects[i].Bytes > projects[j].Bytes
	})

	if len(runs) > 3 {
		runs = runs[:3]
	}
	if len(projects) > 3 {
		projects = projects[:3]
	}
	return StorageUsageSnapshot{
		TopProjects: projects,
		TopRuns:     runs,
	}
}

func cloneStorageUsageSnapshot(snapshot StorageUsageSnapshot) StorageUsageSnapshot {
	cloned := StorageUsageSnapshot{}
	if len(snapshot.TopProjects) > 0 {
		cloned.TopProjects = append([]StorageUsage(nil), snapshot.TopProjects...)
	}
	if len(snapshot.TopRuns) > 0 {
		cloned.TopRuns = append([]StorageUsage(nil), snapshot.TopRuns...)
	}
	return cloned
}
