package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDataDirMustBeAccessible 确认 dataDir 不是目录时不能启动。
// TestDataDirMustBeAccessible checks that startup fails when dataDir is not a directory.
func TestDataDirMustBeAccessible(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Agent: 把文件路径当成数据目录时必须失败。
	if _, err := New(file, "sqlite"); err == nil {
		t.Fatal("file path started")
	}
}
