package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWALFilePrefixUsesEnvAndOrdinal 确认前缀是 APP_ENV 加 StatefulSet 序号。
// TestWALFilePrefixUsesEnvAndOrdinal checks the prefix is APP_ENV plus the StatefulSet ordinal.
func TestWALFilePrefixUsesEnvAndOrdinal(t *testing.T) {
	// Agent: 生产 STS 的 HOSTNAME 形如 worb-1，前缀必须是 prod-1。
	t.Setenv("APP_ENV", "prod")
	t.Setenv("HOSTNAME", "worb-1")
	got, err := walFilePrefix()
	if err != nil {
		t.Fatalf("walFilePrefix: %v", err)
	}
	if got != "prod-1" {
		t.Fatalf("prefix=%s want prod-1", got)
	}
}

// TestWALFilePrefixLocalDefault 本机未配 APP_ENV、主机名不是 STS 序号时用 local-0。
// TestWALFilePrefixLocalDefault uses local-0 when APP_ENV is unset and HOSTNAME has no ordinal.
func TestWALFilePrefixLocalDefault(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("HOSTNAME", "dev-macbook.local")
	got, err := walFilePrefix()
	if err != nil {
		t.Fatalf("walFilePrefix: %v", err)
	}
	if got != "local-0" {
		t.Fatalf("prefix=%s want local-0", got)
	}
}

// TestWALFilePrefixRejectsUnsafeEnv APP_ENV 含路径字符时拒绝，避免写出 dataDir 之外。
// TestWALFilePrefixRejectsUnsafeEnv rejects APP_ENV with path characters so files stay in dataDir.
func TestWALFilePrefixRejectsUnsafeEnv(t *testing.T) {
	t.Setenv("APP_ENV", "../prod")
	t.Setenv("HOSTNAME", "worb-0")
	if _, err := walFilePrefix(); err == nil {
		t.Fatal("unsafe APP_ENV accepted")
	}
}

// TestWALInstanceFilesAreSeparate 不同环境、不同序号各自写文件，不读对方的 ndjson。
// TestWALInstanceFilesAreSeparate writes per env and ordinal and does not read the other file.
func TestWALInstanceFilesAreSeparate(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("APP_ENV", "prod")
	t.Setenv("HOSTNAME", "worb-0")
	wProd, err := newWAL(dir)
	if err != nil {
		t.Fatalf("prod newWAL: %v", err)
	}
	if err := wProd.Append(walEntry{RunID: "run-prod"}); err != nil {
		t.Fatalf("prod append: %v", err)
	}
	wProd.close()

	prodPath := filepath.Join(dir, "prod-0-wal.ndjson")
	prodInfo, err := os.Stat(prodPath)
	if err != nil {
		t.Fatalf("prod wal missing: %v", err)
	}
	if prodInfo.Size() == 0 {
		t.Fatal("prod wal empty after append")
	}

	t.Setenv("APP_ENV", "pre")
	t.Setenv("HOSTNAME", "worb-0")
	wPre, err := newWAL(dir)
	if err != nil {
		t.Fatalf("pre newWAL: %v", err)
	}
	preInfo, err := wPre.file.Stat()
	if err != nil {
		t.Fatalf("pre stat: %v", err)
	}
	// Agent: 预发打开空文件，证明没有接着读 prod-0 的内容。
	if preInfo.Size() != 0 {
		t.Fatalf("pre wal size=%d, would have consumed prod content", preInfo.Size())
	}
	if filepath.Base(wPre.ndjsonPath) != "pre-0-wal.ndjson" {
		t.Fatalf("pre file=%s", wPre.ndjsonPath)
	}
	wPre.close()

	t.Setenv("APP_ENV", "prod")
	t.Setenv("HOSTNAME", "worb-1")
	wPod1, err := newWAL(dir)
	if err != nil {
		t.Fatalf("pod1 newWAL: %v", err)
	}
	if filepath.Base(wPod1.ndjsonPath) != "prod-1-wal.ndjson" {
		t.Fatalf("pod1 file=%s", wPod1.ndjsonPath)
	}
	pod1Info, err := wPod1.file.Stat()
	if err != nil {
		t.Fatalf("pod1 stat: %v", err)
	}
	if pod1Info.Size() != 0 {
		t.Fatalf("pod1 wal size=%d, would have consumed pod0 content", pod1Info.Size())
	}
	wPod1.close()

	if _, err := os.Stat(filepath.Join(dir, "wal.ndjson")); !os.IsNotExist(err) {
		t.Fatalf("unprefixed wal.ndjson should not be created, err=%v", err)
	}
}

// TestNewWALRejectsUnsafeEnv 启动时 APP_ENV 不安全则打不开 WAL。
// TestNewWALRejectsUnsafeEnv fails WAL open when APP_ENV is not a safe prefix.
func TestNewWALRejectsUnsafeEnv(t *testing.T) {
	t.Setenv("APP_ENV", "prod/pre")
	t.Setenv("HOSTNAME", "worb-0")
	if _, err := newWAL(t.TempDir()); err == nil {
		t.Fatal("newWAL accepted unsafe APP_ENV")
	}
}

// TestWalFilePrefixCases 覆盖环境、序号和非法 APP_ENV 的组合。
// TestWalFilePrefixCases covers env, ordinal, and rejected APP_ENV combinations.
func TestWalFilePrefixCases(t *testing.T) {
	cases := []struct {
		name     string
		appEnv   string
		hostname string
		want     string
		wantErr  bool
	}{
		{name: "prod-0", appEnv: "prod", hostname: "worb-0", want: "prod-0"},
		{name: "pre-2", appEnv: "pre", hostname: "worb-2", want: "pre-2"},
		{name: "sts extra hyphen", appEnv: "prod", hostname: "worb-web-3", want: "prod-3"},
		{name: "trim APP_ENV", appEnv: "  prod  ", hostname: "worb-0", want: "prod-0"},
		{name: "underscore env", appEnv: "prod_canary", hostname: "worb-0", want: "prod_canary-0"},
		{name: "empty env", appEnv: "", hostname: "laptop", want: "local-0"},
		{name: "no ordinal", appEnv: "pre", hostname: "dev-macbook.local", want: "pre-0"},
		{name: "trailing hyphen", appEnv: "prod", hostname: "worb-", want: "prod-0"},
		// Agent: 路径字符必须拒绝，否则前缀会写出 dataDir 之外。
		{name: "slash", appEnv: "prod/pre", hostname: "worb-0", wantErr: true},
		{name: "dot dot", appEnv: "../prod", hostname: "worb-0", wantErr: true},
		{name: "space", appEnv: "prod pre", hostname: "worb-0", wantErr: true},
		{name: "backslash", appEnv: `prod\pre`, hostname: "worb-0", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("APP_ENV", tc.appEnv)
			t.Setenv("HOSTNAME", tc.hostname)
			got, err := walFilePrefix()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("prefix=%s, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("walFilePrefix: %v", err)
			}
			if got != tc.want {
				t.Fatalf("prefix=%s want %s", got, tc.want)
			}
		})
	}
}

// TestWalInstancePathsUsePrefix ndjson 与 pos 使用同一前缀，compact 临时文件也带这个前缀。
// TestWalInstancePathsUsePrefix keeps ndjson, pos, and compact tmp on the same prefix.
func TestWalInstancePathsUsePrefix(t *testing.T) {
	t.Setenv("APP_ENV", "pre")
	t.Setenv("HOSTNAME", "worb-4")
	dir := t.TempDir()
	ndjson, pos, err := walInstancePaths(dir)
	if err != nil {
		t.Fatalf("walInstancePaths: %v", err)
	}
	if filepath.Base(ndjson) != "pre-4-wal.ndjson" {
		t.Fatalf("ndjson=%s", ndjson)
	}
	if filepath.Base(pos) != "pre-4-wal.pos" {
		t.Fatalf("pos=%s", pos)
	}
	if !strings.HasPrefix(ndjson, dir) || !strings.HasPrefix(pos, dir) {
		t.Fatal("paths escaped dataDir")
	}
	if filepath.Base(ndjson+".tmp") != "pre-4-wal.ndjson.tmp" {
		t.Fatal("compact tmp lost prefix")
	}
}

// TestWALPosFileUsesSamePrefix 写入后 pos 文件名与 ndjson 前缀一致。
// TestWALPosFileUsesSamePrefix names the pos file with the same prefix as ndjson.
func TestWALPosFileUsesSamePrefix(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("APP_ENV", "prod")
	t.Setenv("HOSTNAME", "worb-0")
	w, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}
	w.pos = 12
	w.savePos()
	w.close()
	if _, err := os.Stat(filepath.Join(dir, "prod-0-wal.pos")); err != nil {
		t.Fatalf("prefixed pos missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal.pos")); !os.IsNotExist(err) {
		t.Fatalf("unprefixed wal.pos should not exist, err=%v", err)
	}
}

// TestWALIgnoresUnprefixedLegacyFile 目录里残留的 wal.ndjson 不会被本实例打开。
// TestWALIgnoresUnprefixedLegacyFile does not open leftover unprefixed wal.ndjson.
func TestWALIgnoresUnprefixedLegacyFile(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "wal.ndjson")
	if err := os.WriteFile(legacy, []byte("{\"run_id\":\"legacy\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_ENV", "prod")
	t.Setenv("HOSTNAME", "worb-0")
	w, err := newWAL(dir)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}
	defer w.close()
	if w.size != 0 {
		t.Fatalf("instance size=%d, opened legacy content", w.size)
	}
	info, err := os.Stat(legacy)
	if err != nil {
		t.Fatalf("legacy missing: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("legacy file was truncated")
	}
	if filepath.Base(w.ndjsonPath) != "prod-0-wal.ndjson" {
		t.Fatalf("file=%s", w.ndjsonPath)
	}
}
