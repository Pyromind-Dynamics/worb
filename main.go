package main

import (
	"os"
	"path/filepath"

	"github.com/sarna/worb/internal/logerr"
	"github.com/sarna/worb/internal/server"
	"github.com/spf13/cobra"
)

func main() {
	home, _ := os.UserHomeDir()
	defaultDataDir := filepath.Join(home, ".worb")

	var host string
	var port int
	var dataDir string
	var dbEngine string

	cmd := &cobra.Command{
		Use:   "worb",
		Short: "Local, single-binary server compatible with the standard wandb Python client",
		// Agent: 启动失败时不打印整段用法，避免把错误原因淹没掉。
		SilenceUsage: true,
		// Agent: 错误由下面的 log.Fatal 打出，避免和 cobra 再打一遍。
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := server.New(server.Config{
				Host:     host,
				Port:     port,
				DataDir:  dataDir,
				DBEngine: dbEngine,
			})
			if err != nil {
				return err
			}
			return srv.Run()
		},
	}

	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "host to bind to (use 0.0.0.0 for all interfaces)")
	cmd.Flags().IntVar(&port, "port", 8080, "port to listen on")
	cmd.Flags().StringVar(&dataDir, "data", defaultDataDir, "data directory")
	// Agent: 增加 mysql。生产用 --db-engine mysql，本地和测试继续用 sqlite。
	cmd.Flags().StringVar(&dbEngine, "db-engine", "sqlite", "database engine (sqlite, mysql, or turso)")

	if err := cmd.Execute(); err != nil {
		// Agent: 进程退出前先打原因和堆栈，日志里不重复打印 cobra 的用法。
		logerr.Errorf("%v", err)
		os.Exit(1)
	}
}
