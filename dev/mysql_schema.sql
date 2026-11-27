-- 在库 worb 中手动执行。应用启动只检查这些表和索引，不会自己建表。
-- 顺序不能颠倒：后面的外键依赖前面的表。
--
-- 与 SQLite 启动迁移（internal/store/sql_manager.go、file_sql.go）对照：
-- 表名相同，共 11 张。SQLite 用 TEXT/INTEGER/REAL，这里用 VARCHAR/LONGTEXT/INT/BIGINT/DOUBLE/DATETIME。
-- SQLite 有的具名索引这里都有。SQLite 的 UNIQUE(entity, name) 在这里叫 uq_projects_entity_name，列相同。
-- system_events、console_logs：SQLite 用隐式 rowid，这里用 id 自增主键。业务查询不读这个 id。
-- files 有 idx_files_run_id (run_id)，给外键和按 run 翻页用。启动检查要求这个名字。
-- files 另有 idx_files_run_name (run_id, name(191))。name 是 VARCHAR(1024)，utf8mb4 下整列放不进 InnoDB 索引，所以只索引前 191 个字符。等值查找仍会回表核对完整文件名。
-- artifact_files 的五个二级索引和外键，启动检查要求必须有。SQLite 只建了表，没有这些索引，也没有外键。
-- runs.tags：SQLite 默认 '[]'，这里没有默认值。应用插入时会写入 tags。
-- projects.name：SQLite 是不限长的 TEXT，这里是 VARCHAR(191)，否则 (entity, name) 唯一索引超过 InnoDB 长度上限。

-- 项目。同一个 entity 下 name 唯一。entity 是 user_cluster_info.id 的十进制字符串，没有登录时为 local。
CREATE TABLE projects (
  id VARCHAR(128) NOT NULL COMMENT '项目 id',
  entity VARCHAR(512) NOT NULL DEFAULT 'local' COMMENT '所属用户。user_cluster_info.id 的十进制字符串；没有身份时为 local',
  name VARCHAR(191) NOT NULL COMMENT '项目名。与 entity 组成唯一键。长度 191 是为了唯一索引能放下',
  created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  deleted_at DATETIME NULL COMMENT '软删时间。非空表示项目已删除',
  PRIMARY KEY (id),
  UNIQUE KEY uq_projects_entity_name (entity, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='wandb 项目';

-- 一次实验。name 是 wandb 的 run 名，页面展示用 display_name。
CREATE TABLE runs (
  id VARCHAR(128) NOT NULL COMMENT 'run id',
  project_id VARCHAR(128) NOT NULL COMMENT '所属项目，对应 projects.id',
  name VARCHAR(255) NOT NULL COMMENT 'wandb run 名。file_stream 按这个名字查找',
  display_name VARCHAR(255) NULL COMMENT '页面上显示的名字。空则用 name',
  config LONGTEXT COMMENT '启动配置，JSON 文本',
  summary LONGTEXT COMMENT '最终摘要，JSON 文本',
  state VARCHAR(32) DEFAULT 'running' COMMENT '运行状态。代码写入 running、finished、failed',
  host VARCHAR(1024) COMMENT '运行所在主机',
  program VARCHAR(1024) COMMENT '入口程序',
  git_commit VARCHAR(128) COMMENT '代码版本',
  tags LONGTEXT COMMENT '标签，JSON 文本。SQLite 默认空数组，这里不设默认值',
  notes LONGTEXT COMMENT '备注。失败时也会把原因写在这里',
  group_name VARCHAR(255) COMMENT 'wandb group',
  job_type VARCHAR(255) COMMENT 'wandb job_type',
  sweep_name VARCHAR(255) COMMENT 'wandb sweep 名',
  history_line_count INT NOT NULL DEFAULT 0 COMMENT '已写入的指标行数，图表横轴用',
  events_line_count INT NOT NULL DEFAULT 0 COMMENT '已写入的系统事件行数',
  log_line_count INT NOT NULL DEFAULT 0 COMMENT '已写入的控制台日志行数',
  received_history_count INT NOT NULL DEFAULT 0 COMMENT '客户端已上报、尚未按行去重的 history 条数',
  fork_run_id VARCHAR(128) NULL COMMENT 'fork 来源 run 的 id。不是 fork 则为空',
  fork_step INT NULL COMMENT '从父 run 的哪一步开始 fork',
  created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最后一次内容更新时间',
  heartbeat_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最后一次心跳时间',
  deleted_at DATETIME NULL COMMENT '软删时间。非空表示 run 已删除',
  PRIMARY KEY (id),
  KEY idx_runs_project_id (project_id),
  CONSTRAINT fk_runs_project FOREIGN KEY (project_id) REFERENCES projects (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='一次实验 run';

-- 标量指标。一个 run、一个指标名、一个 step 只有一行。
CREATE TABLE history_scalars (
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  step INT NOT NULL COMMENT '训练步',
  `key` VARCHAR(255) NOT NULL COMMENT '指标名，例如 loss',
  value DOUBLE NOT NULL COMMENT '该步的数值',
  x_step DOUBLE NULL COMMENT '横轴值。空则图表用 step',
  PRIMARY KEY (run_id, `key`, step),
  KEY idx_history_scalars_run_step (run_id, step),
  CONSTRAINT fk_history_scalars_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='标量指标历史';

-- 直方图。bins 和 vals 是 JSON 文本。
CREATE TABLE history_histograms (
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  step INT NOT NULL COMMENT '训练步',
  `key` VARCHAR(255) NOT NULL COMMENT '直方图指标名',
  x_step DOUBLE NULL COMMENT '横轴值。空则用 step',
  bins LONGTEXT NOT NULL COMMENT '分桶边界，JSON 文本',
  vals LONGTEXT NOT NULL COMMENT '每个桶的计数，JSON 文本',
  PRIMARY KEY (run_id, `key`, step),
  CONSTRAINT fk_history_histograms_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='直方图历史';

-- 某个 run 出现过的 step。用来数步数、查 fork 的最大步。
CREATE TABLE run_steps (
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  step INT NOT NULL COMMENT '出现过的训练步',
  PRIMARY KEY (run_id, step),
  KEY idx_run_steps_run_step (run_id, step),
  CONSTRAINT fk_run_steps_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='run 的 step 去重表';

-- 某个 run 出现过的指标名。图表勾选指标时按 key 排序读出。
CREATE TABLE run_keys (
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  `key` VARCHAR(255) NOT NULL COMMENT '指标名',
  PRIMARY KEY (run_id, `key`),
  CONSTRAINT fk_run_keys_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='run 的指标名';

-- wandb-events.jsonl。SQLite 用 rowid 删除，这里用 id 做主键。
CREATE TABLE system_events (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键。对应 SQLite 的 rowid，业务不读取',
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  line_num INT NOT NULL COMMENT '事件文件中的行号',
  data LONGTEXT NOT NULL COMMENT '一行系统事件的原文',
  `timestamp` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (id),
  KEY idx_system_events_run_id (run_id),
  CONSTRAINT fk_system_events_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='系统事件';

-- output.log。页面日志按 line_num 顺序读 line。
CREATE TABLE console_logs (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键。对应 SQLite 的 rowid，业务不读取',
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  line_num INT NOT NULL COMMENT '日志行号',
  line LONGTEXT NOT NULL COMMENT '一行控制台文本',
  stream VARCHAR(32) DEFAULT 'stdout' COMMENT '流名称。写入时固定为 stdout',
  `timestamp` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (id),
  KEY idx_console_logs_run_id (run_id),
  CONSTRAINT fk_console_logs_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='控制台日志';

-- Artifact 元数据。文件正文在 artifact_files，不在这张表。
CREATE TABLE artifacts (
  id VARCHAR(128) NOT NULL COMMENT 'artifact id',
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  `type` VARCHAR(255) NOT NULL COMMENT 'artifact 类型',
  name VARCHAR(255) NOT NULL COMMENT 'artifact 集合名',
  digest VARCHAR(255) NULL COMMENT '提交后的摘要。创建时为空',
  state VARCHAR(32) DEFAULT 'pending' COMMENT '状态。创建为 pending，提交后为 committed',
  metadata LONGTEXT COMMENT '元数据，JSON 文本',
  created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  CONSTRAINT fk_artifacts_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='artifact 元数据';

-- run 附件。id 同时是上传地址 /files/upload/{id} 里的 token。
CREATE TABLE files (
  id VARCHAR(128) NOT NULL COMMENT '文件 id，形如 run-{uuid}，也是上传 token',
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run，对应 runs.id',
  name VARCHAR(1024) NOT NULL COMMENT '文件名。对象键的最后一段',
  url LONGTEXT COMMENT '给客户端的上传地址，指向 /files/upload/{id}',
  size BIGINT NOT NULL DEFAULT 0 COMMENT '字节数。本地写完后更新',
  created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  entity VARCHAR(512) NULL COMMENT '所属用户原文，与 projects.entity 相同',
  entity_key VARCHAR(512) NULL COMMENT 'entity 的 Base64URL，用作对象键第一段',
  local_path VARCHAR(2048) NULL COMMENT '本地暂存路径，位于 dataDir/files/{id}',
  local_uploaded_at DATETIME NULL COMMENT '本地文件写完的时间',
  status VARCHAR(32) NULL COMMENT '上云状态。pending、local、uploading、cloud、failed',
  storage_type VARCHAR(32) NULL COMMENT '上云成功后的存储类型。本期为 oss',
  object_key VARCHAR(2048) NULL COMMENT '对象键。entity_key/stand-runs/{run_id}/{name}',
  cloud_uploaded_at DATETIME NULL COMMENT '上传到对象存储的时间',
  cloud_etag VARCHAR(255) NULL COMMENT '对象存储返回的 ETag',
  error_message VARCHAR(1024) NULL COMMENT '最近一次上云或删除失败的原因',
  retry_count INT NOT NULL DEFAULT 0 COMMENT '已重试次数。达到 3 次后不再自动重试',
  local_deleted_at DATETIME NULL COMMENT '本地文件删除时间',
  deleted_at DATETIME NULL COMMENT '业务软删时间。非空则用户不可见',
  updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '行更新时间。进入永久失败时也会更新',
  PRIMARY KEY (id),
  KEY idx_files_run_id (run_id),
  KEY idx_files_run_name (run_id, name(191)),
  KEY idx_files_entity (entity),
  KEY idx_files_entity_key (entity_key),
  KEY idx_files_status (status),
  KEY idx_files_deleted_at (deleted_at),
  CONSTRAINT fk_files_run FOREIGN KEY (run_id) REFERENCES runs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='run 附件及上云状态';

-- Artifact 上传的文件。不与 files 混用。id 形如 art-{uuid}。
CREATE TABLE artifact_files (
  id VARCHAR(128) NOT NULL COMMENT '文件 id，形如 art-{uuid}，也是上传 token',
  run_id VARCHAR(128) NOT NULL COMMENT '所属 run。从 artifact 的 run 带入，不写空',
  artifact_id VARCHAR(128) NOT NULL COMMENT '所属 artifact，对应 artifacts.id',
  name VARCHAR(1024) NOT NULL COMMENT '文件名。对象键的最后一段',
  url LONGTEXT COMMENT '给客户端的上传地址，指向 /files/upload/{id}',
  size BIGINT NOT NULL DEFAULT 0 COMMENT '字节数。本地写完后更新',
  created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  entity VARCHAR(512) NULL COMMENT '所属用户原文，与 projects.entity 相同',
  entity_key VARCHAR(512) NULL COMMENT 'entity 的 Base64URL，用作对象键第一段',
  local_path VARCHAR(2048) NULL COMMENT '本地暂存路径，位于 dataDir/files/{id}',
  local_uploaded_at DATETIME NULL COMMENT '本地文件写完的时间',
  status VARCHAR(32) NULL COMMENT '上云状态。pending、local、uploading、cloud、failed',
  storage_type VARCHAR(32) NULL COMMENT '上云成功后的存储类型。本期为 oss',
  object_key VARCHAR(2048) NULL COMMENT '对象键。entity_key/stand-artifacts/{run_id}/{name}',
  cloud_uploaded_at DATETIME NULL COMMENT '上传到对象存储的时间',
  cloud_etag VARCHAR(255) NULL COMMENT '对象存储返回的 ETag',
  error_message VARCHAR(1024) NULL COMMENT '最近一次上云或删除失败的原因',
  retry_count INT NOT NULL DEFAULT 0 COMMENT '已重试次数。达到 3 次后不再自动重试',
  local_deleted_at DATETIME NULL COMMENT '本地文件删除时间',
  deleted_at DATETIME NULL COMMENT '业务软删时间。非空则用户不可见',
  updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '行更新时间。进入永久失败时也会更新',
  PRIMARY KEY (id),
  KEY idx_artifact_files_entity (entity),
  KEY idx_artifact_files_entity_key (entity_key),
  KEY idx_artifact_files_status (status),
  KEY idx_artifact_files_deleted_at (deleted_at),
  KEY idx_artifact_files_artifact_id (artifact_id),
  CONSTRAINT fk_artifact_files_artifact FOREIGN KEY (artifact_id) REFERENCES artifacts (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='artifact 附件及上云状态';
