# 数据质量监控系统 (DQM)

海通证券数据清洗服务旁路监控模块，通过定时比对 MySQL 生产表与 Pulsar 实时消息，验证数据的完整性、及时性和准确性。

## 监控项

| 监控项 | 维度 | 目标表 | 检查逻辑 | 调度时间 |
|--------|------|--------|----------|----------|
| M1 | 完整性 | `gmdb_plate_info` | `stkcode` 集合与 Pulsar 快照全局比对 | 09:00, 09:09, 09:18 |
| M2 | 及时性 | `gmdb_plate_info` | 按 `mst_type` 分组检查 `send_date` 是否存在当天数据 | 09:00, 09:09, 09:18 |
| M3 | 准确性 | `gmdb_plate_info` | 字段非空、正则、枚举、类型校验（超 10 万行采样） | 09:00, 09:09, 09:18 |
| M4 | 完整性 | `ads_fin_index_compn_stock_interface_ds` | 按 `stkcode` 分组比对 `compn_stock_code` 集合 | 02:00, 08:42, 08:58, 09:04 |
| M5 | 及时性 | `ads_fin_index_compn_stock_interface_ds` | 检查 `send_date` 是否存在当天数据 | 02:00, 08:42, 08:58, 09:04 |
| M6 | 准确性 | `ads_fin_index_compn_stock_interface_ds` | 字段非空、正则、枚举、类型校验（超 10 万行采样） | 02:00, 08:42, 08:58, 09:04 |

检查结果状态：`PASS` / `FAIL` / `ERROR` / `SKIP` / `NODATA`

## 架构

```
                    ┌─────────────────┐
                    │   APScheduler   │
                    │  (Cron 触发)    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   Coordinator   │
                    │  (检查编排)     │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
 ┌────────▼────────┐ ┌──────▼──────┐ ┌────────▼────────┐
 │ CompletenessChk │ │TimelinessChk│ │  AccuracyChk    │
 │    (M1 / M4)    │ │  (M2 / M5)  │ │   (M3 / M6)     │
 └───────┬─────────┘ └──────┬──────┘ └───────┬─────────┘
         │                  │                 │
    ┌────▼─────┐            │           ┌─────▼─────┐
    │  Pulsar  │            │           │ Validator │
    │Collector │            │           │  (字段校验) │
    └──────────┘            │           └───────────┘
                            │
                   ┌────────▼────────┐
                   │  MySQLStorage   │
                   │ (生产表查询 +   │
                   │  监控结果存储)  │
                   └─────────────────┘
```

检查流程采用模板方法模式：`_prepare() → _check() → _record() → _alert()`

## 目录结构

```
data_quality_monitoring/
├── config/                     # 配置
│   ├── .env                    # 环境变量
│   ├── .env.example            # 环境变量模板
│   ├── constants.py            # 常量与枚举
│   ├── logger_config.py        # 日志配置
│   ├── monitor_configs.py      # 监控项配置
│   └── settings.py             # 配置加载
├── scripts/                    # 运维脚本
│   ├── start.py                # 启动服务
│   ├── stop.py                 # 停止服务
│   ├── status.py               # 查看状态
│   ├── init_db.py              # 初始化数据库
│   └── manual_check.py         # 手动触发检查
├── src/dqm/
│   ├── core/                   # 核心调度
│   │   ├── runner.py           # 应用入口
│   │   ├── scheduler.py        # APScheduler 封装
│   │   └── coordinator.py      # 检查器编排
│   ├── checkers/               # 检查器
│   │   ├── base.py             # 抽象基类（模板方法）
│   │   ├── completeness.py     # 完整性检查（M1/M4）
│   │   ├── timeliness.py       # 及时性检查（M2/M5）
│   │   └── accuracy.py         # 准确性检查（M3/M6）
│   ├── collectors/             # 数据采集
│   │   ├── pulsar_collector.py # Pulsar 消息采集
│   │   └── mongo_collector.py  # MongoDB 采集（备用）
│   ├── validators/             # 校验规则
│   │   ├── field_validator.py  # 字段校验器
│   │   ├── type_validator.py   # 类型校验器
│   │   └── rules.py            # 规则定义
│   ├── storage/                # 存储层
│   │   ├── mysql_storage.py    # MySQL 连接管理
│   │   ├── repository.py       # 数据访问
│   │   └── schema.py           # DDL 定义
│   ├── alerts/
│   │   └── formatter.py        # 告警格式化
│   └── cleanup/
│       └── cleaner.py          # 过期数据清理
├── tests/
│   ├── unit/                   # 单元测试
│   └── integration/            # 集成测试
├── PRD.md                      # 产品需求文档
├── TECH_DESIGN.md              # 技术设计文档
├── AGENTS.md                   # AI 开发指引
├── pyproject.toml              # 项目配置
└── requirements.txt            # Python 依赖
```

## 快速开始

### 环境要求

- Python >= 3.9
- MySQL 5.7+
- Apache Pulsar（完整性检查需要）

### 安装

```bash
cd data_quality_monitoring
pip install -r requirements.txt
```

### 配置

复制环境变量模板并修改：

```bash
cp config/.env.example config/.env
```

关键配置项：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MYSQL_HOST` | MySQL 地址 | 127.0.0.1 |
| `MYSQL_PORT` | MySQL 端口 | 3306 |
| `MYSQL_DATABASE` | MySQL 数据库 | segger_db |
| `PULSAR_SERVICE_URL` | Pulsar 地址 | pulsar://127.0.0.1:6650 |
| `PULSAR_TOPIC` | Pulsar Topic | persistent://public/default/security_info |
| `PULSAR_COLLECT_WINDOW_SECONDS` | 采集窗口(秒) | 30 |
| `LOG_LEVEL` | 日志级别 | INFO |

### 初始化数据库

```bash
python scripts/init_db.py
```

创建 3 张监控表：`dqm_security_info_snapshot`、`dqm_check_result`、`dqm_accuracy_detail`

### 启动/停止服务

```bash
# 启动
python scripts/start.py

# 查看状态
python scripts/status.py

# 停止
python scripts/stop.py
```

### 手动触发检查

```bash
# 执行 M1 完整性检查
python scripts/manual_check.py --monitor M1

# 执行 M1 第 2 轮检查
python scripts/manual_check.py --monitor M1 --round 2
```

## 日志

日志输出到控制台及 `logs/` 目录：

| 文件 | 级别 | 说明 |
|------|------|------|
| `dqm.log` | INFO+ | 全量日志，10MB 轮转，保留 30 天 |
| `dqm_alert.log` | WARNING+ | 告警日志 |
| `dqm_error.log` | ERROR+ | 错误日志 |

## 数据清理

每天 01:00 自动执行过期数据清理：

| 数据 | 保留天数 |
|------|----------|
| 临时快照 | 15 天 |
| 检查结果 | 90 天 |
| 准确性明细 | 15 天 |

可通过环境变量 `SNAPSHOT_RETENTION_DAYS`、`CHECK_RESULT_RETENTION_DAYS`、`ACCURACY_DETAIL_RETENTION_DAYS` 调整。

## 测试

```bash
# 运行单元测试
pytest tests/unit/ -v

# 运行集成测试（需要本地 MySQL）
pytest tests/integration/ -v

# 查看覆盖率
pytest --cov=src/dqm tests/unit/ -v
```

## 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 调度引擎 | APScheduler | >= 3.10 |
| 消息队列 | Apache Pulsar | >= 3.5 |
| 数据库 | PyMySQL | >= 1.1 |
| 日志 | Loguru | >= 0.7 |
| 配置 | python-dotenv | >= 1.0 |
| 测试 | pytest | >= 7.0 |
