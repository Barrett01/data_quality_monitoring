## 数据质量监控系统技术设计


## 技术栈
Python	3.9+	开发语言
APScheduler	>=3.10.0	定时调度引擎,精确到分钟的定时任务
pulsar-client	3.5.0	Pulsar 客户端,监听实时消息
pymongo	4.6.3	MongoDB 客户端,查询线上表数据
pymysql	1.1.1	MySQL 客户端,查询存储过程数据和结果
pydantic	2.6.4	数据验证和配置管理
pydantic-settings	2.2.1	配置加载和环境变量管理
logging	Python 内置	日志输出(控制台+文件)
json	Python 内置	消息体解码

## 项目结构

数据质量监控系统 - 目录结构图

📁 data_quality_monitoring/
│
├── scripts/                          # 命令行脚本
│   ├── start.py                     # 服务启动脚本
│   ├── stop.py                      # 服务停止脚本
│   ├── status.py                    # 服务状态查询
│   ├── init_db.py                   # 数据库初始化脚本
│   └── manual_check.py              # 手动检查脚本
│
├── config/                          # 配置文件
│   ├── __init__.py                  # 模块初始化
│   ├── settings.py                  # 环境变量配置
│   ├── logger_config.py             # 日志配置
│   ├── constants.py                 # 常量定义
│   ├── monitor_configs.py           # 监控项配置
│   ├── schema.py                   # 数据库表结构
│   └── .env                        # 环境变量文件
│
├── src/dqm/                        # 主业务代码
│   ├── __init__.py                 # 模块初始化
│   │
│   ├── core/                       # 核心调度层
│   │   ├── __init__.py             # 模块初始化
│   │   ├── scheduler.py            # 定时任务调度器
│   │   ├── coordinator.py          # 协调器
│   │   └── runner.py               # 应用入口
│   │
│   ├── checkers/                   # 数据质量检查器
│   │   ├── __init__.py             # 模块初始化
│   │   ├── base.py                 # 检查器基类
│   │   ├── completeness.py         # 完整性检查器
│   │   ├── timeliness.py           # 及时性检查器
│   │   └── accuracy.py             # 准确性检查器
│   │
│   ├── storage/                    # 数据存储层
│   │   ├── __init__.py             # 模块初始化
│   │   ├── mysql_storage.py        # MySQL 存储封装
│   │   ├── repository.py           # 数据访问对象
│   │   └── schema.py              # 数据库表结构
│   │
│   ├── collectors/                  # 数据采集器
│   │   ├── __init__.py             # 模块初始化
│   │   ├── mongo_collector.py      # MongoDB 采集器
│   │   └── pulsar_collector.py    # Pulsar 采集器
│   │
│   ├── validators/                  # 数据校验器
│   │   ├── __init__.py             # 模块初始化
│   │   ├── field_validator.py      # 字段校验器
│   │   └── rules.py               # 规则引擎
│   │
│   ├── alerts/                     # 告警模块
│   │   ├── __init__.py             # 模块初始化
│   │   └── formatter.py           # 告警格式化器
│   │
│   ├── cleanup/                     # 数据清理模块
│   │   ├── __init__.py             # 模块初始化
│   │   └── cleaner.py             # 数据清理器
│   │
│   └── utils/                      # 工具函数
│       ├── __init__.py             # 模块初始化
│       └── helpers.py              # 辅助函数
│
├── tests/                          # 测试目录
│   ├── __init__.py                 # 模块初始化
│   │
│   ├── unit/                       # 单元测试
│   │   ├── __init__.py             # 模块初始化
│   │   ├── test_checkers/          # 检查器测试
│   │   ├── test_collectors/        # 采集器测试
│   │   │
│   │   ├── test_validators/        # 校验器测试
│   │   │
│   │   ├── test_storage/           # 存储测试
│   │   │
│   │   └── test_core/              # 核心层测试
│   │
│   └── integration/                 # 集成测试
│       ├── __init__.py             # 模块初始化
│       ├── test_scheduler.py       # 调度器集成测试
│       ├── test_e2e.py             # 端到端测试
│       └── fixtures/               # 测试数据
│           ├── __init__.py        # 模块初始化
│           ├── sample_data.json
│           └── test_db_setup.sql
│
├── data/                          # 数据目录
│   ├── logs/                      # 日志文件
│   │   ├── dqm.log               # 主日志
│   │   ├── dqm_error.log         # 错误日志
│   │   └── dqm_alert.log         # 告警日志
│   ├── snapshots/                 # 快照数据
│   └── temp/                      # 临时文件
│
├── docs/                          # 文档目录
│   ├── api.md                    # API 文档
│   ├── architecture.md           # 架构说明
│   ├── deployment.md             # 部署文档
│   └── examples/                 # 示例代码
│
├── requirements.txt              # Python 依赖
├── setup.py                      # 安装脚本
└── README.md                     # 项目说明






## 关键技术点
┌─────────────────────────────────────────────────────────────┐
│                    数据质量监控系统 - 核心技术点              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. 旁路监控架构 (Pulsar 独立 Consumer)                       │
│  2. 定时调度引擎 (APScheduler 精确调度)                        │
│  3. 模板方法模式 (BaseChecker 抽象基类)                       │
│  4. 快照采集机制 (时间窗口 + 消息过滤)                        │
│  5. 数据比对算法 (集合差异 + 按组比对)                         │
│  6. 字段校验规则 (非空 + 类型 + 正则)                         │
│  7. 幂等性保障 (去重 + 覆盖更新)                              │
│  8. 过期数据清理 (定时任务 + 索引优化)                        │
│  9. 异常容错机制 (降级 + 重试 + 隔离)                          │
│  10. 分层架构设计 (应用层 + 业务层 + 数据层)                   │
│                                                               │
└─────────────────────────────────────────────────────────────┘

