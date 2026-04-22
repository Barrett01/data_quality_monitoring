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

data-quality-monitor/
│
├── README.md                          # 项目说明文档
├── requirements.txt                   # Python 依赖
├── pyproject.toml                     # 项目配置(可选,用于打包)
├── .env.example                       # 环境变量示例
├── .gitignore                         # Git 忽略文件
│
├── config/                            # 配置文件目录
│   ├── __init__.py
│   ├── settings.py                    # Pydantic 配置类
│   ├── constants.py                   # 常量定义
│   └── logger_config.py               # 日志配置
│
├── src/                               # 源代码目录
│   └── dqm/                           # Data Quality Monitor 包
│       ├── __init__.py
│       │
│       ├── core/                      # 核心模块
│       │   ├── __init__.py
│       │   ├── scheduler.py           # 调度引擎
│       │   ├── coordinator.py         # 检查协调器
│       │   └── runner.py              # 运行入口
│       │
│       ├── checkers/                  # 检查器模块
│       │   ├── __init__.py
│       │   ├── base.py                # 检查器抽象基类
│       │   ├── completeness.py        # 完整性检查器
│       │   ├── timeliness.py          # 及时性检查器
│       │   └── accuracy.py            # 准确性检查器
│       │
│       ├── collectors/                # 数据采集模块
│       │   ├── __init__.py
│       │   ├── base.py                # 采集器基类
│       │   ├── mongo_collector.py    # MongoDB l
│       │   ├── pulsar_collector.py   # Pulsar 消息采集
│       │   └── snapshot.py            # 快照管理器
│       │
│       ├── validators/                # 校验器模块
│       │   ├── __init__.py
│       │   ├── field_validator.py    # 字段校验器
│       │   ├── type_validator.py     # 类型校验器
│       │   └── rules.py              # 校验规则定义
│       │
│       ├── storage/                   # 存储模块
│       │   ├── __init__.py
│       │   ├── base.py                # 存储基类
│       │   ├── mysql_storage.py      # MySQL 存储实现
│       │   ├── schema.py              # 数据库表定义
│       │   └── repository.py          # 数据访问层
│       │
│       ├── models/                    # 数据模型
│       │   ├── __init__.py
│       │   ├── check_result.py       # 检查结果模型
│       │   ├── snapshot.py           # 快照数据模型
│       │   ├── field_error.py        # 字段错误模型
│       │   └── config.py             # 监控配置模型
│       │
│       ├── alerts/                    # 告警模块
│       │   ├── __init__.py
│       │   ├── formatter.py          # 告警格式化
│       │   └── logger.py             # 告警日志输出
│       │
│       ├── cleanup/                   # 清理模块
│       │   ├── __init__.py
│       │   ├── cleaner.py            # 过期数据清理器
│       │   └── scheduler.py          # 清理调度器
│       │
│       ├── utils/                     # 工具模块
│       │   ├── __init__.py
│       │   ├── date_utils.py         # 日期工具
│       │   ├── mongo_utils.py        # MongoDB 工具
│       │   ├── pulsar_utils.py       # Pulsar 工具
│       │   └── mysql_utils.py        # MySQL 工具
│       │
│       └── config/                    # 监控配置目录
│           ├── __init__.py
│           └── monitor_configs.py    # 各监控项配置
│
├── scripts/                           # 脚本目录
│   ├── start.py                       # 服务启动脚本
│   ├── stop.py                        # 服务停止脚本
│   ├── status.py                      # 服务状态查询
│   ├── init_db.py                     # 数据库初始化脚本
│   └── manual_check.py                # 手动执行检查脚本
│
├── tests/                             # 测试目录
│   ├── __init__.py
│   ├── conftest.py                    # pytest 配置
│   ├── unit/                          # 单元测试
│   │   ├── test_checkers/
│   │   ├── test_collectors/
│   │   ├── test_validators/
│   │   └── test_storage/
│   └── integration/                    # 集成测试
│       ├── test_scheduler.py
│       └── test_e2e.py
│
├── docs/                              # 文档目录
│   ├── architecture.md               # 架构设计文档
│   ├── api.md                         # API 文档
│   ├── deployment.md                  # 部署文档
│   └── troubleshooting.md             # 故障排查文档
│
├── logs/                              # 日志目录(运行时生成)
│   ├── dqm.log                        # 主日志
│   ├── dqm_error.log                  # 错误日志
│   └── dqm_alert.log                  # 告警日志





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

