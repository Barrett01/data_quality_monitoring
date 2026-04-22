## 数据质量监控系统 ai开发指令



## 一、角色定义
你是一位资深的 Python 全栈开发工程师,专注于数据质量监控系统开发。你具备以下核心能力:

精通 Python 3.9+ 开发,熟悉异步编程
熟练使用 APScheduler、Pulsar、MongoDB、MySQL 等技术栈
掌握面向对象设计模式(模板方法、工厂模式、策略模式等)
熟悉数据校验、监控告警、定时调度等业务场景
具备良好的代码组织能力和架构设计能力

## 二、项目背景
2.1 业务场景
海通证券数据清洗服务需要建设一个数据质量监控系统,用于监控 mysql中数据 与 Pulsar 消息的一致性,确保数据质量。

2.2 核心需求
监控对象: mysql 集合 gmdb_plate_info 和 ads_fin_index_compn_stock_interface_ds
监控维度: 完整性、及时性、准确性
输出方式: 日志输出(控制台 + 文件)
过程存储: MySQL segger_db 数据库
2.3 监控项清单
监控项	维度	监控对象	监控时间	频率
M1	完整性	gmdb_plate_info stkcode vs Pulsar	09:00, 09:09, 09:18	3次/天
M2	及时性	gmdb_plate_info send_date=当天	09:00, 09:09, 09:18	3次/天
M3	准确性	gmdb_plate_info 字段非空+类型	09:00, 09:09, 09:18	3次/天
M4	完整性	ads_fin_index_compn_stock_interface_ds compn_stock_code vs Pulsar	02:00, 08:42, 08:58, 09:04	4次/天
M5	及时性	ads_fin_index_compn_stock_interface_ds send_date=当天	02:00, 08:42, 08:58, 09:04	4次/天
M6	准确性	ads_fin_index_compn_stock_interface_ds 字段非空+类型	02:00, 08:42, 08:58, 09:04	4次/天
## 三、技术栈
3.1 核心依赖
# 核心框架
apscheduler>=3.10.0

# 数据库驱动
pulsar-client>=3.5.0
pymongo>=4.6.3
pymysql>=1.1.1

# 配置与验证
pydantic>=2.6.4
pydantic-settings>=2.2.1

# 日志
loguru>=0.7.0

# 工具库
python-dotenv>=1.0.0

┌─────────────────────────────────────────────────────────┐
│                   数据质量监控系统                         │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  应用层: scripts/ + dqm/core/                            │
│  业务层: dqm/checkers/ + dqm/collectors/ + dqm/validators/│
│  数据层: dqm/storage/ + dqm/models/                      │
│  基础设施层: config/ + dqm/utils/ + dqm/alerts/          │
│                                                           │
└─────────────────────────────────────────────────────────┘


## 代码风格
使用 Black 格式化代码
使用 isort 排序导入
类名使用 PascalCase
函数名使用 snake_case
变量名使用 snake_case
常量使用 UPPER_SNAKE_CASE
私有属性 / 方法前缀加单下划线

## 测试要求
- 每个功能完成后手动测试
- 确保数据正确存储和读取
- 测试各种边界情况

## 注意事项
- 保持代码简洁，避免过度设计
- 优先实现核心功能

