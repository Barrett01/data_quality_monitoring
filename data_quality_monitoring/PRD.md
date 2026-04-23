## 数据质量监控系统需要文档

## 一，产品概述
**hidatapilot-data-clean 数据质量监控系统** 的完整产品需求。该系统作为现有数据清洗服务的旁路监控模块，通过定时任务对关键数据表进行**完整性、及时性、准确性**三维度校验，以日志形式输出检查结果与告警，过程数据持久化到 MySQL，支撑问题追溯和历史回溯。

## 1，适用范围
| 范围 | 说明 |
|------|------|
| 目标系统 | hidatapilot-data-clean 数据清洗服务 |
| 监控对象 | mysql 表 `gmdb_plate_info`、`ads_fin_index_compn_stock_interface_ds`；Pulsar topic `persistent://public/default/security_info` |
| 数据源 | mysql（线上正式表） + Pulsar 消息（临时快照） |
| 输出方式 | Python `logging` 日志（控制台 + 文件） |
| 过程存储 | MySQL `segger_db` 数据库 |


## 2，术语定义
| 术语 | 定义 |
|------|------|
| **线上表** | mysql 中的正式持久化（如 `gmdb_plate_info`、`ads_fin_index_compn_stock_interface_ds`）|
| **临时表** | 监控系统在 MySQL 中写数据进入临时快照表，用于存储从 Pulsar 消息中解码得到的"期望数据"，供与线上表比对 |
| **stkcode** | 板块代码，`gmdb_plate_info` 的核心标识字段 |
| **compn_stock_code** | 成分股代码，`ads_fin_index_compn_stock_interface_ds` 的核心标识字段 |
| **完整性** | 线上表与临时表的核心代码集合完全一致（无遗漏、无多余） |
| **及时性** | 线上表在预期时间窗口内存在当天 `send_date` 的数据 |
| **准确性** | 字段非空、字段值符合预定义的数据类型约束 |

## 二，系统架构

### 1， 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        数据质量监控系统                               │
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐              │
│  │  完整性检查   │    │  及时性检查   │    │  准确性检查   │              │
│  │  (Checker)  │    │  (Checker)  │    │  (Checker)  │              │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘              │
│         │                  │                  │                      │
│         └──────────────────┼──────────────────┘                      │
│                            ▼                                         │
│                   ┌────────────────┐                                 │
│                   │  调度引擎       │                                 │
│                   │  (Scheduler)   │                                 │
│                   └───────┬────────┘                                 │
│                           │                                          │
│              ┌────────────┼────────────┐                             │
│              ▼            ▼            ▼                             │
│     ┌──────────────┐ ┌──────────┐ ┌──────────────┐                  │
│     │  日志输出     │ │ MySQL    │ │  告警判断     │                  │
│     │  (logging)   │ │ 过程存储  │ │  (Alert)     │                  │
│     └──────────────┘ └──────────┘ └──────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘

数据源：
┌──────────────┐         ┌──────────────────────┐
│  MongoDB     │         │  Pulsar               │
│  (线上表)    │         │  (实时消息 → 临时快照) │
└──────────────┘         └──────────────────────┘
```
## 三，核心设计原则

1. **模块迭代**：生成一个功能，测试一下功能，一个功能测试完毕，再生成下一个功能
2. **日志驱动**：所有检查结果和告警通过 Python `logging` 输出，无 Web UI
3. **过程可溯**：每次检查的快照数据存入 MySQL，支持历史回溯
4. **自动清理**：过程数据按策略自动过期删除，避免无限膨胀

## 四、监控规则详细设计
## 4.1 监控项 M1：gmdb_plate_info 完整性检查
## 4.1.1 监控目标
验证 mysql `gmdb_plate_info` 表中的 `stkcode`（板块代码）与 Pulsar的topic `security_info` 中实时消息解码后得到的 `stkcode` 完全一致。

### 4.1.2 监控规则
| 属性 | 值 |
|------|-----|
| **监控对象** | mysql `gmdb_plate_info` 的 `stkcode` 字段 vs Pulsar  topic `security_info` 中解码后的 `stkcode` |
| **监控维度** | 完整性 |
| **监控时间** | 每天 09:00 - 09:18 |
| **监控频率** | 每 9 分钟一次（即 09:00、09:09、09:18，共 3 次） |
| **比对方式** | 等价比对：线上表 stkcode 集合 ⊇ 临时表 stkcode 集合，且无多余 |
| **告警条件** | ① 临时表有但线上表无 → 告警"数据遗漏" ② 线上表有但临时表无 → 告警"数据多余" |
| **告警级别** | ERROR |
| **通过级别** | INFO |

## 4.1.3 临时快照采集逻辑

```
1. 在监控时间窗口内，监听 Pulsar topic: persistent://public/default/security_info
2. 使用独立 Consumer（非共享订阅，避免影响主消费流程），订阅名：data-quality-monitor-sub
3. 接收消息后，将消息体写入临时文件 解码 JSON 消息体，提取 stkcode 字段
5. 将 {check_date, check_time, stkcode, stkname, mst_type...} 写入 MySQL 临时表 dqm_security_info_snapshot
```

## 4.1.5 过程数据存储

| 存储位置 | MySQL `segger_db` |
|----------|-------------------|
| **临时快照表** | `dqm_security_info_snapshot` |
| **检查结果表** | `dqm_check_result` |
| **留存策略** | 每天仅保留最新一份快照数据（同一天同一次检查覆盖前一次）；快照数据保留 15 天，过期自动删除 |
| **清理方式** | 每次检查前，删除 `check_date < (current_date - 15天)` 的快照记录 |

---
## 4.2 监控项 M2：gmdb_plate_info 及时性检查
## 4.2.1 监控目标

验证 MySQL表 `gmdb_plate_info` 在每天盘前时段存在 `send_date = 当天日期` 的数据

#### 4.2.2 监控规则

| 属性 | 值 |
|------|-----|
| **监控对象** | mysql `gmdb_plate_info` 中 `send_date` 字段 |
| **监控维度** | 及时性 |
| **监控时间** | 每天 09:00 - 09:18 |
| **监控频率** | 每 9 分钟一次（即 09:00、09:09、09:18，共 3 次） |
| **检查条件** | 查询 `gmdb_plate_info` 中是否存在 `send_date = current_date` 的记录 |
| **告警条件** | 查询结果为空（无当天数据）→ 告警"数据未及时到达" |
| **告警级别** | ERROR |
| **通过级别** | INFO |

### 4.2.4 过程数据存储

| 存储位置 | MySQL `segger_db` |
|----------|-------------------|
| **检查结果表** | `dqm_check_result`（记录每次检查的时间、监控项、结果、详情） |
| **留存策略** | 检查结果保留 90 天，过期自动删除 |


## 4.3 监控项 M3：gmdb_plate_info 准确性检查

#### 4.3.1 监控目标
验证 mysql `gmdb_plate_info` 集合中所有字段数据非空且满足字段类型约束。
#### 4.3.2 监控规则

| 属性 | 值 |
|------|-----|
| **监控对象** | mysql `gmdb_plate_info` |
| **监控维度** | 准确性（非空 + 类型） |
| **监控时间** | 每天 09:00 - 09:18 |
| **监控频率** | 每 9 分钟一次（即 09:00、09:09、09:18，共 3 次） |
| **告警条件** | ① 任一非空字段存在 null/空字符串 → 告警 ② 任一字段值不满足预定义类型 → 告警 |
| **告警级别** | ERROR |
| **通过级别** | INFO |

#### 4.3.3 字段约束定义

| 字段名 | 是否必填 | 数据类型 | 类型校验规则 | 说明 |
|--------|----------|----------|-------------|------|
| `stkcode` | 是 | string | 非空字符串，匹配正则 `^[0-9]{6}$` | 板块代码，6位数字 |
| `stkname` | 是 | string | 非空字符串 | 板块名称 |
| `std_stkcode` | 是 | string | 非空字符串，匹配正则 `^[0-9]{6}\.[A-Z]{2}$` | 标准板块代码，6位数字.2位大写字母 |
| `zhishubankuaileibie` | 是 | string | 非空字符串 | 指数板块类别 |
| `mst_type` | 是 | string | 枚举值：`INDUSTRY_PLATE_INFO` / `REGION_PLATE_INFO` / `HOTIDEA_PLATE_INFO` | 消息类型 |
| `send_date` | 是 | string | 非空字符串，匹配正则 `^\d{4}-\d{2}-\d{2}$` 或 `^\d{8}$` | 发送日期 |
| `send_time` | 否 | string | 字符串类型 | 发送时间 |
| `sum` | 否 | number | 数值类型（int/float），≥ 0 | 成分股数量 |  #如果线上表没有就不判断，有这判断这一情况；

#### 4.3.5 过程数据存储

| 存储位置 | MySQL `segger_db` |
|----------|-------------------|
| **检查结果表** | `dqm_check_result`（记录异常字段、异常值、所属记录标识） |
| **明细表** | `dqm_accuracy_detail`（记录每条异常记录的详细信息） |
| **留存策略** | 检查结果保留 90 天；明细数据保留 15 天 |


### 4.4 监控项 M4：ads_fin_index_compn_stock_interface_ds 完整性检查
#### 4.4.1 监控目标

验证 mysql `ads_fin_index_compn_stock_interface_ds` 中的 `compn_stock_code`（成分股代码）与 Pulsar topic `security_info` 解码后得到的临时表“dqm_security_info_snapshot” 中的`compn_stock_code` 完全一致。

#### 4.4.2 监控规则

| 属性 | 值 |
|------|-----|
| **监控对象** | mysql `ads_fin_index_compn_stock_interface_ds` 的 `compn_stock_code` 字段 vs 临时表“dqm_security_info_snapshot”的 `compn_stock_code` |
| **监控维度** | 完整性 |
| **监控时间** | 每天 02:00、08:42、08:58、09:04（09:03 晚1分钟） |
| **监控频率** | 每个时间点执行一次，共 4 次 |
| **比对方式** | 按 `stkcode`（板块代码）分组后，对每组内的 `compn_stock_code` 集合做等价比对 |
| **告警条件** | ① 临时表有但线上表无 → 告警"成分股遗漏" ② 线上表有但临时表无 → 告警"成分股多余" |
| **告警级别** | ERROR |
| **通过级别** | INFO |

#### 4.4.5 过程数据存储

| 存储位置 | MySQL `segger_db` |
|----------|-------------------|
| **临时快照表** | `dqm_security_info_snapshot` |
| **检查结果表** | `dqm_check_result` |
| **留存策略** | 每天仅保留最新一份快照；快照数据保留 15 天，过期自动删除 |
| **清理方式** | 每次检查前，删除 `check_date < (current_date - 15天)` 的快照记录 |

---

### 4.5 监控项 M5：ads_fin_index_compn_stock_interface_ds 及时性检查
#### 4.5.1 监控目标

验证 mysql `ads_fin_index_compn_stock_interface_ds` 表在每天关键时间点存在 `send_date = 当天日期` 的 `PLATE_STOCKS` 类型数据。

#### 4.5.2 监控规则

| 属性 | 值 |
|------|-----|
| **监控对象** | Mysql `ads_fin_index_compn_stock_interface_ds` 中 `send_date` 字段 |
| **监控维度** | 及时性 |
| **监控时间** | 每天 02:00、08:42、08:58、09:04 |
| **监控频率** | 每个时间点执行一次，共 4 次 |
| **检查条件** | 查询 `ads_fin_index_compn_stock_interface_ds` 中是否存在 `send_date = current_date` 的记录 |
| **告警条件** | 查询结果为空 → 告警"PLATE_STOCKS 数据未及时到达" |
| **告警级别** | ERROR |
| **通过级别** | INFO |

#### 4.5.4 过程数据存储

同 M2，写入 `dqm_check_result` 表，留存策略 90 天。


### 4.6 监控项 M6：ads_fin_index_compn_stock_interface_ds 准确性检查
#### 4.6.1 监控目标

验证 mysql `ads_fin_index_compn_stock_interface_ds` 表中关键字段数据非空且满足字段类型约束。

#### 4.6.2 监控规则

| 属性 | 值 |
|------|-----|
| **监控对象** | mysql `ads_fin_index_compn_stock_interface_ds` |
| **监控维度** | 准确性（非空 + 类型） |
| **监控时间** | 每天 02:00、08:42、08:58、09:04 |
| **监控频率** | 每个时间点执行一次，共 4 次 |
| **告警条件** | ① 任一必填字段为空 → 告警 ② 任一字段值不满足预定义类型 → 告警 |
| **告警级别** | ERROR |
| **通过级别** | INFO |

#### 4.6.3 字段约束定义

| 字段名 | 是否必填 | 数据类型 | 类型校验规则 | 说明 |
|--------|----------|----------|-------------|------|
| `stkcode` | 是 | string | 非空字符串 | 板块代码 |
| `compn_stock_code` | 是 | string | 非空字符串 | 成分股代码 |
| `compn_stock_name` | 是 | string | 非空字符串 | 成分股简称 |
| `index_name` | 是 | string | 非空字符串 | 指数简称（板块名称） |
| `send_date` | 是 | string | 非空字符串，匹配日期格式 | 交易日期/发送日期 |
| `compn_stock_thscode` | 否 | string | 字符串类型 | 成分股同花顺代码 |
| `valid_from` | 否 | number | 数值类型 | 生效起始时间戳 |
| `valid_to` | 否 | number | 数值类型 | 生效结束时间戳 |
| `timestamp` | 否 | number | 数值类型 | 记录时间戳 |


#### 4.6.5 过程数据存储

同 M3，写入 `dqm_check_result` 和 `dqm_accuracy_detail` 表，留存策略 90/15 天。

## 五、监控调度总览

### 5.1 调度时间表

| 监控项 | 调度时间点 | 频率 | 涉及数据源 |
|--------|-----------|------|-----------|
| M1 完整性 - gmdb_plate_info | 09:00, 09:09, 09:18 | 每 9min × 3 | mysql + Pulsar |
| M2 及时性 - gmdb_plate_info | 09:00, 09:09, 09:18 | 每 9min × 3 | mysql |
| M3 准确性 - gmdb_plate_info | 09:00, 09:09, 09:18 | 每 9min × 3 | mysql |
| M4 完整性 - ads_fin_index_compn_stock_interface_ds | 02:00, 08:42, 08:58, 09:04 | 4 次/天 | mysql |
| M5 及时性 - ads_fin_index_compn_stock_interface_ds | 02:00, 08:42, 08:58, 09:04 | 4 次/天 | mysql |
| M6 准确性 - ads_fin_index_compn_stock_interface_ds | 02:00, 08:42, 08:58, 09:04 | 4 次/天 | mysql |


## 七、日志规范

### 7.1 日志格式

```
[{timestamp}] [{level}] [{monitor_id}] [{dimension}] {message} | detail={json_detail}
```

### 7.2 日志级别

| 级别 | 使用场景 |
|------|----------|
| `INFO` | 检查通过、任务开始/结束 |
| `WARNING` | 非关键异常（如快照采集部分消息超时） |
| `ERROR` | 检查不通过（数据遗漏/多余/缺失/字段异常） |
| `CRITICAL` | 系统级异常（数据库连接失败、Pulsar 不可达等） |

### 7.3 日志输出示例

```
# 检查开始
[2026-04-17 09:00:00] [INFO] [M1] [完整性] 开始完整性检查: gmdb_plate_info vs Pulsar快照 | check_date=2026-04-17, check_round=1

# 采集快照
[2026-04-17 09:00:05] [INFO] [M1] [完整性] Pulsar快照采集完成 | received=156, filtered=142, timeout=false

# 比对通过
[2026-04-17 09:00:06] [INFO] [M1] [完整性] gmdb_plate_info 板块代码完全一致 | online=142, snapshot=142, missing=0, extra=0

# 比对告警
[2026-04-17 09:00:06] [ERROR] [M1] [完整性] gmdb_plate_info 板块代码不一致 | online=140, snapshot=142, missing=["993305","993306"], extra=[]

# 及时性通过
[2026-04-17 09:00:07] [INFO] [M2] [及时性] gmdb_plate_info 当天数据已到达 | mst_type=INDUSTRY_PLATE_INFO, count=85

# 及时性告警
[2026-04-17 09:00:07] [ERROR] [M2] [及时性] gmdb_plate_info 无当天数据 | mst_type=INDUSTRY_PLATE_INFO, send_date=2026-04-17

# 准确性通过
[2026-04-17 09:00:08] [INFO] [M3] [准确性] gmdb_plate_info 准确性检查通过 | total=142, errors=0

# 准确性告警
[2026-04-17 09:00:08] [ERROR] [M3] [准确性] gmdb_plate_info 数据异常 | total=142, errors=3, details=[{"stkcode":"993305","field":"std_stkcode","error":"TYPE_MISMATCH","value":""}]

# 系统异常
[2026-04-17 09:00:00] [CRITICAL] [SYSTEM] MongoDB连接失败 | host=47.101.37.151:28018, error=ConnectionRefused
```

---

## 八、Pulsar 消息监听设计

### 8.1 独立 Consumer 设计

为避免影响主业务消费流程，监控系统使用**独立的 Pulsar Consumer**：

| 配置项 | 值 | 说明 |
|--------|-----|------|
| 订阅名 | `data-quality-monitor-sub` | 独立订阅 |
| 订阅类型 | `Exclusive` | 排他模式，仅监控服务消费 |
| 初始位置 | `Latest` | 不回溯历史，只消费最新消息 |
| 接收超时 | `5000ms` | 每次轮询 5 秒 |
| 采集窗口 | `30s` | 每个检查时间点监听 30 秒后关闭 |
| Topic | `persistent://public/default/security_info` | 业务 Topic |

### 8.2 消息过滤与解码

| 步骤 | 说明 |
|------|------|
| 1. 接收消息 | 从 Pulsar Consumer 接收原始消息 |
| 2. 判断格式 | 根据 topic 判断消息为 JSON 格式 |
| 3. 解码 JSON | `json.loads(msg.data())` |
| 4. 提取字段 |  提取 `stkcode` 或 `compn_stock_code` |
| 5. 写入快照 | 将提取的数据写入 MySQL 临时快照表 |

### 8.3 采集窗口策略

由于 Pulsar 消息是持续推送的，监控系统在固定时间点采集时采用**时间窗口**策略：

```
检查时间点 T:
  T+0s    → 创建 Consumer，开始监听
  T+30s   → 关闭 Consumer，停止监听
  T+30s   → 比对线上表 vs 临时快照
  T+35s   → 输出检查结果
```

> **注意**：30 秒窗口可能无法覆盖所有消息。对于完整性检查，如果线上表数据量大于快照数据量，可能是窗口不足导致，此时应结合及时性检查一起判断。

---



## 九、异常处理与容错

### 9.1 异常场景与处理策略

| 异常场景 | 影响范围 | 处理策略 |
|----------|----------|----------|
| MongoDB 连接失败 | 所有检查项 | CRITICAL 日志 → 跳过本轮检查 → 记录到 dqm_check_result（result=ERROR） |
| MySQL 连接失败 | 过程存储 | CRITICAL 日志 → 检查仍执行（仅日志输出，不存储过程数据） |
| Pulsar 连接失败 | M1、M4（完整性） | WARNING 日志 → 跳过完整性比对 → 记录到 dqm_check_result（result=SKIP） |
| Pulsar 采集窗口内无消息 | M1、M4（完整性） | WARNING 日志 → 无法比对 → 记录到 dqm_check_result（result=NODATA） |
| 检查超时（单轮 > 60s） | 当前检查项 | WARNING 日志 → 终止当前检查 → 继续下一项 |
| 数据量异常大（> 10万条） | M3、M6（准确性） | 采样检查（随机抽取 10% 记录） → 日志标注"采样模式" |

### 9.2 重试机制

| 场景 | 重试次数 | 重试间隔 |
|------|----------|----------|
| Mysql 查询失败 | 3 次 | 5 秒 |
| MySQL 写入失败 | 3 次 | 2 秒 |
| Pulsar Consumer 创建失败 | 2 次 | 10 秒 |

### 9.3 幂等性保障

- 同一 `check_date` + `check_round` + `monitor_id` 的检查结果，若已存在则**覆盖更新**
- 同一 `check_date` 的临时快照，同 `check_round` 的数据先删后插

---




## 验收标准

| 序号 | 验收项 | 验收标准 |
|------|--------|----------|
| 1 | 完整性检查 - 正常场景 | 线上表与临时表代码完全一致时，输出 INFO 日志，dqm_check_result 记录 PASS |
| 2 | 完整性检查 - 异常场景 | 代码不一致时，输出 ERROR 日志（含遗漏/多余详情），dqm_check_result 记录 FAIL |
| 3 | 及时性检查 - 正常场景 | 存在当天数据时，输出 INFO 日志，记录各 mst_type 的记录数 |
| 4 | 及时性检查 - 异常场景 | 无当天数据时，输出 ERROR 日志，明确标注缺失的 mst_type |
| 5 | 准确性检查 - 正常场景 | 所有字段非空且类型正确时，输出 INFO 日志 |
| 6 | 准确性检查 - 异常场景 | 字段为空或类型不匹配时，输出 ERROR 日志，dqm_accuracy_detail 记录异常详情 |
| 7 | 过程数据存储 | 每次检查的结果写入 MySQL 对应表，数据完整可查 |
| 8 | 数据清理 | 超过保留天数的快照/明细数据自动删除，结果数据保留 90 天 |
| 9 | 临时快照覆盖 | 同一天同 check_round 的快照数据覆盖前一次 |
| 10 | 调度准确性 | 各监控项在指定时间点准时触发，误差 < 10 秒 |
| 11 | 容错性 | MongoDB/Pulsar/MySQL 任一组件不可用时，输出 CRITICAL/WARNING 日志，不导致程序崩溃 |
| 12 | 幂等性 | 同一轮检查重复执行不产生重复告警或重复数据 |
| 13 | 性能 | 单轮检查耗时 < 60 秒（10 万条以内数据量） |
| 14 | 日志格式 | 所有日志遵循第七章定义的统一格式 |