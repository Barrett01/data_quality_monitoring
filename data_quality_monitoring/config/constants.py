"""常量定义：监控项 ID、维度、告警级别等。"""

from enum import Enum


class MonitorID(str, Enum):
    """监控项标识"""

    M1 = "M1"  # 完整性 - gmdb_plate_info
    M2 = "M2"  # 及时性 - gmdb_plate_info
    M3 = "M3"  # 准确性 - gmdb_plate_info
    M4 = "M4"  # 完整性 - ads_fin_index_compn_stock_interface_ds
    M5 = "M5"  # 及时性 - ads_fin_index_compn_stock_interface_ds
    M6 = "M6"  # 准确性 - ads_fin_index_compn_stock_interface_ds


class Dimension(str, Enum):
    """监控维度"""

    COMPLETENESS = "完整性"
    TIMELINESS = "及时性"
    ACCURACY = "准确性"


class CheckResult(str, Enum):
    """检查结果状态"""

    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"  # 系统异常
    SKIP = "SKIP"  # 跳过
    NODATA = "NODATA"  # 无数据


class MstType(str, Enum):
    """消息类型枚举"""

    INDUSTRY_PLATE_INFO = "INDUSTRY_PLATE_INFO"
    REGION_PLATE_INFO = "REGION_PLATE_INFO"
    HOTIDEA_PLATE_INFO = "HOTIDEA_PLATE_INFO"
    PLATE_STOCKS = "PLATE_STOCKS"


# 监控表名
TABLE_GMDB_PLATE_INFO = "gmdb_plate_info"
TABLE_ADS_FIN_INDEX = "ads_fin_index_compn_stock_interface_ds"

# 临时快照表
TABLE_SNAPSHOT = "dqm_security_info_snapshot"

# 检查结果表
TABLE_CHECK_RESULT = "dqm_check_result"

# 准确性明细表
TABLE_ACCURACY_DETAIL = "dqm_accuracy_detail"

# 重试配置
MYSQL_QUERY_RETRY = 3
MYSQL_QUERY_RETRY_INTERVAL = 5
MYSQL_WRITE_RETRY = 3
MYSQL_WRITE_RETRY_INTERVAL = 2
PULSAR_CONNECT_RETRY = 2
PULSAR_CONNECT_RETRY_INTERVAL = 10

# 采样阈值
SAMPLE_THRESHOLD = 100000
SAMPLE_RATIO = 0.1
