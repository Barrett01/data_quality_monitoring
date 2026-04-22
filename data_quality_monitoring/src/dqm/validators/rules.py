"""校验规则定义：定义各表的字段约束规则。"""

from dataclasses import dataclass, field
from typing import Any, List, Optional, Set


@dataclass
class FieldRule:
    """字段校验规则"""

    name: str
    required: bool = True
    data_type: str = "string"
    regex_pattern: Optional[str] = None
    enum_values: Set[str] = field(default_factory=set)
    min_value: Optional[float] = None
    description: str = ""


# gmdb_plate_info 字段约束 (M3)
GMDB_PLATE_INFO_RULES: List[FieldRule] = [
    FieldRule(name="stkcode", required=True, regex_pattern=r"^[0-9]{6}$", description="板块代码，6位数字"),
    FieldRule(name="stkname", required=True, description="板块名称"),
    FieldRule(name="std_stkcode", required=True, regex_pattern=r"^[0-9]{6}\.[A-Z]{2}$", description="标准板块代码"),
    FieldRule(name="zhishubankuaileibie", required=True, description="指数板块类别"),
    FieldRule(
        name="mst_type",
        required=True,
        enum_values={"INDUSTRY_PLATE_INFO", "REGION_PLATE_INFO", "HOTIDEA_PLATE_INFO"},
        description="消息类型",
    ),
    FieldRule(name="send_date", required=True, regex_pattern=r"^\d{4}-\d{2}-\d{2}$|^\d{8}$", description="发送日期"),
    FieldRule(name="send_time", required=False, description="发送时间"),
    FieldRule(name="sum", required=False, data_type="number", min_value=0, description="成分股数量"),
]

# ads_fin_index_compn_stock_interface_ds 字段约束 (M6)
ADS_FIN_INDEX_RULES: List[FieldRule] = [
    FieldRule(name="stkcode", required=True, description="板块代码"),
    FieldRule(name="compn_stock_code", required=True, description="成分股代码"),
    FieldRule(name="compn_stock_name", required=True, description="成分股简称"),
    FieldRule(name="index_name", required=True, description="指数简称（板块名称）"),
    FieldRule(name="send_date", required=True, regex_pattern=r"^\d{4}-\d{2}-\d{2}$|^\d{8}$", description="发送日期"),
    FieldRule(name="compn_stock_thscode", required=False, description="成分股同花顺代码"),
    FieldRule(name="valid_from", required=False, data_type="number", description="生效起始时间戳"),
    FieldRule(name="valid_to", required=False, data_type="number", description="生效结束时间戳"),
    FieldRule(name="timestamp", required=False, data_type="number", description="记录时间戳"),
]
