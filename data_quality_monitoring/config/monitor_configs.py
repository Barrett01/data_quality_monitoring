"""各监控项配置定义。"""

from config.constants import Dimension, MonitorID, TABLE_GMDB_PLATE_INFO, TABLE_ADS_FIN_INDEX


# M1: 完整性 - gmdb_plate_info
M1_CONFIG = {
    "monitor_id": MonitorID.M1,
    "name": "gmdb_plate_info 完整性检查",
    "dimension": Dimension.COMPLETENESS,
    "table": TABLE_GMDB_PLATE_INFO,
    "key_field": "stkcode",
    "cron_times": ["09:00", "09:09", "09:18"],
    "needs_pulsar": True,
}

# M2: 及时性 - gmdb_plate_info
M2_CONFIG = {
    "monitor_id": MonitorID.M2,
    "name": "gmdb_plate_info 及时性检查",
    "dimension": Dimension.TIMELINESS,
    "table": TABLE_GMDB_PLATE_INFO,
    "key_field": "send_date",
    "cron_times": ["09:00", "09:09", "09:18"],
    "needs_pulsar": False,
}

# M3: 准确性 - gmdb_plate_info
M3_CONFIG = {
    "monitor_id": MonitorID.M3,
    "name": "gmdb_plate_info 准确性检查",
    "dimension": Dimension.ACCURACY,
    "table": TABLE_GMDB_PLATE_INFO,
    "key_field": "stkcode",
    "cron_times": ["09:00", "09:09", "09:18"],
    "needs_pulsar": False,
}

# M4: 完整性 - ads_fin_index_compn_stock_interface_ds
M4_CONFIG = {
    "monitor_id": MonitorID.M4,
    "name": "ads_fin_index_compn_stock_interface_ds 完整性检查",
    "dimension": Dimension.COMPLETENESS,
    "table": TABLE_ADS_FIN_INDEX,
    "key_field": "compn_stock_code",
    "group_field": "stkcode",  # 按 stkcode 分组后比对 compn_stock_code
    "cron_times": ["02:00", "08:42", "08:58", "09:04"],
    "needs_pulsar": True,
}

# M5: 及时性 - ads_fin_index_compn_stock_interface_ds
M5_CONFIG = {
    "monitor_id": MonitorID.M5,
    "name": "ads_fin_index_compn_stock_interface_ds 及时性检查",
    "dimension": Dimension.TIMELINESS,
    "table": TABLE_ADS_FIN_INDEX,
    "key_field": "send_date",
    "cron_times": ["02:00", "08:42", "08:58", "09:04"],
    "needs_pulsar": False,
}

# M6: 准确性 - ads_fin_index_compn_stock_interface_ds
M6_CONFIG = {
    "monitor_id": MonitorID.M6,
    "name": "ads_fin_index_compn_stock_interface_ds 准确性检查",
    "dimension": Dimension.ACCURACY,
    "table": TABLE_ADS_FIN_INDEX,
    "key_field": "compn_stock_code",
    "cron_times": ["02:00", "08:42", "08:58", "09:04"],
    "needs_pulsar": False,
}

ALL_CONFIGS = {MonitorID.M1: M1_CONFIG, MonitorID.M2: M2_CONFIG, MonitorID.M3: M3_CONFIG,
               MonitorID.M4: M4_CONFIG, MonitorID.M5: M5_CONFIG, MonitorID.M6: M6_CONFIG}
