"""数据库表定义：DDL 语句。"""

# dqm_security_info_snapshot 临时快照表
CREATE_SNAPSHOT_TABLE = """
CREATE TABLE IF NOT EXISTS `dqm_security_info_snapshot` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `check_date` DATE NOT NULL,
    `check_round` INT NOT NULL DEFAULT 1,
    `check_time` DATETIME NOT NULL,
    `stkcode` VARCHAR(20) DEFAULT NULL,
    `stkname` VARCHAR(100) DEFAULT NULL,
    `std_stkcode` VARCHAR(30) DEFAULT NULL,
    `mst_type` VARCHAR(50) DEFAULT NULL,
    `compn_stock_code` VARCHAR(20) DEFAULT NULL,
    `compn_stock_name` VARCHAR(100) DEFAULT NULL,
    `index_name` VARCHAR(100) DEFAULT NULL,
    `send_date` VARCHAR(20) DEFAULT NULL,
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_check_date_round` (`check_date`, `check_round`),
    INDEX `idx_stkcode` (`stkcode`),
    INDEX `idx_compn_stock_code` (`compn_stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量监控-临时快照表';
"""

# dqm_check_result 检查结果表
CREATE_CHECK_RESULT_TABLE = """
CREATE TABLE IF NOT EXISTS `dqm_check_result` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `check_date` DATE NOT NULL,
    `check_round` INT NOT NULL DEFAULT 1,
    `check_time` DATETIME NOT NULL,
    `monitor_id` VARCHAR(10) NOT NULL COMMENT '监控项ID: M1-M6',
    `dimension` VARCHAR(20) NOT NULL COMMENT '维度: 完整性/及时性/准确性',
    `result` VARCHAR(10) NOT NULL COMMENT 'PASS/FAIL/ERROR/SKIP/NODATA',
    `detail` TEXT DEFAULT NULL COMMENT '检查详情(JSON)',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_check` (`check_date`, `check_round`, `monitor_id`),
    INDEX `idx_result` (`result`),
    INDEX `idx_check_date` (`check_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量监控-检查结果表';
"""

# dqm_accuracy_detail 准确性明细表
CREATE_ACCURACY_DETAIL_TABLE = """
CREATE TABLE IF NOT EXISTS `dqm_accuracy_detail` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `check_date` DATE NOT NULL,
    `check_round` INT NOT NULL DEFAULT 1,
    `monitor_id` VARCHAR(10) NOT NULL,
    `record_key` VARCHAR(100) DEFAULT NULL COMMENT '异常记录标识(stkcode等)',
    `field_name` VARCHAR(50) NOT NULL COMMENT '异常字段名',
    `error_type` VARCHAR(30) NOT NULL COMMENT 'NULL_VALUE/TYPE_MISMATCH/ENUM_INVALID/REGEX_MISMATCH',
    `error_value` TEXT DEFAULT NULL COMMENT '异常值',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_detail` (`check_date`, `check_round`, `monitor_id`, `record_key`, `field_name`),
    INDEX `idx_check_date` (`check_date`),
    INDEX `idx_monitor_id` (`monitor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量监控-准确性明细表';
"""

ALL_DDL = [CREATE_SNAPSHOT_TABLE, CREATE_CHECK_RESULT_TABLE, CREATE_ACCURACY_DETAIL_TABLE]
