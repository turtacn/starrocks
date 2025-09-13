-- Table for recording failed tablets
CREATE TABLE IF NOT EXISTS tablet_failure (
    tablet_id BIGINT NOT NULL,
    backend_id BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    fail_time TIMESTAMP NOT NULL,
    recovery_time TIMESTAMP NULL,
    status TINYINT NOT NULL COMMENT '0: FAILED, 1: RECOVERING, 2: RECOVERED',
    temp_table_id BIGINT NULL COMMENT 'Temporary table ID',
    cleanup_status TINYINT DEFAULT 0 COMMENT '0: NOT_CLEANED, 1: CLEANING, 2: CLEANED, 3: FAILED',
    cleanup_time TIMESTAMP NULL,
    cleanup_error VARCHAR(1000) NULL,
    PRIMARY KEY (tablet_id),
    INDEX idx_backend (backend_id),
    INDEX idx_table (table_id)
) ENGINE=OLAP COMMENT 'Records information about failed tablets';
