package com.starrocks.partial.failure;

import com.starrocks.partial.CleanupStatus;
import com.starrocks.partial.TabletStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MySQLTabletFailureRepository implements TabletFailureRepository {
    private static final Logger LOG = LogManager.getLogger(MySQLTabletFailureRepository.class);

    private static final String TABLE_NAME = "tablet_failures";

    private String jdbcUrl;
    private String user;
    private String password;
    private Connection connection;

    public MySQLTabletFailureRepository(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        init();
    }

    private void init() {
        try {
            connect();
            createTableIfNotExists();
        } catch (SQLException e) {
            LOG.error("Failed to initialize MySQLTabletFailureRepository", e);
            throw new RuntimeException(e);
        }
    }

    private void connect() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(jdbcUrl, user, password);
            } catch (ClassNotFoundException e) {
                throw new SQLException("MySQL JDBC driver not found", e);
            }
        }
    }

    private void createTableIfNotExists() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
                + "tablet_id BIGINT PRIMARY KEY, "
                + "backend_id BIGINT NOT NULL, "
                + "partition_id BIGINT NOT NULL, "
                + "table_id BIGINT NOT NULL, "
                + "fail_time BIGINT NOT NULL, "
                + "recovery_time BIGINT, "
                + "status VARCHAR(255), "
                + "temp_tablet_id BIGINT, "
                + "cleanup_status VARCHAR(255), "
                + "cleanup_time BIGINT, "
                + "cleanup_error TEXT"
                + ")";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    @Override
    public void save(TabletFailure tabletFailure) {
        String sql = "REPLACE INTO " + TABLE_NAME + " (tablet_id, backend_id, partition_id, table_id, fail_time, "
                + "recovery_time, status, temp_tablet_id, cleanup_status, cleanup_time, cleanup_error) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, tabletFailure.getTabletId());
            pstmt.setLong(2, tabletFailure.getBackendId());
            pstmt.setLong(3, tabletFailure.getPartitionId());
            pstmt.setLong(4, tabletFailure.getTableId());
            pstmt.setLong(5, tabletFailure.getFailTime());
            pstmt.setObject(6, tabletFailure.getRecoveryTime());
            pstmt.setString(7, tabletFailure.getStatus() != null ? tabletFailure.getStatus().name() : null);
            pstmt.setObject(8, tabletFailure.getTempTabletId());
            pstmt.setString(9, tabletFailure.getCleanupStatus() != null ? tabletFailure.getCleanupStatus().name() : null);
            pstmt.setObject(10, tabletFailure.getCleanupTime());
            pstmt.setString(11, tabletFailure.getCleanupError());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Failed to save tablet failure for tabletId: {}", tabletFailure.getTabletId(), e);
        }
    }

    @Override
    public TabletFailure findByTabletId(long tabletId) {
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE tablet_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, tabletId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToTabletFailure(rs);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to find tablet failure for tabletId: {}", tabletId, e);
        }
        return null;
    }

    @Override
    public void deleteByTabletId(long tabletId) {
        String sql = "DELETE FROM " + TABLE_NAME + " WHERE tablet_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, tabletId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Failed to delete tablet failure for tabletId: {}", tabletId, e);
        }
    }

    @Override
    public List<TabletFailure> getTabletsByStatus(TabletStatus status) {
        List<TabletFailure> failures = new ArrayList<>();
        String sql = "SELECT * FROM " + TABLE_NAME;
        if (status != null) {
            sql += " WHERE status = ?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (status != null) {
                pstmt.setString(1, status.name());
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    failures.add(mapResultSetToTabletFailure(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to get tablets by status: {}", status, e);
        }
        return failures;
    }

    @Override
    public List<TabletFailure> getCompletedMergeTablets(long beforeTime) {
        List<TabletFailure> failures = new ArrayList<>();
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE status = ? AND recovery_time < ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, TabletStatus.RECOVERED.name());
            pstmt.setLong(2, beforeTime);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    failures.add(mapResultSetToTabletFailure(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to get completed merge tablets before: {}", beforeTime, e);
        }
        return failures;
    }

    @Override
    public void markTempTabletCleaned(long tabletId) {
        String sql = "UPDATE " + TABLE_NAME + " SET cleanup_status = ?, cleanup_time = ? WHERE tablet_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, CleanupStatus.DONE.name());
            pstmt.setLong(2, System.currentTimeMillis());
            pstmt.setLong(3, tabletId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Failed to mark temp tablet cleaned for tabletId: {}", tabletId, e);
        }
    }

    @Override
    public void markCleanupFailed(long tabletId, String error) {
        String sql = "UPDATE " + TABLE_NAME + " SET cleanup_status = ?, cleanup_error = ? WHERE tablet_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, CleanupStatus.FAILED.name());
            pstmt.setString(2, error);
            pstmt.setLong(3, tabletId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Failed to mark cleanup failed for tabletId: {}", tabletId, e);
        }
    }

    @Override
    public int getPendingCleanupCount() {
        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE cleanup_status = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, CleanupStatus.PENDING.name());
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to get pending cleanup count", e);
        }
        return 0;
    }

    private TabletFailure mapResultSetToTabletFailure(ResultSet rs) throws SQLException {
        TabletFailure failure = new TabletFailure();
        failure.setTabletId(rs.getLong("tablet_id"));
        failure.setBackendId(rs.getLong("backend_id"));
        failure.setPartitionId(rs.getLong("partition_id"));
        failure.setTableId(rs.getLong("table_id"));
        failure.setFailTime(rs.getLong("fail_time"));
        failure.setRecoveryTime(rs.getObject("recovery_time", Long.class));
        String statusStr = rs.getString("status");
        if (statusStr != null) {
            failure.setStatus(TabletStatus.valueOf(statusStr));
        }
        failure.setTempTabletId(rs.getObject("temp_tablet_id", Long.class));
        String cleanupStatusStr = rs.getString("cleanup_status");
        if (cleanupStatusStr != null) {
            failure.setCleanupStatus(CleanupStatus.valueOf(cleanupStatusStr));
        }
        failure.setCleanupTime(rs.getObject("cleanup_time", Long.class));
        failure.setCleanupError(rs.getString("cleanup_error"));
        return failure;
    }
}
