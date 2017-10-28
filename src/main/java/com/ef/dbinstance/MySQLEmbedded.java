package com.ef.dbinstance;

import com.ef.constants.ActiveIpColumnNames;
import com.ef.constants.LogColumnNames;
import com.ef.constants.Constant;

import com.ef.constants.Duration;
import com.ef.model.ActiveIP;
import com.ef.utils.FSUtil;
import com.ef.utils.SQLBuilder;
import com.wix.mysql.config.MysqldConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_6_latest;

public class MySQLEmbedded {
    private static MySQLEmbedded mySQLEmbedded = null;

    private MySQLEmbedded() {}

    public static MySQLEmbedded startEbmeddedMySQLServer() {
        if (mySQLEmbedded == null) {
            MysqldConfig config = aMysqldConfig(v5_6_latest)
                    .withCharset(UTF8)
                    .withPort(Integer.valueOf(FSUtil.readPropertyValue("db_port")))
                    .build();

            anEmbeddedMysql(config)
                    .addSchema(FSUtil.readPropertyValue("db_schema"), classPathScript("init.sql"))
                    .start();

            loadMySQLDriver();
            mySQLEmbedded = new MySQLEmbedded();
        }

        return mySQLEmbedded;
    }

    public boolean uploadMainLogToDB(String filePathSource, String tableNameTarget) {
        try(Connection connection = getConnection()) {
            connection.setAutoCommit(false);
            String sqlInsert = SQLBuilder.buildInsertQuery(FSUtil.readPropertyValue("db_schema"), tableNameTarget,
                    Stream.of(LogColumnNames.values()).map(Enum::name).collect(Collectors.toList()));
            System.out.println("Generated sql insert query: " + sqlInsert);
            try (final BufferedReader br = Files.newBufferedReader(Paths.get(filePathSource));
                 final PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert)) {
                String fileRow;
                int counter = 0;
                while ((fileRow = br.readLine()) != null) {
                    String[] rowValue = fileRow.split(Constant.PIPE_ESCAPED);
                    preparedStatement.setTimestamp(1, Timestamp.valueOf(rowValue[0]));
                    preparedStatement.setString(2, rowValue[1]);
                    preparedStatement.setString(3, rowValue[2]);
                    preparedStatement.setInt(4, Integer.valueOf(rowValue[3]));
                    preparedStatement.setString(5, rowValue[4]);

                    preparedStatement.addBatch();
                    counter ++;
                    if (counter == Integer.valueOf(FSUtil.readPropertyValue("batch_size"))) {
                        System.out.println("Please wait... Batch insert executing...");
                        preparedStatement.executeBatch();
                        connection.commit();
                        counter = 0;
                    }
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch(IOException e) {
                throw new RuntimeException(e.getCause());
            }
        } catch (SQLException sqlEx) {
            throw new RuntimeException(sqlEx.getCause());
        }

        return true;
    }

    public void uploadActiveIpToDB(List<ActiveIP> activeIPs, String tableName) {
        try (Connection connection = getConnection()){
            String sqlSt = SQLBuilder.buildInsertQuery(FSUtil.readPropertyValue("db_schema"), tableName,
                    Stream.of(ActiveIpColumnNames.values()).map(Enum::name).collect(Collectors.toList()));
            try (PreparedStatement pSt = connection.prepareStatement(sqlSt)) {
                activeIPs.forEach(activeIP -> {
                    try {
                        pSt.setString(1, activeIP.getIp());
                        pSt.setTimestamp(2, Timestamp.valueOf(activeIP.getStartDate()));
                        pSt.setString(3, activeIP.getDurationType());
                        pSt.setInt(4, activeIP.getReqCount());
                        pSt.execute();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (SQLException sqlEx) {
            throw new RuntimeException(sqlEx.getCause());
        }
    }

    public int countRowsInTable(String tableName) {
        int rowCount = 0;
        try(Connection connection = getConnection()) {
            String sqlSelectCount = SQLBuilder.buildSelectQuery(true, FSUtil.readPropertyValue("db_schema"), tableName,
                    Stream.of(LogColumnNames.values()).map(Enum::name).collect(Collectors.toList()));
            System.out.println("Generated select count sql is " + sqlSelectCount);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlSelectCount)) {
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet != null && resultSet.next()) {
                    rowCount = Math.toIntExact(resultSet.getLong(1));
                }
            }
        } catch (SQLException sqlEx) {
            throw new RuntimeException(sqlEx.getCause());
        }

        return rowCount;
    }

    public List<ActiveIP> findActiveIPForPeriod(LocalDateTime startDate, Duration duration, int reqCount) {
        List<ActiveIP> activeIPs = new ArrayList<>();
        LocalDateTime endDate;
        try(Connection connection = getConnection()) {
            switch (duration) {
                case DAILY:
                    endDate = startDate.plusDays(1);
                    break;

                case HOURLY:
                    endDate = startDate.plusHours(1);
                    break;

                default:
                    throw new IllegalArgumentException("Please check duration parameter");
            }
            String sqlStm = "SELECT IP, count(IP) from MAIN_LOG where START_DATE between ? and ? GROUP BY IP HAVING count(IP) >= ?";
            PreparedStatement pSt = connection.prepareStatement(sqlStm);
            pSt.setTimestamp(1, Timestamp.valueOf(startDate));
            pSt.setTimestamp(2, Timestamp.valueOf(endDate));
            pSt.setInt(3, reqCount);
            ResultSet resultSet = pSt.executeQuery();
            if (resultSet != null) {
                System.out.println("Find next IP's:");
                while (resultSet.next()) {
                    ActiveIP activeIP = new ActiveIP();
                    activeIP.setIp(resultSet.getString(1));
                    activeIP.setReqCount(resultSet.getInt(2));
                    activeIP.setStartDate(startDate);
                    activeIP.setDurationType(duration.name());
                    System.out.println(activeIP);
                    activeIPs.add(activeIP);
                }
            }
        } catch (SQLException sqlEx) {
            throw new RuntimeException(sqlEx.getCause());
        }

        return activeIPs;
    }

    private static void loadMySQLDriver() {
        try {
            Class.forName(FSUtil.readPropertyValue("jdbc_driver"));
        } catch (ClassNotFoundException cnf) {
            throw new RuntimeException(cnf.getCause());
        }
    }

    private Connection getConnection() throws SQLException {
            return DriverManager.getConnection(FSUtil.readPropertyValue("db_url"),
                    FSUtil.readPropertyValue("db_user"),
                    FSUtil.readPropertyValue("db_pass"));
    }
}