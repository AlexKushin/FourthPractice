package com.shpp.mentoring.okushin.task4;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;


public class SqlExecute {
    private static final Logger logger = LoggerFactory.getLogger(SqlExecute.class);

    public static void executeSqlScript(Connection connection, String sqlFilePath) throws SQLException {

        try (Statement stmt = connection.createStatement()) {
            String[] commands = readSqlCommandFromFile(sqlFilePath).split(";");
            for (String command : commands) {
                stmt.execute(command);
            }
            connection.close();
            logger.info("DDL commands executed successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeSqlCommand(Connection connection, String sqlCommand) throws SQLException {
        try (Statement stmt = connection.createStatement()) {

            stmt.executeUpdate(sqlCommand);
            connection.close();
            logger.info("DDL commands executed successfully");
        }
    }

    public static void executeQuerySqlScript(Connection connection, String sqlFilePath, String productType) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(readSqlCommandFromFile(sqlFilePath))) {
            stmt.setString(1, productType.toLowerCase());
            StopWatch watch = new StopWatch();
            watch.start();
            ResultSet res = stmt.executeQuery();
            watch.stop();
            logger.info("2Searching store speed = {}", watch.getTime() / 1000.0);
            watch.reset();
            ResultSetMetaData resultSetMetaData = res.getMetaData();
            while (res.next()) {
                for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                    String result = res.getString(i);
                    logger.info("{}: {}", resultSetMetaData.getColumnName(i), result);
                }
            }
            connection.close();
            logger.info("DDL commands executed successfully");
        } catch (Exception e) {
            logger.error("Error executing DDL commands: {}", e.getMessage());
        }
    }

    public static int executeQuerySqlScript(Connection connection, String query) throws SQLException {
        try (
                Statement statement = connection.createStatement()) {
            ResultSet res = statement.executeQuery(query);
            int count = 0;
            while (res.next()) {
                count = res.getInt(1);
            }
            connection.close();
            return count;
        }
    }

    private static String readSqlCommandFromFile(String sqlFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(sqlFilePath))) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append(" ");
            }
            return sb.toString();
        }
    }
}
