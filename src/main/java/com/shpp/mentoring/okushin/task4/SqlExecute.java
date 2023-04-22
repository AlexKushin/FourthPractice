package com.shpp.mentoring.okushin.task4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;


public class SqlExecute {
    private static final Logger logger = LoggerFactory.getLogger(SqlExecute.class);
    public static void executeSqlScript(String jdbcURL, String username, String password, String sqlFilePath) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password); Statement stmt = conn.createStatement()) {
            String[] commands = readSqlCommandFromFile(sqlFilePath).split(";");
            for (String command : commands) {
                stmt.execute(command);
            }
            logger.info("DDL commands executed successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeSqlCommand(String jdbcURL, String username, String password, String sqlCommand) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password); Statement stmt = conn.createStatement()) {

            stmt.execute(sqlCommand);

            logger.info("DDL commands executed successfully");
        }
    }

    public static void executeQuerySqlScript(String jdbcURL, String username, String password, String sqlFilePath, String productType) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password); PreparedStatement stmt = conn.prepareStatement(readSqlCommandFromFile(sqlFilePath))) {
            stmt.setString(1, productType.toLowerCase());
            ResultSet res = stmt.executeQuery();
            ResultSetMetaData resultSetMetaData = res.getMetaData();
            while (res.next()) {
                for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                    if (i > 1) {
                        System.out.print(", ");
                    }
                    String result = res.getString(i);
                    System.out.print(result + " " + resultSetMetaData.getColumnName(i));
                }
                System.out.println();
            }
            logger.info("DDL commands executed successfully");
        } catch (Exception e) {
            logger.error("Error executing DDL commands: " + e.getMessage());
        }
    }

    public static int executeQuerySqlScript(String jdbcURL, String username, String password, String query) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password)) {
            Statement statement = conn.createStatement();
            ResultSet res = statement.executeQuery(query);
            int count = 0;
            while (res.next()) {
                count = res.getInt(1);
            }
            return count;
        }
    }

    private static String readSqlCommandFromFile(String sqlFilePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(sqlFilePath));
        String line;
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append(" ");
        }
        br.close();
        return sb.toString();
    }
}