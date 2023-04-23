package com.shpp.mentoring.okushin.task4;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CsvImporter {

    private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    public static void importToDB(String jdbcURL, String username, String password, String csvFilePath, String tableName) {

        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password);
             CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {

            String[] nextLine;

            String[] header = reader.readNext();


            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(tableName);
            sql.append("(");
            for (int i = 0; i < header.length; i++) {
                sql.append(header[i]);
                if (i != header.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(") VALUES (");
            for (int i = 0; i < header.length; i++) {
                sql.append("?");
                if (i != header.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")");


            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {

                while ((nextLine = reader.readNext()) != null) {
                    for (int i = 0; i < header.length; i++) {
                        statement.setString(i + 1, nextLine[i]);
                    }
                    statement.executeUpdate();
                }
                logger.info("Data was successfully imported");
            }

        } catch (SQLException | CsvValidationException e) {
            logger.error("Error SQL: {} ", e.getMessage());
        } catch (FileNotFoundException e) {
            logger.error("There is no file to read {}", e.getMessage());
        } catch (IOException e) {
            logger.error("Error whole input/output {}", e.getMessage());
        }

    }
}
