package com.shpp.mentoring.okushin.task4;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CsvImporter {


    public void importToDB(String jdbcURL, String username, String password, String csvFilePath, String tableName) {

        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {

            // Создание объекта чтения CSV-файла
            CSVReader reader = new CSVReader(new FileReader(csvFilePath));

            String[] nextLine;

            // Первая строка CSV-файла - заголовок
            String[] header = reader.readNext();

            // Формируем запрос на вставку данных в таблицу
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

            // Создание Prepared Statement для выполнения запроса
            PreparedStatement statement = connection.prepareStatement(sql.toString());

            // Чтение остальных строк CSV-файла и вставка данных в таблицу
            while ((nextLine = reader.readNext()) != null) {
                for (int i = 0; i < header.length; i++) {
                    statement.setString(i + 1, nextLine[i]);
                }
                statement.executeUpdate();
            }

            System.out.println("Данные успешно импортированы!");
            reader.close();

        } catch (SQLException e) {
            System.out.println("Ошибка SQL: " + e.getMessage());
        } catch (FileNotFoundException e) {
            System.out.println("Файл не найден: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Ошибка ввода/вывода: " + e.getMessage());
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

    }
}
