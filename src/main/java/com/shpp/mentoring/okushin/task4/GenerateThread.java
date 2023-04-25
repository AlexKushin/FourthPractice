package com.shpp.mentoring.okushin.task4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GenerateThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GenerateThread.class);
    ProductGenerator productGenerator;
    int amount;


    private final String sql;
private final Connection connection;

    private final int typesCount;

    public GenerateThread(ProductGenerator productGenerator, int amount, String sql, Connection connection, int typesCount) {
        this.productGenerator = productGenerator;
        this.amount = amount;
        this.sql = sql;
        this.connection = connection;

        this.typesCount = typesCount;
    }


    @Override
    public void run() {
        try {

            //Connection connection = DriverManager.getConnection(url, user, password);

            PreparedStatement statement = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            productGenerator.insertValidatedProducts(statement, amount, typesCount);
            statement.executeBatch();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}