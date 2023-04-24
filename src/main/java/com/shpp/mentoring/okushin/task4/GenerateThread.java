package com.shpp.mentoring.okushin.task4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GenerateThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GenerateThread.class);
    ProductGenerator productGenerator;
    int amount;

    private final String password;
    private final String url;
    private final String user;
    private final String sql;

    public GenerateThread(ProductGenerator productGenerator, int amount, String password, String url, String user, String sql) {
        this.productGenerator = productGenerator;
        this.amount = amount;
        this.password = password;
        this.url = url;
        this.user = user;
        this.sql = sql;
    }


    @Override
    public void run() {
        try {
            int typesCount = SqlExecute.executeQuerySqlScript(url, user, password, "SELECT count(*) from availability_goods.types;");
            Connection connection = DriverManager.getConnection(url, user, password);
            PreparedStatement statement = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            productGenerator.insertValidatedProducts(statement, amount,typesCount);
            statement.executeBatch();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}