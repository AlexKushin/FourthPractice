package com.shpp.mentoring.okushin.task4;

import com.shpp.mentoring.okushin.exceptions.CreateStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class GenerateThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GenerateThread.class);
    ProductGenerator productGenerator;
    int amount;



    private final Connection connection;

    private final int typesCount;

    public GenerateThread(ProductGenerator productGenerator, int amount, Connection connection, int typesCount) {
        this.productGenerator = productGenerator;
        this.amount = amount;
        this.connection = connection;
        this.typesCount = typesCount;
    }


    @Override
    public void run() {
        logger.info("Thread starts");
        try {
            connection.setAutoCommit(false);
            productGenerator.insertValidatedProducts(connection, amount, typesCount);
            connection.commit();
            connection.close();

        } catch (SQLException e) {
            logger.error("Error executing Sql command: {}", e.getMessage(), e);
            throw new CreateStatementException("Exception while creating statement");
        }
    }
}