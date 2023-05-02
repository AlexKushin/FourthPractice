package com.shpp.mentoring.okushin.task4;

import com.shpp.mentoring.okushin.exceptions.CreateStatementException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ProductGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    private final Validator validator;
    Random random = new Random();


    public ProductGenerator(Validator validator) {
        this.validator = validator;

    }
    public void insertValidatedProducts(Connection connection, int amount, int typesCount) throws SQLException {

        final AtomicInteger totalQuantity = new AtomicInteger(0);
        int prodCountForStatement = 1;
        int batchCount = 0;
        int batchSize = 50;
        StopWatch watch = new StopWatch();
        watch.start();
        String sql = null;
        int numberOfProductsInPS = 0;
        int leftAmount = amount;
        while (leftAmount>0) {
            if (leftAmount <= 10) {
                sql = ("INSERT INTO availability_goods.products (type_id,product_name) VALUES (CAST(? AS INTEGER), ? )");
                numberOfProductsInPS = 1;
            }
            if (leftAmount > 10 && leftAmount <= 100) {
                sql = ("INSERT INTO availability_goods.products (type_id,product_name) VALUES" + " (CAST(? AS INTEGER), ? ), ".repeat(9) +
                        "(CAST(? AS INTEGER), ? )");
                numberOfProductsInPS = 10;
            }
            if (leftAmount >100 && leftAmount <= 1000) {
                sql = ("INSERT INTO availability_goods.products (type_id,product_name) VALUES" + " (CAST(? AS INTEGER), ? ), ".repeat(99) +
                        "(CAST(? AS INTEGER), ? )");
                numberOfProductsInPS = 100;
            }
            if (leftAmount > 1000) {
                sql = "INSERT INTO availability_goods.products (type_id,product_name) VALUES" + " (CAST(? AS INTEGER), ? ), ".repeat(999) +
                        "(CAST(? AS INTEGER), ? )";
                numberOfProductsInPS = 1000;
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                while (prodCountForStatement < (numberOfProductsInPS*2)+1) {
                    Product p = new Product(RandomStringUtils.randomAlphabetic(1, 10), random.nextInt(typesCount + 1));
                    try {
                        if (validator.validate(p).isEmpty()) {
                            statement.setString(prodCountForStatement, String.valueOf(p.getTypeId()));
                            prodCountForStatement++;
                            statement.setString(prodCountForStatement, String.valueOf(p.getName()));
                            prodCountForStatement++;
                            totalQuantity.incrementAndGet();
                            leftAmount--;

                            if (prodCountForStatement > numberOfProductsInPS*2 ) {
                                statement.addBatch();
                                batchCount++;
                            }
                            if (batchCount > 0 && batchCount % batchSize == 0) {
                                statement.executeBatch();
                            }
                            statement.executeBatch();
                        }
                    } catch (SQLException e) {
                        logger.error("Error executing Sql command: {}", e.getMessage(), e);
                        throw new CreateStatementException("Exception while creating statement");
                    }
                }
            }
            prodCountForStatement=1;
        }
        watch.stop();
        double elapsedSeconds = watch.getTime() / 1000.0;
        double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
        logger.info("batchSize = {}", batchSize);
        logger.info("GENERATING SPEED: {} , total = {} messages, elapseSeconds = {}",
                messagesPerSecond, totalQuantity.get(), elapsedSeconds);

    }
}

