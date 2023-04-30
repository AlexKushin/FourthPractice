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
import java.util.stream.Stream;

public class ProductGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    private final Validator validator;
    Random random = new Random();


    public ProductGenerator(Validator validator) {
        this.validator = validator;

    }

    public void insertValidatedProducts(Connection connection, int amount, int typesCount, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            final AtomicInteger totalQuantity = new AtomicInteger(0);
            final AtomicInteger prodCountForStatement = new AtomicInteger(1);
            final AtomicInteger batchCount = new AtomicInteger(0);

            StopWatch watch = new StopWatch();
            watch.start();
            Stream.generate(() -> new Product(RandomStringUtils.randomAlphabetic(1, 10), random.nextInt(typesCount + 1)))
                    .takeWhile(b -> totalQuantity.get() < amount)
                    .forEach(product -> {
                        try {
                            if (validator.validate(product).isEmpty()) {
                                statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getTypeId()));
                                statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getName()));
                                totalQuantity.incrementAndGet();
                                if (prodCountForStatement.get() > 2000) {
                                    statement.addBatch();
                                    batchCount.incrementAndGet();
                                    prodCountForStatement.set(1);
                                }
                                int batchSize = 100;
                                logger.info("batchSize = {}", batchSize);
                                if (batchCount.get() > 0 && batchCount.get() % batchSize == 0) {
                                    statement.executeBatch();
                                }

                            }
                        } catch (SQLException e) {
                            logger.error("Error executing Sql command: {}", e.getMessage(), e);
                            throw new CreateStatementException("Exception while creating statement");
                        }
                    });
            watch.stop();
            double elapsedSeconds = watch.getTime() / 1000.0;
            double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
            logger.info("GENERATING SPEED: {} , total = {} messages, elapseSeconds = {}",
                    messagesPerSecond, totalQuantity.get(), elapsedSeconds);
        }
    }
}

