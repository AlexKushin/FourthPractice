package com.shpp.mentoring.okushin.task4;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
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

    public void insertValidatedProducts(PreparedStatement statement, int amount,int typesCount) {

        final AtomicInteger totalQuantity = new AtomicInteger(0);
        final AtomicInteger prodCountForStatement = new AtomicInteger(1);
        long startTime = System.currentTimeMillis();
        Stream.generate(() -> new Product(RandomStringUtils.randomAlphabetic(1, 10), random.nextInt(typesCount+1), RandomStringUtils.randomAlphabetic(10, 20)))
                .takeWhile(b -> totalQuantity.get() < amount)
                .forEach(product -> {
                    try {
                        int batchCount = 0;
                         if (validator.validate(product).isEmpty()) {

                            statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getTypeId()));
                            statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getName()));
                            totalQuantity.incrementAndGet();

                            if (prodCountForStatement.get() > 2000) {
                                statement.addBatch();
                                batchCount++;
                                prodCountForStatement.set(1);
                            }
                            if (batchCount % 1000 == 0) {
                                statement.executeBatch();
                            }
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        long endTime = System.currentTimeMillis();
        double elapsedSeconds = (endTime - startTime) / 1000.0;
        double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
        logger.info("RECEIVING SPEED: {} messages per second, total = {} messages, elapseSeconds = {}",
                messagesPerSecond, totalQuantity.get(), elapsedSeconds);
    }
}

