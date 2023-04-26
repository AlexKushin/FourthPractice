package com.shpp.mentoring.okushin.task4;

import org.apache.commons.lang3.RandomStringUtils;
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

  //  public void insertValidatedProducts(PreparedStatement statement, int amount,int typesCount) {
        public void insertValidatedProducts(Connection connection, int amount, int typesCount,String sql) throws SQLException {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                final AtomicInteger totalQuantity = new AtomicInteger(0);
                final AtomicInteger prodCountForStatement = new AtomicInteger(1);
                final AtomicInteger batchCount = new AtomicInteger(0);
                long startTime = System.currentTimeMillis();
                Stream.generate(() -> new Product(RandomStringUtils.randomAlphabetic(1, 10), random.nextInt(typesCount + 1)))
                        .takeWhile(b -> totalQuantity.get() < amount)
                        .forEach(product -> {
                            try {
                                // int batchCount = 0;
                                if (validator.validate(product).isEmpty()) {

                                    statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getTypeId()));
                                    statement.setString(prodCountForStatement.getAndIncrement(), String.valueOf(product.getName()));
                                    totalQuantity.incrementAndGet();
                                    // System.out.println("1prodCountForStatement= "+prodCountForStatement.get());
                                    if (prodCountForStatement.get() > 2000) {
                                        statement.addBatch();
                                        batchCount.incrementAndGet();
                                       // System.out.println("1batchCount= " + batchCount);
                                       // System.out.println("2prodCountForStatement= " + prodCountForStatement.get());
                                        prodCountForStatement.set(1);
                                    }
                                    if (batchCount.get() >0 && batchCount.get() % 100 == 0) {
                                     //   System.out.println("2batchCount= " + batchCount);
                                        //  System.out.println("1000 prepStatements in batch, 1000 products in every statement");
                                        statement.executeBatch();
                                     //   connection.commit();
                                    }

                                }
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
                long endTime = System.currentTimeMillis();
                double elapsedSeconds = (endTime - startTime) / 1000.0;
                double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
                logger.info("GENERATING SPEED: {} , total = {} messages, elapseSeconds = {}",
                        messagesPerSecond, totalQuantity.get(), elapsedSeconds);
            }
        }
}

