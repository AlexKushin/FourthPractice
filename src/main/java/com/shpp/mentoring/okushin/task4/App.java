package com.shpp.mentoring.okushin.task4;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        //String url = "jdbc:postgresql://localhost:5432/epicentr_repo";
          String url = "jdbc:postgresql://epicentr-repo.crw51pyylhbt.us-east-1.rds.amazonaws.com:5432/";
        String user = "postgres";
        String password = "1234password4321";
        StopWatch watch = new StopWatch();
        try {
            SqlExecute.executeSqlScript(url, user, password, "ddlScriptForDataBaseCreating.sql");

            CsvImporter.importToDB(url, user, password,
                    "stores.csv", "availability_goods.stores");
            CsvImporter.importToDB(url, user, password,
                    "types.csv", "availability_goods.types");

            //int storesCount = SqlExecute.executeQuerySqlScript(url, user, password, "SELECT count(*) from availability_goods.stores;");


            int numberThreads = 10;
            ExecutorService executorService = Executors.newFixedThreadPool(numberThreads);

            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO availability_goods.products (type_id,product_name) VALUES");
            for (int i = 0; i < 999; i++) {
                sqlBuilder.append(" (CAST(? AS INTEGER), ? ), ");
            }
            sqlBuilder.append("(CAST(? AS INTEGER), ? )");
            String sql = sqlBuilder.toString();

            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                Validator validator = factory.getValidator();
                ProductGenerator generator = new ProductGenerator(validator);
                watch.start();
                int amount = 3000000;
                for (int i = 0; i < numberThreads; i++) {
                    executorService.submit(new GenerateThread(generator, amount / numberThreads, password, url, user, sql));
                }
                executorService.shutdown();
                while (true) {
                    if (executorService.isTerminated()) {
                        watch.stop();
                        double elapsedSeconds = watch.getTime() / 1000.0;
                        double messagesPerSecond = amount / elapsedSeconds;
                        logger.info("GENERATING SPEED by {} threads: {} , total = {} messages, elapseSeconds = {}"
                                , numberThreads, messagesPerSecond, amount, elapsedSeconds);
                        watch.reset();
                        watch.start();
                        SqlExecute.executeSqlScript(url, user, password, "dmlCommandForFillingTable.sql");
                        watch.stop();
                        logger.info("filling stores speed with products= {}", watch.getTime() / 1000.0);
                        watch.reset();
                        System.setProperty("file.encoding", "UTF-8");
                        String productType = System.getProperty("productType");
                        productType = new String(productType.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);


                        watch.start();
                        SqlExecute.executeQuerySqlScript(url, user, password, "sqlCommandsToExecute.sql", productType);
                        watch.stop();
                        logger.info("Search store time= {}", watch.getTime() / 1000.0);
                        watch.reset();
                        return;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

