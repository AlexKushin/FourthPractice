package com.shpp.mentoring.okushin.task4;

import com.shpp.mentoring.okushin.task3.PropertyManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws SQLException {
        //String url = "jdbc:postgresql://epicentr-repo.crw51pyylhbt.us-east-1.rds.amazonaws.com:5432/";
        //String jdbcUrl="jdbc:postgresql://localhost:5432/epicentr_repo"

        //I will have success in the short term
        StopWatch watch = new StopWatch();
        Properties prop = new Properties();
        PropertyManager.readPropertyFile("application.properties", prop);
        HikariConfig config = new HikariConfig(prop);
        DataSource dataSource = new HikariDataSource(config);

        try {
            watch.start();
            SqlExecute.executeSqlScript(dataSource.getConnection(), "ddlScriptForDataBaseCreating.sql");
            watch.stop();
            logger.info("Creating tables time= {}", watch.getTime() / 1000.0);
            watch.reset();
            watch.start();
            CsvImporter.importToDB(dataSource.getConnection(),
                    "stores.csv", "availability_goods.stores");
            CsvImporter.importToDB(dataSource.getConnection(),
                    "types.csv", "availability_goods.types");
            watch.stop();
            logger.info("Import fields from csv time= {}", watch.getTime() / 1000.0);
            watch.reset();

            int numberThreads = 10;
            ExecutorService executorService = Executors.newFixedThreadPool(numberThreads);

            String sql = "INSERT INTO availability_goods.products (type_id,product_name) VALUES" + " (CAST(? AS INTEGER), ? ), ".repeat(999) +
                    "(CAST(? AS INTEGER), ? )";

            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                Validator validator = factory.getValidator();

                int typesCount = SqlExecute.executeQuerySqlScript(dataSource.getConnection(), "SELECT count(*) from availability_goods.types;");
                logger.info("Types number = {}", typesCount);
                int storesCount = SqlExecute.executeQuerySqlScript(dataSource.getConnection(), "SELECT count(*) from availability_goods.stores;");
                logger.info("Stores number = {}", storesCount);
                ProductGenerator generator = new ProductGenerator(validator);
                watch.start();
                int amount = 3000000;
                for (int i = 0; i < numberThreads; i++) {
                    executorService.submit(new GenerateThread(generator, amount / numberThreads, sql, dataSource.getConnection(), typesCount));
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
                        //  SqlExecute.executeSqlScript(dataSource.getConnection(), "dmlCommandForFillingTable.sql");
                        SqlExecute.executeSqlPreparedStatement(dataSource.getConnection(), "dmlCommandForFillingTable.sql", storesCount);
                        watch.stop();
                        logger.info("filling stores speed with products= {}", watch.getTime() / 1000.0);
                        watch.reset();
                        watch.start();
                        SqlExecute.executeSqlStatement(dataSource.getConnection(), "CREATE INDEX  ON  availability_goods.products (type_id)");
                        SqlExecute.executeSqlStatement(dataSource.getConnection(), "CREATE INDEX  ON  availability_goods.quantity_in_store (product_id,store_id)");
                        watch.stop();
                        logger.info("Adding indexes time= {}", watch.getTime() / 1000.0);
                        watch.reset();
                        //System.setProperty("file.encoding", "UTF-8");
                        String productType = System.getProperty("productType");
                        //productType = new String(productType.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);


                        watch.start();
                        SqlExecute.executeQuerySqlScript(dataSource.getConnection(), "sqlCommandsToExecute.sql", productType);
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

