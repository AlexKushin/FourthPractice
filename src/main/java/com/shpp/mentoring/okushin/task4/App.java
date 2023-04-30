package com.shpp.mentoring.okushin.task4;

import com.shpp.mentoring.okushin.exceptions.CreateStatementException;
import com.shpp.mentoring.okushin.task3.PropertyManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final Logger loggerDBCP = LoggerFactory.getLogger(BasicDataSource.class);


    public static void main(String[] args) {

        //AWS String url = "jdbc:postgresql://epicentr-repo.cm8gtofqzkw2.eu-central-1.rds.amazonaws.com:5432/";
        //LocalDB String jdbcUrl="jdbc:postgresql://localhost:5432/epicentr_repo"

        //I will have success in the short term
        StopWatch watch = new StopWatch();

        Properties configPropForQueries = new Properties();
        PropertyManager.readPropertyFile("configurationsHikariCpForQueries.properties", configPropForQueries);
        logger.info("HikariCP configurations was successfully read ");
        Properties configPropForGenerating = new Properties();
        PropertyManager.readPropertyFile("configurationsDBCPForGeneratingProducts.properties", configPropForGenerating);

        HikariConfig config1 = new HikariConfig(configPropForQueries);


        try (HikariDataSource dataSourceForQueries = new HikariDataSource(config1)) {
            watch.start();
            SqlExecuter.executeSqlScript(dataSourceForQueries.getConnection(), "ddlScriptForDataBaseCreating.sql");
            watch.stop();
            logger.info("Creating tables time= {}", watch.getTime() / 1000.0);
            watch.reset();
            watch.start();
            CsvImporter.importToDB(dataSourceForQueries.getConnection(),
                    "stores.csv", "availability_goods.stores");
            CsvImporter.importToDB(dataSourceForQueries.getConnection(),
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

                int typesCount = SqlExecuter.executeQuerySqlScript(dataSourceForQueries.getConnection(), "SELECT count(*) from availability_goods.types;");
                logger.info("Types number = {}", typesCount);
                int storesCount = SqlExecuter.executeQuerySqlScript(dataSourceForQueries.getConnection(), "SELECT count(*) from availability_goods.stores;");
                logger.info("Stores number = {}", storesCount);

                watch.start();
                BasicDataSource basicDataSourceForGeneratingProducts = createBasicDataSource(configPropForGenerating);
                watch.stop();
                logger.info("Setting 10 connection for generating products time = {}", watch.getTime() / 1000.0);
                watch.reset();

                ProductGenerator generator = new ProductGenerator(validator);
                int amount = 3000000;

                watch.start();
                for (int i = 0; i < numberThreads; i++) {
                    executorService.submit(new GenerateThread(generator, amount / numberThreads, sql, basicDataSourceForGeneratingProducts.getConnection(), typesCount));
                }
                executorService.shutdown();
                while (true) {
                    if (executorService.isTerminated()) {
                        watch.stop();
                        basicDataSourceForGeneratingProducts.close();
                        logger.info("10 connection for generating products are closed");
                        double elapsedSeconds = watch.getTime() / 1000.0;
                        double productsPerSecond = amount / elapsedSeconds;
                        logger.info("GENERATING SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                                , numberThreads, productsPerSecond, amount, elapsedSeconds);
                        watch.reset();

                        watch.start();
                        SqlExecuter.executeSqlPreparedStatement(dataSourceForQueries.getConnection(), "dmlCommandForFillingTable.sql", storesCount);
                        watch.stop();
                        logger.info("filling stores speed with products= {}", watch.getTime() / 1000.0);
                        watch.reset();

                        watch.start();
                        SqlExecuter.executeSqlStatement(dataSourceForQueries.getConnection(),
                                "CREATE INDEX  ON  availability_goods.products (type_id)");
                        SqlExecuter.executeSqlStatement(dataSourceForQueries.getConnection(),
                                "CREATE INDEX  ON  availability_goods.quantity_in_store (product_id,store_id)");
                        watch.stop();
                        logger.info("Adding indexes time= {}", watch.getTime() / 1000.0);
                        String productType = System.getProperty("productType");

                        SqlExecuter.executeQuerySqlScript(dataSourceForQueries.getConnection(), "sqlCommandsToExecute.sql", productType);
                        return;
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error executing Sql command: {}", e.getMessage(), e);
            throw new CreateStatementException("Exception while creating statement");
        }
    }

    public static BasicDataSource createBasicDataSource(Properties basicDataSourceConfig) {
        BasicDataSource basicDataSourceForGeneratingProducts = new BasicDataSource();
        String jdbcUrl = basicDataSourceConfig.getProperty("jdbcUrl");
        String userName = basicDataSourceConfig.getProperty("username");
        String password = basicDataSourceConfig.getProperty("password");
        int initialSize = Integer.parseInt(basicDataSourceConfig.getProperty("initialSize"));
        int maxActive = Integer.parseInt(basicDataSourceConfig.getProperty("maxActive"));
        int minIdle = Integer.parseInt(basicDataSourceConfig.getProperty("minIdle"));
        int maxIdle = Integer.parseInt(basicDataSourceConfig.getProperty("maxIdle"));


        basicDataSourceForGeneratingProducts.setUrl(jdbcUrl);
        basicDataSourceForGeneratingProducts.setUsername(userName);
        basicDataSourceForGeneratingProducts.setPassword(password);
        basicDataSourceForGeneratingProducts.setInitialSize(initialSize);
        basicDataSourceForGeneratingProducts.setMaxActive(maxActive);
        basicDataSourceForGeneratingProducts.setMinIdle(minIdle);
        basicDataSourceForGeneratingProducts.setMaxIdle(maxIdle);

        loggerDBCP.debug("jdbcUrl = {}", jdbcUrl);
        loggerDBCP.debug("userName = {}", userName);
        loggerDBCP.debug("password = *********(masked)");
        loggerDBCP.debug("initialSize = {}", initialSize);
        loggerDBCP.debug("maxActive = {}", maxActive);
        loggerDBCP.debug("minIdle = {}", minIdle);
        loggerDBCP.debug("maxIdle = {}", maxIdle);
        return basicDataSourceForGeneratingProducts;
    }
}

