package com.shpp.mentoring.okushin.task4;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/epicentr_repo";
        String user = "postgres";
        String password = "1234password4321";
        StopWatch watch = new StopWatch();
        watch.start();

        watch.stop();
        System.out.println(watch.getTime() / 1000.0);

        try {
            SqlExecute.executeSqlScript(url, user, password, "ddlScriptForDataBaseCreating.sql");

            CsvImporter csvImporter = new CsvImporter();
            csvImporter.importToDB(url, user, password,
                    "stores.csv", "availability_goods.stores");
            csvImporter.importToDB(url, user, password,
                    "types.csv", "availability_goods.types");
            int storesCount = SqlExecute.executeQuerySqlScript(url, user, password, "SELECT count(*) from availability_goods.stores;");
            System.out.println(storesCount);
            int typesCount = SqlExecute.executeQuerySqlScript(url, user, password, "SELECT count(*) from availability_goods.types;");
            System.out.println(typesCount);

            ExecutorService executorService = Executors.newFixedThreadPool(10);

            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO availability_goods.products (type_id,product_name) VALUES");
            for (int i = 0; i < 999; i++) {
                sqlBuilder.append(" (CAST(? AS INTEGER), ? ), ");
            }
            sqlBuilder.append("(CAST(? AS INTEGER), ? )");
            String sql = sqlBuilder.toString();

            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                Validator validator = factory.getValidator();
                ProductGenerator generator = new ProductGenerator(validator);
                long startTime = System.currentTimeMillis();
                int amount = 30000;
                for (int i = 0; i < 10; i++) {
                    executorService.submit(new GenerateThread(generator, amount, password, url, user, sql));
                }
                executorService.shutdown();
                while (true) {
                    if (executorService.isTerminated()) {
                        long endTime = System.currentTimeMillis();
                        double elapsedSeconds = (endTime - startTime) / 1000.0;
                        double messagesPerSecond = 300000 / elapsedSeconds;
                        logger.info("RECEIVING SPEED in 10 threads: {} messages per second, total = {} messages, elapseSeconds = {}",
                                messagesPerSecond, 300000, elapsedSeconds);

                        SqlExecute.executeSqlScript(url, user, password, "dmlCommandForFillingTable.sql");

                        Scanner scanner = new Scanner(System.in);
                        String productType = "";
                        while (!productType.equals("стоп")) {
                            System.out.print("Введіть тип товару: ");
                            productType = scanner.nextLine();
                            SqlExecute.executeQuerySqlScript(url, user, password, "sqlCommandsToExecute.sql", productType);
                        }
                        scanner.close();
                        return;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

