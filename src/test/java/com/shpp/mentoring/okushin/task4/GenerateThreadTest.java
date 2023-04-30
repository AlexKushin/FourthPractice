package com.shpp.mentoring.okushin.task4;

import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

class GenerateThreadTest extends GenerateThread {

    public GenerateThreadTest(ProductGenerator productGenerator, int amount, String sql, Connection connection, int typesCount) {
        super(productGenerator, amount, sql, connection, typesCount);
    }

    @Test
    void testRun() {
    }
}