package com.shpp.mentoring.okushin.task4;

import org.junit.jupiter.api.Test;

import javax.validation.Validator;

import static org.junit.jupiter.api.Assertions.*;

class ProductGeneratorTest extends ProductGenerator {

    public ProductGeneratorTest(Validator validator) {
        super(validator);
    }

    @Test
    void testInsertValidatedProducts() {
    }
}