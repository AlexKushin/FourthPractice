package com.shpp.mentoring.okushin.task4;

import javax.validation.constraints.*;

public class Product {
    //@NotNull(message = "Name cannot be null")
    //@Size(min = 7, message = "Name must be more than 7 characters") //medium
    //@Pattern(regexp = ".*a.*") //hard
    String name;
    @Min(1)
    @Max(17)
    int typeId;
    String description;

    public Product(String name, int typeId, String description) {
        this.name = name;
        this.typeId = typeId;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }


    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
