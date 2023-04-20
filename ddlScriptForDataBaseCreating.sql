DROP TABLE IF EXISTS epicentr_repo.availability_goods.types CASCADE;
DROP TABLE IF EXISTS epicentr_repo.availability_goods.stores CASCADE;
DROP TABLE IF EXISTS epicentr_repo.availability_goods.products CASCADE;
DROP TABLE IF EXISTS epicentr_repo.availability_goods.quantity_in_store;
CREATE TABLE IF NOT EXISTS availability_goods.stores
(
    id    SERIAL PRIMARY KEY,
    store VARCHAR(200) NULL
);
CREATE TABLE IF NOT EXISTS availability_goods.types
(
    id   SERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS availability_goods.products
(
    id           SERIAL PRIMARY KEY,
    type_id      INT NOT NULL,
    product_name VARCHAR(70),
    quantity     INT DEFAULT (1),
    FOREIGN KEY (type_id) REFERENCES availability_goods.types (id)
);
CREATE TABLE IF NOT EXISTS availability_goods.quantity_in_store
(
    store_id   INT NOT NULL,
    product_id INT NOT NULL,
    total      INT,
    FOREIGN KEY (store_id) REFERENCES availability_goods.stores (id),
    FOREIGN KEY (product_id) REFERENCES availability_goods.products (id)
);







