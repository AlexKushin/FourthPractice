INSERT INTO availability_goods.quantity_in_store (store_id, product_id, total)
SELECT round(random() * 19) + 1 ::int, availability_goods.products.id, round(random() * 100) + 1::int
FROM availability_goods.products;