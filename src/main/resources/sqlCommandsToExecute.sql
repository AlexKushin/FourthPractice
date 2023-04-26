SELECT store, producttype, sum(availability_goods.quantity_in_store.total) as tot_type
FROM availability_goods.stores,
     availability_goods.types,
     availability_goods.products,
     availability_goods.quantity_in_store
WHERE products.type_id = types.id
  and stores.id = quantity_in_store.store_id
  and quantity_in_store.product_id = products.id
  and (types.producttype) = ?
GROUP BY types.producttype, stores.store
ORDER BY tot_type desc LIMIT 1;





