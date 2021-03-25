select c.id, c.name, c.city, t.date, t.amount, t.flag 
from customers_latest c, transactions_with_products t
where c.id = t.customer_id