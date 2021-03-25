select t.date, t.amount, t.customer_id, p.flag 
from products p, transactions t
where p.id = t.product_id