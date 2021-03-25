select t.id, t.name, t.city
from customers t
inner join (
    select id, max(date) as MaxDate
    from customers
    group by id
) tm on t.id = tm.id and t.date = tm.MaxDate