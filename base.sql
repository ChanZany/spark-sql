
t1
-----------------
select
	a.*,
	c.area,
	p.product_name
from user_visit_action a
join city_info c on c.city_id = a.city_id
join product_info p on p.product_id = a.click_product_id
where a.click_product_id>-1

----------------

t2
---------------
select
	area,
	product_name,
	count(*) as clickCount
from(
	select
		a.*,
		c.area,
		p.product_name
	from user_visit_action a
	join city_info c on c.city_id = a.city_id
	join product_info p on p.product_id = a.click_product_id
	where a.click_product_id>-1
) t1
group by area,product_name

---------------


t3
-------------------
select
	*,
	rank() over(partition by area order by clickCount desc) as rank
from(
	select
		area,
		product_name,
		count(*) as clickCount
	from(
		select
			a.*,
			c.area,
			p.product_name
		from user_visit_action a
		join city_info c on c.city_id = a.city_id
		join product_info p on p.product_id = a.click_product_id
		where a.click_product_id>-1
	) t1
	group by area,product_name
)t2

---------------------

t4
----------------------
select
	*
from (
	select
		*,
		rank() over(partition by area order by clickCount desc) as rank
	from(
		select 
			area,
			product_name,
			count(*) as clickCount
		from(
			select
				a.*,
				c.area,
				p.product_name
			from user_visit_action a
			join city_info c on c.city_id = a.city_id
			join product_info p on p.product_id = a.click_product_id
			where a.click_product_id>-1
		) t1 
		group by area,product_name
	) t2 
) t3 
where rank<=3


