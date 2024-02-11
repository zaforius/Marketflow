create or replace view public.fact_table_2 as
with a as (
SELECT 
 "SKU_number"
, product_name
, store
, case when final_price is null then price_of_weight else final_price end as price 
, case when final_price is null then  
		 (case when (price_of_weight_before_discount is null or price_of_weight_before_discount =0) then price_of_weight else price_of_weight_before_discount end)
  else   (case when price_before_discount is null then final_price else price_before_discount end)
  end as starting_price
--, price_before_discount
--, price_of_weight
--, price_of_weight_before_discount
--, link
, cast("date" as date) as "date" 
FROM public.fact_table
)
select a.*
,starting_price - price as discount
from a
where price is not null 
and price != 0
and starting_price is not null
and starting_price != 0;

