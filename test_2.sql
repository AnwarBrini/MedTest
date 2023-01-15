-- Question 1
With Quant as (
Select
Date,
prod_price * prd_qty as qty_price
From  TRANSACTION
Where  Date >= '01/01/2019' and Date <= '31/12/2019'
)
select
Date,
sum(qty_price) as ventes
From quant
Group by Date
Order by Date DESC



-- Question 2
Select
client_id,
product_type,
sum(prod_price * prod_qty)
From (
Select trs.date, trs.client_id, trs.prop_id, trs.prod_price, trs.prod_qty, pn.product_type
From TRANSACTION trs
Left join 	PRODUCT_NOMENCLATURE pn
On trs.prop_id = pn.product_id ) enriched_transactions

group by 1, 2



)