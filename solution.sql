-- Create sequence of dates starting from min. production date ending at max. production date. 
declare @startdate date = (select min(production_date) from prodution_monthly);
declare @enddate date = (select max(production_date) from prodution_monthly);
with dates as (
	select @startdate as production_date
	union all 
	select EOMONTH(dateadd(month, 1,production_date))
	from dates
	where EOMONTH(dateadd(month, 1,production_date)) <= @enddate
	),


-- Getting unique well IDs
well_ids as (
select distinct well_id
	from prodution_monthly),


-- Join well IDs with dates sequence to generate CTE with well IDs and all production dates.
ids_vs_dates as (
select *
from well_ids
cross join dates
), 


-- Join between the original table " prodution_monthly " and the previously generated table to insert the missing production dates.
filled_table_nulls as (	
select ids_vs_dates.well_id, ids_vs_dates.production_date, prodution_monthly.oil_production
from  ids_vs_dates
full outer join prodution_monthly
on (ids_vs_dates.well_id = prodution_monthly.well_id and ids_vs_dates.production_date = prodution_monthly.production_date)
),


-- Setting 0 as default oil production instead of NULL for the missing production dates.
filled_table as (
select well_id, production_date, 
case when oil_production is not null then oil_production
else 0 
end as oil_production
from filled_table_nulls
),


-- Calculating the difference BETWEEN oil production this month and last month in lag_down_time column.
down_time_table as (
select well_id, production_date,oil_production,
CASE 
	WHEN DATEDIFF(MONTH,lag(production_date) over(partition by well_id order by production_date) ,production_date) = 1
		THEN oil_production- lag(oil_production) over(partition by well_id order by production_date)
	ELSE oil_production
	end as lag_down_time
from filled_table
),


-- Flagging down months as one's
down_flag_table as (
select well_id, production_date,oil_production, lag_down_time,
case when (lag_down_time <= -30 or oil_production <= 25) then 1 else 0
end as down_flag
from down_time_table
),

-- Table index generation on well_id and production_date
rn_down_flag_table as (
select *,Row_Number() over(order by well_id, production_date) as rn from down_flag_table
),



-- Grouping continuous down times in one group to downtime_duration
grouped_rn_down_flag_table as(
select *,
rn - Row_Number() over(order by well_id,production_date) as data_grouping
from rn_down_flag_table
where down_flag = 1
),


-- Generating the report columns 
-- used datefrom parts to make as the down time is from starting of the down month not from its ending , concluded that from the sample output 
downtime_report as (
select well_id, datefromparts(year(min(production_date)), month(min(production_date)), 1) as downtime_start_date, 
	   datefromparts(year(max(production_date)), month(max(production_date)), 1) as downtime_end_date, 
	   count(data_grouping) as downtime_duration
from grouped_rn_down_flag_table
group by data_grouping,well_id
)

-- Ignoring anomalies as the engineering team is interested to know downtime periods.
select * from downtime_report 
where downtime_duration > 1
order by well_id, downtime_start_date