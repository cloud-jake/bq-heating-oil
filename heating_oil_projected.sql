with get_deliveries as (
SELECT  delivery_date, 
        LAG(delivery_date) OVER (ORDER BY delivery_date) as previous_delivery,
FROM `bq-jake.homedata.heating_oil` 
WHERE delivery_date is not null
ORDER BY delivery_date
),

-- append Today's date to end of list so we can estimate for today
append_today as (
SELECT  CURRENT_DATE('America/New_York') as delivery_date,
        max(delivery_date) as previous_delivery
from get_deliveries
),

all_deliveries as (
SELECT * FROM get_deliveries
UNION ALL 
SELECT * FROM append_today
),

calc_days as (
SELECT  delivery_date,
        previous_delivery,
        DATE_DIFF(delivery_date, previous_delivery, DAY) as CALC_days,  -- Number of days between deliveries
--        GENERATE_DATE_ARRAY(previous_delivery,delivery_date, INTERVAL 1 DAY) as CALC_dates, -- list of dates between delivieries
       GENERATE_DATE_ARRAY(previous_delivery,DATE_SUB(delivery_date, INTERVAL 1 DAY), INTERVAL 1 DAY) as CALC_dates, -- list of dates between delivieries (exclude next delivery date)
from all_deliveries
),

flatten_dates as (
-- unnest days by delivery date so we can join to weather data
SELECT delivery_date, previous_delivery, dates_flattened
from calc_days, UNNEST(CALC_dates) as dates_flattened
),
-- Wilcard Tables - https://cloud.google.com/bigquery/docs/querying-wildcard-tables
--GSOD_dates as (
--SELECT CAST(CONCAT(year,'-',mo,'-',da) as DATE) as DATE,
--       min, 
--       max,
--       temp
--FROM
--  `bigquery-public-data.noaa_gsod.gsod20*`
--WHERE
--  max != 9999.9 # code for missing data
--  AND _TABLE_SUFFIX BETWEEN '11'
--  AND '20'
--  AND stn = '725037'
--  AND wban = '94745'
--ORDER BY DATE
--),

GSOD_dates as (
SELECT * FROM `bq-jake.temp.gsod_dates` 
),

get_degree_days as (
SELECT 
        delivery_date, 
        previous_delivery,
        CAST(temp as Numeric) as mean_temp,
        CASE 
          WHEN 65-CAST(temp as Numeric) > 0 THEN 65-CAST(temp as Numeric)
          ELSE 0
        END as CALC_degree_days,
        CAST(CASE
          WHEN CAST(temp as Numeric) >= 62 THEN 6
          WHEN CAST(temp as Numeric) >= 58 THEN 5
          WHEN CAST(temp as Numeric) >= 54 THEN 4
          WHEN CAST(temp as Numeric) >= 50 THEN 3
          WHEN CAST(temp as Numeric) >= 46 THEN 2
          WHEN CAST(temp as Numeric) >= 43 THEN 1
          ELSE 0.0
        END as Numeric)as CALC_hot_water
-- http://www.degreeday.com/faqs.aspx
FROM flatten_dates
LEFT JOIN GSOD_dates
ON DATE = dates_flattened
),

sum_degree_days as (
SELECT delivery_date, previous_delivery, SUM(CALC_degree_days + CALC_hot_water) as degree_days
from get_degree_days
GROUP BY delivery_date, previous_delivery
),

all_history as (
SELECT  CASE
          WHEN a.delivery_date is NULL THEN CURRENT_DATE('America/New_York') -- so we can estimate for today's date
          ELSE a.delivery_date
        END as delivery_date,
        a.* EXCEPT(delivery_date),
        c.previous_delivery,
        b.* EXCEPT(delivery_date, previous_delivery),
       degree_days/gallons as Kfactor
FROM sum_degree_days b, all_deliveries c
LEFT JOIN `bq-jake.homedata.heating_oil` a
ON a.delivery_date = b.delivery_date
WHERE c.previous_delivery = b.previous_delivery
),
calc_kfactor as (
SELECT  CURRENT_DATE('America/New_York') as DATE,
--        COUNT(Kfactor) as count_Kfactor, 
--        SUM(Kfactor) as sum_Kfactor,
        SUM(Kfactor) / COUNT(Kfactor) as mean_Kfactor
from all_history

),
put_it_all_together as (
SELECT  delivery_date,
        previous_delivery,
        CASE
          WHEN gallons is not null THEN gallons
          ELSE degree_days / mean_Kfactor
        END as gallons,
        cost_per_gallon,
        total_price,
        CALC_gallons_per_day,
        CALC_cost_per_day,
        CALC_cost_per_month,
        CASE
          WHEN company is null THEN 'Google Cloud'
          ELSE company
        END as company,
        degree_days,
        CASE
          WHEN Kfactor > 0 THEN Kfactor
          WHEN Kfactor is null THEN mean_Kfactor
          ELSE null
        END as Kfactor,
        CASE
          WHEN company is null THEN CONCAT('Kfactor estimated from mean Kfactor, gallons calculated to be delivered on ', delivery_date)
          ELSE CONCAT(gallons,' gallons were delivered on ',delivery_date,' with ',degree_days,' degree days since last delivery on ',previous_delivery)
        END as Comments
FROM all_history 
LEFT JOIN calc_kfactor
ON DATE = delivery_date
)

SELECT * FROM put_it_all_together
ORDER BY delivery_date
