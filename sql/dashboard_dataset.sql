create or replace view transportation.gold.vw_ride_performance as
select
    business_date,
    city_id,
    city_name,
    passenger_category,
    distance_kms,
    sales_amt,
    passenger_rating,
    driver_rating,
    year,
    month,
    month_name,
    quarter,
    week_of_year,
    day_of_week,
    is_weekday,
    is_weekend,
    national_holiday
from transportation.gold.fact_trips;

