with cte as (
    select sum(case when trip_miles < 5 then 1 else 0 end) as trip_miles4,
           sum(case when trip_miles >= 5 and trip_miles <= 15 then 1 else 0 end) as trip_miles5_15,
           sum(case when trip_miles >= 16 and trip_miles <= 25 then 1 else 0 end) as trip_miles16_25,
           sum(case when trip_miles >= 26 and trip_miles <= 100 then 1 else 0 end) as trip_miles26_100,
           cast(count(*) AS DOUBLE) cnt
    from chicago_taxi_trips_parquet
)
select  (trip_miles4 / cnt) * 100 as trip_miles4,
        (trip_miles5_15 / cnt) * 100 as trip_miles5_15,
        (trip_miles16_25 / cnt) * 100 as trip_miles16_25,
        (trip_miles26_100 / cnt) * 100 as trip_miles26_100
from cte
;
