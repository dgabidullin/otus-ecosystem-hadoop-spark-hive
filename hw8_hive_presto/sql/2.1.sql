with cte as (
    select date_format(CAST(trip_start_timestamp as timestamp), '%Y-%m') date_month,
           count(*) total
    from chicago_taxi_trips_parquet
    group by 1
)
select date_month,
       total,
       total - coalesce(LAG(total) OVER (ORDER BY date_month), total) dynamic
from cte
;
