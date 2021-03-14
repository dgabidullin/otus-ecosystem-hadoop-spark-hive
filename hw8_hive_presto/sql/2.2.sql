with company_total as (
    select company,
           sum(trip_total) as total
    from chicago_taxi_trips_parquet
    group by company
),
     rn_total as (
         select company,
                total,
                row_number() over (order by total desc) as rn
         from company_total
     )
select company
from rn_total
where rn <= 10
order by rn
;
