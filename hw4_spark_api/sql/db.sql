DROP
DATABASE IF EXISTS otus;

CREATE
DATABASE otus;

DROP TABLE IF EXISTS public.trip_distribution;

CREATE TABLE public.trip_distribution
(
    borough       text,
    total         int,
    mean_distance float,
    std_distance  float,
    min_distance  float,
    max_distance  float
);
