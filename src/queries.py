"""
- calculate route_length_km
    1. routes left join trips using (route_id) left join stop_times st using (trip_id)
    2. max(st.shape_dist_traveled) as trip_len per each route
    3. most frequent value of trip_len per each route is approx. route_length_km
- calculate stops_amount
    1. routes left join trips using (route_id)
    2. count(distinct trips.stop_id) as stops_per_trip per each route
    3. most frequent value of stops_per_trip per each route is approx. stops_amount
"""

LINE_DIM_QUERY = """
with trip_lengths as (
    select
        t.route_id,
        t.trip_id,
        max(st.shape_dist_traveled) as trip_len
    from trips t
    left join stop_times st on t.trip_id = st.trip_id
    group by t.route_id, t.trip_id
),
trip_len_mode as (
    select
        route_id,
        trip_len,
        count(*) as freq,
        row_number() over (partition by route_id order by count(*) desc, trip_len desc) as rn
    from trip_lengths
    group by route_id, trip_len
),
route_length_mode as (
    select route_id, trip_len as route_length_km
    from trip_len_mode
    where rn = 1
),
stops_per_trip as (
    select
        t.route_id,
        t.trip_id,
        count(distinct st.stop_id) as stops_per_trip
    from trips t
    left join stop_times st on t.trip_id = st.trip_id
    group by t.route_id, t.trip_id
),
stops_mode as (
    select
        route_id,
        stops_per_trip,
        count(*) as freq,
        row_number() over (partition by route_id order by count(*) desc, stops_per_trip desc) as rn
    from stops_per_trip
    group by route_id, stops_per_trip
),
route_stops_mode as (
    select route_id, stops_per_trip as stops_amount
    from stops_mode
    where rn = 1
)
select
    r.route_id as id,
    v.carrier as operator,
    case r.route_type
        when 0 then 'tram'
        when 2 then 'rail'
        when 3 then 'bus'
        else 'unknown'
    end as line_type,
    coalesce(rl.route_length_km, 0) as route_length_km,
    coalesce(rs.stops_amount, 0) as stops_amount
from routes r
left join delays d on r.route_id = d."Route"
left join vehicles v on d."Vehicle No" = v.vehicle_number
left join route_length_mode rl on r.route_id = rl.route_id
left join route_stops_mode rs on r.route_id = rs.route_id
"""

STOP_DIM_QUERY = """
select
    stop_id as id,
    stop_name as name,
    cast(stop_lat as float) as lat,
    cast(stop_lon as float) as lon
from stops
"""

VEHICLE_DIM_QUERY = """
select
    vehicle_number as id,
    manufacturer as brand,
    type as v_model,
    cast(production_year as integer) as year_produced
from vehicles
where
    vehicle_number is not null
    and trim(vehicle_number) != ''
    and manufacturer is not null
    and trim(manufacturer) != ''
    and type is not null
    and trim(type) != ''
    and production_year is not null
    and cast(production_year as varchar) ~ '^\d+$'
order by vehicle_number
"""

WEATHER_DIM_QUERY = """
select
    id,
    temperature,
    fall_mm,
    fall_type,
    wind_speed_mps,
    wind_direction_deg,
    humidity_percent,
    pressure_hpa,
    general_circumstances
from weather
"""

TIME_DIM_QUERY = """
select
    id,
    full_timestamp,
    hour_,
    weekday,
    weekday_num,
    month_,
    month_num,
    season,
    year_,
    time_of_day,
    is_business_day
from time_dim
"""

DELAY_FACT_QUERY = """
select
    d.Delay as delay_mins,
    t.id as time_id,
    w.id as weather_id,
    v.vehicle_number as vehicle_id,
    r.route_id as line_id,
    s.stop_id as stop_id
from delays d
join time_dim t on t.full_timestamp = d.Timestamp
join weather w on w.id = '12375-' || strftime(cast(d.Timestamp as timestamp), '%Y-%m-%d-%H')
join vehicles v on v.vehicle_number = d."Vehicle No"
join routes r on r.route_id = d.Route
join stops s on s.stop_name = d."Stop Name"
"""
