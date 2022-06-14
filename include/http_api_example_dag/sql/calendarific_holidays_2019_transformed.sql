drop table if exists demo.calendarific_holidays_2019_transformed;
create table demo.calendarific_holidays_2019_transformed as (
  select
    date_key,
    holiday_name,
    holiday_description,
    country_id,
    country_name,
    listagg(distinct holiday_type, ', ') as holiday_types,
    listagg(distinct locations, ', ') as locations
  from (
    select
      t0.value:"date"."iso"::date as date_key,
      t0.value:"name"::varchar as holiday_name,
      t0.value:"type"[0]::varchar as holiday_type,
      t0.value:"description"::varchar as holiday_description,
      t0.value:"country"."id"::varchar as country_id,
      t0.value:"country"."name"::varchar as country_name,
      t0.value:"locations"::varchar as locations
    from cs.demo.calendarific_holidays_2019,
         lateral flatten(input => calendarific_holidays_2019) as t0
  )
  group by date_key, holiday_name, holiday_description, country_id, country_name
);