create table if not exists `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table }}` (
  date_key date,
  crime int,
  b_district int,
  d_district int,
  f_district int,
  uk_district int,
  h_district int,
  c_district int,
  i_district int,
  a_district int,
  eightyeight_district int,
  g_district int,
  e_district int,
  ap_district int
);

delete from `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table }}` where date_key = '{{ ds }}';

insert into `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table }}`
  select
   extract(date from timestamp) as date_key,
   sum(1) as crime,
   sum(case when district = "B" then 1 else 0 end) as b_district,
   sum(case when district = "D" then 1 else 0 end) as d_district,
   sum(case when district = "F" then 1 else 0 end) as f_district,
   sum(case when district = "UK" then 1 else 0 end) as uk_district,
   sum(case when district = "H" then 1 else 0 end) as h_district,
   sum(case when district = "C" then 1 else 0 end) as c_district,
   sum(case when district = "I" then 1 else 0 end) as i_district,
   sum(case when district = "A" then 1 else 0 end) as a_district,
   sum(case when district = "88" then 1 else 0 end) as eightyeight_district,
   sum(case when district = "G" then 1 else 0 end) as g_district,
   sum(case when district = "E" then 1 else 0 end) as e_district,
   sum(case when district = "AP" then 1 else 0 end) as ap_district
  from `{{ params.project_id }}.public_data.crime`
  where extract(date from timestamp) = '{{ ds }}'
  group by date_key
;