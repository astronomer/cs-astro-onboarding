drop table if exists cs.{{ params.schema_name }}.{{ params.table_name }};
create table cs.{{ params.schema_name }}.{{ params.table_name }} (calendarific_holidays_2019 variant);
copy into cs.{{ params.schema_name }}.{{ params.table_name }} from 's3://astro-onboarding/http_api_example_dag/holidays.json'
credentials = (aws_key_id='{{ conn.aws_default.extra_dejson.aws_access_key_id }}' aws_secret_key='{{ conn.aws_default.extra_dejson.aws_secret_access_key }}')
file_format = (type = json)
;