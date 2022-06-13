select
  count(*) = 0 as has_unique_values
from (
    select
        {{ params.unique_col }},
        count(*)
    from `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table }}`
    group by {{ params.unique_col }}
    having count(*) > 1
);