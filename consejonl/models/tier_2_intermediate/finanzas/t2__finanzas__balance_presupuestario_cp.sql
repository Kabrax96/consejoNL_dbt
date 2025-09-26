/*
===========================================================================================================
model name : t2__finanzas__balance_presupuestario_cp
author : Alejandro Morales Benavides
date : September 24th, 2025
usage : dbt run --select t2__finanzas__balance_presupuestario_cp
objective :
 this model performs the following:
 1) applies incremental materialization with full-date filtering - CP VERSION.
 2) prepares clean intermediate table for indicator generation.
dependencies :
 - depends on: t1__finanzas__balance_presupuestario_cp
assumptions/notes :
 - CREATE_DTTM is added for auditing.
 - model is incremental by FULL_DATE.
 - CP version of balance_presupuestario intermediate model.
===========================================================================================================
history :
-----------------------------------------------------------------------------------------------------------
name | date | project | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales | 08/08/2025 | consejo_nl_dbt | Created intermediate model for balance_presupuestario.
Alejandro Morales | 09/24/2025 | consejo_nl_dbt | Created CP version of balance_presupuestario intermediate model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/
{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}
{{
 config(
 materialized = "table",
 unique_key = ["SURROGATE_KEY"],
 on_schema_change = "append_new_columns",
 merge_exclude_columns = ["CREATE_DTTM"],
 snowflake_warehouse = "COMPUTE_WH",
 tags=['finanzas', 'balance_presupuestario', 't2', 'balance_presupuestario_cp'],
 pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"
 ],
 post_hook = [
 "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
 ]
 )
}}
with base as (
select
TYPE,
AMOUNT,
CONCEPT,
SUBLABEL,
FULL_DATE,
YEAR_QUARTER,
SURROGATE_KEY,
current_timestamp() as CREATE_DTTM
from {{ ref('t1__finanzas__balance_presupuestario_cp') }}
 {% if is_incremental() %}
where FULL_DATE > (select coalesce(max(FULL_DATE), '2000-01-01') from {{ this }})
 {% endif %}
)
select *
from base
