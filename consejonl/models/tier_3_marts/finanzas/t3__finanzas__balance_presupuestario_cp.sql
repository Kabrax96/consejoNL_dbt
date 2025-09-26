/*
===========================================================================================================
Model Name : t3__finanzas__balance_presupuestario_cp
Author : Alejandro Morales Benavides
Date : September 24th, 2025
Usage : dbt build --select t3__finanzas__balance_presupuestario_cp
Objective :
 This model - CP VERSION:
 1) Aggregates balance presupuestario data by YEAR_QUARTER
 2) Provides total balance and latest date per quarter
 3) Serves as a mart for CP balance presupuestario analysis
Dependencies :
 - Source model: t2__finanzas__balance_presupuestario_cp
Assumptions/Notes :
 - Groups by YEAR_QUARTER and sums AMOUNT
 - Includes audit fields (CREATE_TMS, CREATE_BY)
 - CP version of balance_presupuestario mart
===========================================================================================================
History :
-----------------------------------------------------------------------------------------------------------
Name | Date | Project | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides | 2025-09-24 | consejonl | Created CP version of balance_presupuestario mart.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/
{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}
{{
 config(
 materialized = "table",
 snowflake_warehouse = "COMPUTE_WH",
 tags=['finanzas', 'balance_presupuestario', 't3', 'balance_presupuestario_cp'],
 pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
 post_hook = [
 "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
 ]
 )
}}
with source_data as (
 select *
 from {{ ref('t2__finanzas__balance_presupuestario_cp') }}
),
final as (
select
YEAR_QUARTER,
sum(AMOUNT) as total_balance_presupuestario,
max(FULL_DATE) as latest_date,
current_timestamp as CREATE_TMS,
'amoralesb196@gmail.com' as CREATE_BY
from source_data
group by YEAR_QUARTER
)
select * from final
order by YEAR_QUARTER
