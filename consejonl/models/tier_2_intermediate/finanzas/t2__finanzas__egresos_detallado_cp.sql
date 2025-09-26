/*
===========================================================================================================
model name : t2__finanzas__egresos_detallado_cp
author : Alejandro Morales Benavides
date : September 24th, 2025
usage : dbt build --select t2__finanzas__egresos_detallado_cp
objective :
 This intermediate model - CP VERSION:
 1) Performs data type validation and minor cleaning.
 2) Ensures key fields are standardized.
 3) Prepares the data for marts in tier 3 (Sección I, II, inflación).
dependencies :
 - Depends on t1__finanzas__egresos_detallado_cp
assumptions/notes :
 - Removes rows where essential fields (concepto, modificado, cuarto) are null.
 - Trims whitespace from text fields.
 - Parses date string if required.
 - CP version of egresos_detallado intermediate model.
===========================================================================================================
history :
-----------------------------------------------------------------------------------------------------------
name | date | project | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales | 08/08/2025 | consejo_nl_dbt | Created intermediate model for egresos_detallado.
Alejandro Morales | 09/24/2025 | consejo_nl_dbt | Created CP version of egresos_detallado intermediate model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/
{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "table",
        unique_key = ["surrogate_key"],
        on_schema_change = "append_new_columns",
        merge_exclude_columns = ["create_dttm"],
        snowflake_warehouse = "COMPUTE_WH",
        tags=['finanzas','egresos_detallado','t2','egresos_detallado_cp'],
        pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
        post_hook = [
            "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
        ]
    )
}}

with base as (
    select
        try_to_date(fecha) as fecha,
        codigo,
        cuarto,
        pagado,
        seccion,
        aprobado,
        concepto,
        devengado,
        modificado,
        subejercicio,
        surrogate_key,
        "AMPLIACIONES/REDUCCIONES",
        current_timestamp() as create_dttm
    from {{ ref('t1__finanzas__egresos_detallado_cp') }}
    {% if is_incremental() %}
        where try_to_date(fecha) > (select coalesce(max(fecha), '2000-01-01') from {{ this }})
    {% endif %}
)

select * from base
