/*
===========================================================================================================
Model Name          : t3__finanzas__inflacion_gasto_no_etiquetado
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t3__finanzas__inflacion_gasto_no_etiquetado

Objective           :
    This model:
        1) Aggregates Section I (Gasto No Etiquetado) by calendar year.
        2) Joins annual inflation (seed: inflacion_anual) for year X.
        3) Computes the requested indicator:
           ((GNE in year X) / (GNE in year X-5 * (1 + INFLATION_X)))^(1/4) - 1

Dependencies        :
    - Source model: t3__finanzas__egresos_no_etiquetados
    - Inflation seed: inflacion_anual

Assumptions/Notes   :
    - Temporal grain is calendar year derived from FECHA.
    - GNE is measured using MODIFICADO.
    - Seed provides one row per year (columns: year, inflation).
    - Denominator validation to avoid division by zero or nulls.

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-08-12  | consejonl   | Created Tier 3 model computing GNE inflation-adjusted growth metric using seed.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "table",
        snowflake_warehouse = "COMPUTE_WH",
        pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
        post_hook = [
            "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
        ]
    )
}}

with gne_by_year as (
    select
        date_part(year, fecha)::int as year,
        sum(modificado)            as gne
    from {{ ref('t3__finanzas__egresos_no_etiquetados') }}
    group by 1
),

base as (
    select
        a.year       as year_x,
        a.gne        as gne_x,
        b.gne        as gne_x_minus_5,
        i.inflation  as inflation_x
    from gne_by_year a
    left join gne_by_year b
        on b.year = a.year - 5
    left join {{ ref('inflacion_anual') }} i
        on i.year = a.year
),

result as (
    select
        year_x,
        gne_x,
        gne_x_minus_5,
        inflation_x,
        case
            when gne_x_minus_5 is not null
             and inflation_x     is not null
             and (gne_x_minus_5 * (1 + inflation_x)) > 0
            then power( gne_x / ( gne_x_minus_5 * (1 + inflation_x) ), 1.0/4 ) - 1
            else null
        end as adjusted_rate_4yr
    from base
)

select
    year_x as year,
    gne_x,
    gne_x_minus_5,
    inflation_x,
    adjusted_rate_4yr,
    current_timestamp() as create_dttm
from result
order by year
