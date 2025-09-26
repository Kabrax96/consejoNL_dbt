/*
===========================================================================================================
Model Name          : t3__finanzas__ip_sobre_ild_annual_cp
Author              : Alejandro Morales Benavides
Date                : September 24th, 2025
Usage               : dbt build --select t3__finanzas__ip_sobre_ild_annual_cp

Objective           :
    This model - CP VERSION (Annual Aggregation):
      1) Computes Ingresos Propios (IP) = A. IMPUESTOS + D. DERECHOS + E. PRODUCTOS + F. APROVECHAMIENTOS.
      2) Computes Ingresos de Libre Disposición (ILD).
      3) Outputs the ratio IP / ILD * 100 aggregated by calendar year.

Dependencies        :
    - Source model: t2__finanzas__ingresos_detallado_cp

Assumptions/Notes   :
    - Concepts matched case-insensitively, accents removed.
    - ILD uses exact match for concept names found in data.
    - Uses absolute value of ILD for percentage calculation.
    - Annual aggregation - one record per year.
    - CP version of ip_sobre_ild model (annual grain).

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-09-24  | consejonl   | Created CP version of IP/ILD annual model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "table",
        snowflake_warehouse = "COMPUTE_WH",
        tags=['finanzas', 'ip_sobre_ild', 't3', 'ip_sobre_ild_cp', 'annual'],
        pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
        post_hook = [
            "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
        ]
    )
}}

with t2 as (
    select
        fecha,
        concepto,
        recaudado
    from {{ ref('t2__finanzas__ingresos_detallado_cp') }}
),
norm as (
    select
        fecha,
        upper(translate(concepto,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ', 'AEIOUAEIOUAEIOUAEIOUNN')) as concepto_norm,
        recaudado
    from t2
),
by_year as (
    select
        date_part(year, fecha)::int as year,
        sum(case
            when concepto_norm in ('A. IMPUESTOS','D. DERECHOS','E. PRODUCTOS','F. APROVECHAMIENTOS')
            then coalesce(recaudado,0)
            else 0
        end) as ip_recaudado,
        sum(case
            when concepto_norm in ('I. TOTAL DE INGRESOS DE LIBRE DISPOSICION (I=A+B+C+D+E+F+G+H+I+J+K+L)', 'INGRESOS DE LIBRE DISPOSICION')
            then coalesce(recaudado,0)
            else 0
        end) as ild_recaudado
    from norm
    group by 1
)
select
    year,
    ip_recaudado,
    ild_recaudado,
    case when abs(ild_recaudado) > 0 then (ip_recaudado / ild_recaudado) * 100 else 0 end as ip_sobre_ild_pct,
    current_timestamp() as create_dttm
from by_year
order by year
