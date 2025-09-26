/*
===========================================================================================================
model name : t1__finanzas__egresos_detallado_cp
author : Alejandro Morales Benavides
date : September 24th, 2025
usage : dbt build --select t1__finanzas__egresos_detallado_cp
objective :
 this model performs the following:
 1) creates a snowflake staging view for 1-1 mapping from source table (consejo_nl.staging.nuevo_leon_egresos_detallado) - CP VERSION.
dependencies :
 1) source table is present and is getting refreshed.
assumptions/notes :
 - airbyte columns are excluded.
 - source table is not altered.
 - CP version of egresos_detallado model.
===========================================================================================================
history :
-----------------------------------------------------------------------------------------------------------
name | date | project | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales | 08/08/2025 | consejo_nl_dbt | created raw tier 1 model from snowflake source.
Alejandro Morales | 09/24/2025 | consejo_nl_dbt | created CP version of egresos_detallado model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/
{{ config(
    materialized='view',
    tags=['finanzas', 'egresos_detallado', 'egresos_detallado_cp']
) }}

with source as (
    select
        surrogate_key,
        Codigo,
        Concepto,
        Aprobado,
        "AMPLIACIONES/REDUCCIONES",
        Modificado,
        Devengado,
        Pagado,
        Subejercicio,
        Fecha,
        Cuarto,
        Seccion
    from {{ source('staging', 'nuevo_leon_egresos_detallado_cp') }}
)

select * from source
