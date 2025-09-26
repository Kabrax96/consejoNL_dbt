/*
===========================================================================================================
model name : t1__finanzas__balance_presupuestario_cp
author : Alejandro Morales Benavides
date : September 24th, 2025
usage : dbt build --select t1__finanzas__balance_presupuestario_cp
objective :
 this model performs the following:
 1) creates a snowflake staging view for 1-1 mapping from source table (consejo_nl.staging.nuevo_leon_balance_presupuestario) - CP VERSION.
dependencies :
 1) source table is present and is getting refreshed.
assumptions/notes :
 - airbyte columns are excluded.
 - source table is not altered.
 - CP version of balance_presupuestario model.
===========================================================================================================
history :
-----------------------------------------------------------------------------------------------------------
name | date | project | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales | 08/06/2025 | consejo_nl_dbt | created raw tier 1 model from snowflake source.
Alejandro Morales | 09/24/2025 | consejo_nl_dbt | created CP version of balance_presupuestario model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/
{{ config(
    materialized='view',
    tags=['finanzas', 'balance_presupuestario', 'balance_presupuestario_cp']
) }}

with source as (
    select
        surrogate_key,
        concept,
        sublabel,
        year_quarter,
        full_date,
        type,
        amount
    from {{ source('staging', 'nuevo_leon_balance_presupuestario_cp') }}
)

select * from source
