/*
===========================================================================================================
Model Name : t1__finanzas__ingresos_detallado_cp
Author : Alejandro Morales Benavides
Date : September 24th, 2025
Usage : dbt build --select t1__finanzas__ingresos_detallado_cp
Objective :
 - Create a 1:1 staging view from CONSEJO_NL.STAGING.NUEVO_LEON_INGRESOS_DETALLADO - CP VERSION
 excluding Airbyte metadata columns, and applying minimal technical cleaning to handle NaNs and blanks.
Dependencies :
 - Source: source('staging', 'nuevo_leon_ingresos_detallado')
Assumptions/Notes :
 - No business transformations here.
 - Minimal cleaning performed via macros:
 * clean_date() for FECHA
 * clean_text() for text columns
 * clean_numeric() for numeric-like strings (handles symbols and parentheses)
 - CP version of ingresos_detallado model.
===========================================================================================================
History :
-----------------------------------------------------------------------------------------------------------
Name | Date | Project | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides | 2025-08-12 | consejonl | Created Tier 1 staging model with reusable cleaning macros.
Alejandro Morales Benavides | 2025-09-24 | consejonl | Created CP version of ingresos_detallado model.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{{ config(
    materialized='view',
    tags=['finanzas', 'ingresos_detallado', 'ingresos_detallado_cp']
) }}

with src as (
    select
        -- Raw columns from staging table (excluding Airbyte metadata)
        fecha,
        cuarto,
        seccion,
        concepto,
        estimado,
        devengado,
        recaudado,
        diferencia,
        modificado,
        surrogate_key,
        clave_primaria,
        clave_secundaria,
        ampliaciones_reducciones
    from {{ source('staging', 'nuevo_leon_ingresos_detallado_cp') }}
),

normalized as (
    select
        {{ clean_date('fecha') }} as fecha,
        {{ clean_text('cuarto') }} as cuarto,
        {{ clean_text('seccion') }} as seccion,
        {{ clean_text('concepto') }} as concepto,
        {{ clean_numeric('estimado') }} as estimado,
        {{ clean_numeric('devengado') }} as devengado,
        {{ clean_numeric('recaudado') }} as recaudado,
        {{ clean_numeric('diferencia') }} as diferencia,
        {{ clean_numeric('modificado') }} as modificado,
        {{ clean_text('surrogate_key') }} as surrogate_key,
        {{ clean_text('clave_primaria') }} as clave_primaria,
        {{ clean_text('clave_secundaria') }} as clave_secundaria,
        {{ clean_numeric('ampliaciones_reducciones') }} as ampliaciones_reducciones
    from src
)

select *
from normalized
