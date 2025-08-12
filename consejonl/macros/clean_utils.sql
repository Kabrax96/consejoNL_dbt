{% macro clean_text(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{% macro clean_numeric(col) -%}
    -- Trata '', 'NULL', 'NaN' como NULL, elimina símbolos, maneja paréntesis (negativos contables)
    iff(
        regexp_like({{ col }}, '^\s*\(.*\)\s*$'),
        -1, 1
    ) * try_to_number(
          nullif(
            regexp_replace(
              iff(upper(trim({{ col }})) in ('', 'NULL', 'NAN'), null, trim({{ col }})),
              '[^0-9\.\-]',
              ''
            ),
            ''
          )
        )
{%- endmacro %}

{% macro clean_date(col) -%}
    -- Intenta varios formatos comunes antes de rendirse
    coalesce(
      try_to_date(nullif(trim({{ col }}), '')),
      to_date(nullif(trim({{ col }}), ''), 'YYYY-MM-DD'),
      to_date(nullif(trim({{ col }}), ''), 'DD/MM/YYYY'),
      to_date(nullif(trim({{ col }}), ''), 'MM/DD/YYYY')
    )
{%- endmacro %}
