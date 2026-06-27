{# ================================================================
   generate_schema_name (override)
   ================================================================
   По умолчанию dbt склеивает базу как
       {default_schema}_{custom_schema}
   Это приводит к именам вида "dpib_silver", если default_schema
   указан как "dpib" и в модели стоит "+schema: silver".

   Логика тут явная: маппим короткие имена слоёв на полные имена
   баз ClickHouse. Если кто-то поставит "+schema: something_else" —
   получит "dpib_something_else" (стандартное поведение dbt),
   что подскажет об ошибке маппинга.
   ================================================================ #}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif custom_schema_name | trim == 'silver' -%}
        dpib_silver

    {%- elif custom_schema_name | trim == 'gold' -%}
        dpib_gold

    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
