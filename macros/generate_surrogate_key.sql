{% macro generate_surrogate_key(field_list) %}
    {#- 
        Generates a surrogate key from a list of fields using MD5 hash.
        Handles null values by coalescing to empty string.
        
        Usage: {{ generate_surrogate_key(['field1', 'field2']) }}
    -#}
    
    {% set fields = [] %}
    
    {% for field in field_list %}
        {% do fields.append(
            "coalesce(cast(" ~ field ~ " as varchar), '')"
        ) %}
    {% endfor %}
    
    {{ dbt_utils.generate_surrogate_key(field_list) if var('use_dbt_utils', false) else "md5(" ~ fields | join(" || '|' || ") ~ ")" }}
    
{% endmacro %}
