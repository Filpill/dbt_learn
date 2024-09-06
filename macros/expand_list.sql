{% macro expand_list(list_variable) %}{% for event in (list_variable) %} '{{ event }}'{% if not loop.last %},
        {% endif %} {% endfor %}{% endmacro %}