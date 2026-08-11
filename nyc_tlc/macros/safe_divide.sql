{% macro safe_divide(numerator,denominator) %}

{{numerator}} / NULLIF( {{denominator}}, 0 )

{% endmacro %}