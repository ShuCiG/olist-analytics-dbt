{#
    Fails rows where `column_name` is earlier than `earlier_column`.
    Rows where either value is null pass (the comparison is null), so the test
    applies to optional milestones such as delivery timestamps.
#}
{% test is_not_before(model, column_name, earlier_column) %}

    select *
    from {{ model }}
    where {{ column_name }} < {{ earlier_column }}

{% endtest %}
