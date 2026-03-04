{%  macro get_ratecodes(ratecode_id) %}
case 
    when {{ratecode_id}} = 1 then 'Standard rate'
    when {{ratecode_id}} = 2 then 'JFK'
    when {{ratecode_id}} = 3 then 'Newark'
    when {{ratecode_id}} = 4 then 'Nassau or Westchester'
    when {{ratecode_id}} = 5 then 'Negotiated fare'
    when {{ratecode_id}} = 6 then 'Group ride'
    when {{ratecode_id}} = 99 then 'Null/unknown'
    else 'Unknown'
end 
{% endmacro %}