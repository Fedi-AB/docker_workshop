-- Define a reusable dbt macro named "get_vendor_name"
-- It takes one argument: vendor_id (a column or value passed when calling the macro)
{% macro get_vendor_name(vendor_id) %}

-- Create a dictionary (key-value mapping) in Jinja
-- Keys are vendor IDs (integers)
-- Values are the corresponding vendor names (strings)
{% set vendors_names = {
    1: 'Creative Mobile Technologies, LLC',
    2: 'VeriFone Inc.',
    4: 'Unknown'
} %}


-- Start a SQL CASE statement
-- {{ vendor_id }} injects the column or value passed into the macro
case {{ vendor_id }}

    {% for vendor_id, vendor_name in vendors_names.items() %}
    -- Loop through each key-value pair in the dictionary
    -- vendor_id = dictionary key
    -- vendor_name = dictionary value

    when {{ vendor_id }} then '{{ vendor_name }}'
    -- For each vendor_id, generate a WHEN condition
    -- If the input matches the key, return the corresponding vendor_name

    {% endfor %}
    -- End of the loop
end
-- End of the CASE statement

-- End of the macro definition
{% endmacro %}

