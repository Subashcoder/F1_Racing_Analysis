{{
    config(
        materialized='incremental',
        unique_key='driver_number' 
    )
}}

SELECT 
    *
FROM 
    {{ ref('stg_Drivers_info') }} AS stg_drivers
{% if is_incremental() %}
WHERE driver_number NOT IN (SELECT driver_number FROM {{this}})
{% endif %}
