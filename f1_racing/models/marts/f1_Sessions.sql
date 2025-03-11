{{
    config(
        materialized='incremental',
        unique_key='sessionkey' 
    )
}}

select * from {{ref('stg_sessions')}} as staging
{% if is_incremental() %}
left join {{this}} as existing 
on staging.sessionkey = existing.sessionkey 
where existing.sessionkey is null
{% endif %}