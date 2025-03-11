{{
    config(
        materialized='incremental',
        unique_key='meetingkey' 
    )
}}

select * FROM
{{ref('stg_meetings')}} as stg_meeting
{% if is_incremental() %}
LEFT JOIN {{this}} as meeting 
on stg_meeting.meetingkey = meeting.meetingkey
where meeting.meeting is NULL
{% endif %}