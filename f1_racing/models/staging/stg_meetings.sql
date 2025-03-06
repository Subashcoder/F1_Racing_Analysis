select 
    *
 FROM
    {{ source( 'F1RACING', 'f1_meeting') }}