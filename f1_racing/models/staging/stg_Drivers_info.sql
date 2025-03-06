select 
    * 
FROM
    {{source('F1RACING', 'Driver_INFO')}}
where driver_number is not NULL and team_name is not null
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
