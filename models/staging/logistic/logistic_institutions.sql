select distinct 
    instituto 
from
{{ref('logistics_lined')}} 
order by 
    instituto desc