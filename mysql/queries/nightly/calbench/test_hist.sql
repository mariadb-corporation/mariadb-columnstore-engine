select date_format(StartTime,'%Y%m%d') date, 
	description, 
	count(*), 
	sum(elapsedtime), 
	max(id) 
from run_hist 
group by 1,2 
order by max(id);