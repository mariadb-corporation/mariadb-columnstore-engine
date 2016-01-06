select min(start), max(stop), timediff(max(stop), min(start)) from test006_query;

select min(start), max(stop), timediff(max(stop), min(start)) from test006_load;

select min(start), max(stop), timediff(max(stop), min(start)) from
(select min(start) start, max(stop) stop, timediff(max(stop), min(start)) from test006_query union
select min(start) start, max(stop) stop, timediff(max(stop), min(start)) from test006_load) sub;
