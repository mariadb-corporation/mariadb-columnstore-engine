select count(*), min(start), max(stop), timediff(max(stop), min(start)) from test006_load where stop is not null;
