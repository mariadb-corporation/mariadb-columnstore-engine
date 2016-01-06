#
# After importing physical partition 3...
# Drop physical partition 3 and verify 1 partition still exists
#
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 2 then 'good' else 'bad' end) q103 from tbug5237;
select calDropPartitionsByValue('tbug5237', 'batch', '3', '3');
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 1 then 'good' else 'bad' end) q104 from tbug5237;
