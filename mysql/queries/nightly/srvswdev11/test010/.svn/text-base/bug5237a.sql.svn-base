#
# After importing physical partitions 1 and 2...
# Disable physical partition 2 and verify 1 partition still exists
#
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 2 then 'good' else 'bad' end) q101 from tbug5237;
select calDisablePartitionsByValue('tbug5237', 'batch', '2', '2');
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 1 then 'good' else 'bad' end) q102 from tbug5237;
