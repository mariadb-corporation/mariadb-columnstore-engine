#
# After importing physical partition 4...
# Enable physical partition 2 and verify 3 partitions still exist
#
#select calflushcache();
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 2 then 'good' else 'bad' end) q105 from tbug5237;
select calEnablePartitionsByValue('tbug5237', 'batch', '2', '2');
select distinct batch from tbug5237;
select count(distinct batch), (case count(distinct batch) when 3 then 'good' else 'bad' end) q106 from tbug5237;
