set max_length_for_sort_data=5000;

drop table if exists d_60min_spseg;
drop table if exists f_60min_spseg_mpnc_pcsabytearray_none;
drop table if exists f_60min_43200_spseg_combined;
drop table if exists f_60min_43200_spseg_mpnc_pcsabytearray_none;

CREATE TABLE `d_60min_spseg` (
  `id` bigint(20) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `DistributionCenter` int(11) DEFAULT NULL,
  `Region` int(11) DEFAULT NULL,
  `Application` int(11) DEFAULT NULL,
  `ServiceProvider` int(11) DEFAULT NULL,
  `Segment` int(11) DEFAULT NULL,
  `Subscriber` bigint(20)
) ENGINE=InfiniDB ;

CREATE TABLE `f_60min_spseg_mpnc_pcsabytearray_none` (
  `tupleid` bigint(20) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `ByteArr_PCSA_AllValues` varbinary(4104) DEFAULT NULL
) ENGINE=InfiniDB;

CREATE TABLE `f_60min_43200_spseg_combined` (
  `tupleid` bigint(20) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `CN_mNone` double DEFAULT NULL,
  `CN_tNone` double DEFAULT NULL
) ENGINE=InfiniDB;

CREATE TABLE f_60min_43200_spseg_mpnc_pcsabytearray_none (
  `tupleid` bigint(20) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `ByteArr_PCSA_AllValues` varbinary(4104) DEFAULT NULL
) ENGINE=InfiniDB;

insert into d_60min_spseg 			  	values (11, 11, 11, 11, 11, 11, 11, 11);
insert into f_60min_spseg_mpnc_pcsabytearray_none 	values (11, 11, 11);
insert into f_60min_43200_spseg_mpnc_pcsabytearray_none values (11, 11, 11);
insert into f_60min_43200_spseg_combined		values (11, 11, 11, 11);

(SELECT  *
FROM
((SELECT D_60min_SPSeg.DistributionCenter, D_60min_SPSeg.Region, D_60min_SPSeg.Application, D_60min_SPSeg.ServiceProvider, D_60min_SPSeg.Segment, D_60min_SPSeg.Subscriber, F_60min_SPSeg_mpnc_PCSAByteArray_None.ByteArr_PCSA_AllValues
FROM D_60min_SPSeg, F_60min_SPSeg_mpnc_PCSAByteArray_None
WHERE (D_60min_SPSeg.id = F_60min_SPSeg_mpnc_PCSAByteArray_None.tupleid) AND
-- (F_60min_SPSeg_mpnc_PCSAByteArray_None.timestamp >= 1335873600 AND
-- F_60min_SPSeg_mpnc_PCSAByteArray_None.timestamp < 1335877200) )
(F_60min_SPSeg_mpnc_PCSAByteArray_None.timestamp >= 1 AND
F_60min_SPSeg_mpnc_PCSAByteArray_None.timestamp < 22) )
UNION ALL
(SELECT D_60min_SPSeg.DistributionCenter, D_60min_SPSeg.Region, D_60min_SPSeg.Application, D_60min_SPSeg.ServiceProvider, D_60min_SPSeg.Segment, D_60min_SPSeg.Subscriber, F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.ByteArr_PCSA_AllValues
FROM D_60min_SPSeg, F_60min_43200_SPSeg_mpnc_PCSAByteArray_None
WHERE (D_60min_SPSeg.id = F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.tupleid) AND
-- (F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.timestamp >= 1335830400 AND
-- F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.timestamp < 1335873600) ) )NONAGGR)
(F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.timestamp >= 1 AND
F_60min_43200_SPSeg_mpnc_PCSAByteArray_None.timestamp < 22) ) )NONAGGR)
ORDER BY DistributionCenter,Region,Application,ServiceProvider,Segment,Subscriber;

