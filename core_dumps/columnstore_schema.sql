DROP DATABASE IF EXISTS `columnstore_bts`;

CREATE DATABASE `columnstore_bts`;

USE `columnstore_bts`;

CREATE TABLE `airlines` (
  `iata_code` varchar(2) DEFAULT NULL,
  `airline` varchar(30) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

CREATE TABLE `airports` (
  `iata_code` varchar(3) DEFAULT NULL,
  `airport` varchar(80) DEFAULT NULL,
  `city` varchar(30) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `latitude` decimal(11,4) DEFAULT NULL,
  `longitude` decimal(11,4) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

CREATE TABLE `flights` (
  `year` smallint(6) DEFAULT NULL,
  `month` tinyint(4) DEFAULT NULL,
  `day` tinyint(4) DEFAULT NULL,
  `day_of_week` tinyint(4) DEFAULT NULL,
  `fl_date` date DEFAULT NULL,
  `carrier` varchar(2) DEFAULT NULL,
  `tail_num` varchar(6) DEFAULT NULL,
  `fl_num` smallint(6) DEFAULT NULL,
  `origin` varchar(5) DEFAULT NULL,
  `dest` varchar(5) DEFAULT NULL,
  `crs_dep_time` varchar(4) DEFAULT NULL,
  `dep_time` varchar(4) DEFAULT NULL,
  `dep_delay` decimal(13,2) DEFAULT NULL,
  `taxi_out` decimal(13,2) DEFAULT NULL,
  `wheels_off` varchar(4) DEFAULT NULL,
  `wheels_on` varchar(4) DEFAULT NULL,
  `taxi_in` decimal(13,2) DEFAULT NULL,
  `crs_arr_time` varchar(4) DEFAULT NULL,
  `arr_time` varchar(4) DEFAULT NULL,
  `arr_delay` decimal(13,2) DEFAULT NULL,
  `cancelled` decimal(13,2) DEFAULT NULL,
  `cancellation_code` varchar(20) DEFAULT NULL,
  `diverted` decimal(13,2) DEFAULT NULL,
  `crs_elapsed_time` decimal(13,2) DEFAULT NULL,
  `actual_elapsed_time` decimal(13,2) DEFAULT NULL,
  `air_time` decimal(13,2) DEFAULT NULL,
  `distance` decimal(13,2) DEFAULT NULL,
  `carrier_delay` decimal(13,2) DEFAULT NULL,
  `weather_delay` decimal(13,2) DEFAULT NULL,
  `nas_delay` decimal(13,2) DEFAULT NULL,
  `security_delay` decimal(13,2) DEFAULT NULL,
  `late_aircraft_delay` decimal(13,2) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;