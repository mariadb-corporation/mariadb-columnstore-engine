--disable_query_log
--disable_warnings
-- SYSTABLE
create database if not exists calpontsys;

use calpontsys;

drop table if exists systable restrict;
drop table if exists syscolumn restrict;

create table if not exists systable (tablename varchar(128),
                       `schema` varchar(128),
                       objectid int,
                       createdate date,
                       lastupdate date,
                       init int,
                       next int,
                       numofrows int,
                       avgrowlen int,
                       numofblocks int,
                       autoincrement int,
                       auxcolumnoid int not null default 0) engine=columnstore comment='SCHEMA SYNC ONLY';

create table if not exists syscolumn (`schema` varchar(128),
                        tablename varchar(128),
                        columnname varchar(128),
                        objectid integer,
                        dictobjectid integer,
                        listobjectid integer,
                        treeobjectid integer,
                        datatype integer,
                        columnlength integer,
                        columnposition integer,
                        lastupdate date,
                        defaultvalue varchar(64),
                        nullable integer,
                        scale integer,
                        prec integer,
                        autoincrement char(1),
                        distcount integer,
                        nullcount integer,
                        minvalue varchar(64),
                        `maxvalue` varchar(64),
                        compressiontype integer,
                        nextvalue bigint,
                        charsetnum int not null default 0) engine=columnstore comment='SCHEMA SYNC ONLY';

--enable_warnings
--enable_query_log
