drop table twitter;

drop table twitter2;

create table twitter(uid int, fid int) row format delimited fields terminated by ',' stored as textfile;

create table twitter2(fid int, countuid int) row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table twitter;

INSERT INTO twitter2 SELECT twitter.fid,COUNT(twitter.uid) FROM twitter GROUP BY twitter.fid;

SELECT twitter2.countuid, COUNT(twitter2.fid) FROM twitter2 GROUP BY twitter2.countuid;







