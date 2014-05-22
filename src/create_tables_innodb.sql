CREATE DATABASE IF NOT EXISTS tsdb;
USE tsdb;

CREATE TABLE IF NOT EXISTS `metric` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(1024) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tagk` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tagv` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(1024) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tsdb` (
  `val_int` bigint(20) unsigned,
  `val_dbl` double(20,4),
  `ts` bigint NOT NULL,
  `metricid` bigint unsigned NOT NULL,
  `hostid` bigint unsigned,
  `t0_valueid` bigint unsigned,
  `t1_valueid` bigint unsigned,
  `t2_valueid` bigint unsigned,
  `t3_valueid` bigint unsigned,
  `t4_valueid` bigint unsigned,
  `t5_valueid` bigint unsigned,
  `t6_valueid` bigint unsigned
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE INDEX IF NOT EXISTS index1 ON tsdb (metricid,hostid);
CREATE INDEX IF NOT EXISTS index2 ON tsdb (hostid,metricid);

