create database if not exists DataNode;

use DataNode;

drop table if exists datanodeStatus;
create table datanodeStatus (
  groupNumber smallint not null,
  nodeNumber int not null,
  isNormal bool not null default true,
  exCode  smallint not null default 0,
  intervals smallint not null default 0,
  host varchar(50),
  connectPort int,
  heartPort int,
  updateTime timestamp not null,
  primary key (groupNumber, nodeNumber)
 );
  