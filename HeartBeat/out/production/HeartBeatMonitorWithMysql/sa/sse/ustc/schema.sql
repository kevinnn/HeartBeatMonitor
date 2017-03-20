create database if not exists smart;

use smart;

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
  
# insert into datanodeStatus values (0, 1, true, 2, 5, "localhost", 8888, 8889, now());