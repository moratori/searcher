drop database crawler;
create database crawler default character set utf8;
use crawler;
create table dmapper(d_id int primary key not null , name text , vtime int default 0);
create table rmapper(r_id int primary key not null , d_id int  , path varchar(16384) default '/', vtime int default 0, counter int default 0);
create table data(r_id int primary key not null , title text , data text , size int , hash varchar(64) , new boolean);
create table linkr(r_id int, num int , child_r_id int );
alter table linkr add constraint primary key(r_id,num);

insert into dmapper values(1,'http://web.dendai.ac.jp',0);
insert into rmapper(r_id,d_id) values(1,1);
