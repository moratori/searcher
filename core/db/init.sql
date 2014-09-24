drop database if exists searcher;
create database searcher default character set utf8;
use searcher;
create table dmapper(d_id int primary key not null , name text , vtime int default 0);
create table rmapper(r_id int primary key not null , d_id int  , path varchar(16384) default '/', vtime int default 0, counter int default 0);
create table data(r_id int primary key not null , title text , data text , size int , hash varchar(64) , new boolean);
create table black(d_id int not null primary key);
create table white(d_id int not null primary key);
create table linkr(r_id int, num int , child_r_id int , primary key(r_id , num));

insert into dmapper values(1,'http://web.dendai.ac.jp',0);
insert into dmapper values(2,'http://www.sie.dendai.ac.jp',0);
insert into dmapper values(3,'https://www.cse.dendai.ac.jp',0);
insert into rmapper(r_id,d_id) values(1,1);
insert into rmapper(r_id,d_id) values(2,2);
insert into rmapper(r_id,d_id) values(3,3);


create table nmapper(n_id int not null primary key auto_increment , noun text);
create table freq(n_id int , r_id int , freq float ,tfidf float, tstamp int default 0 ,primary key(n_id , r_id));
create table place (n_id int , r_id int , num int , place int , primary key(n_id , r_id , num));