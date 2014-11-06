drop database if exists searcher;
create database searcher default character set utf8;
use searcher;

create table dmapper(d_id int primary key not null auto_increment, name text , vtime int default 0);
create table rmapper(r_id int primary key not null auto_increment, d_id int  , path varchar(16384) default '/', vtime int default 0, counter int default 0);

create table data(r_id int primary key not null , title text , data text , size int , hash varchar(64) , new boolean);

create table black(name varchar(512) not null);
create table white(name varchar(512) not null);

create table linkr(r_id int, num int , child_r_id int , primary key(r_id , num));


insert into dmapper values(1,'http://web.dendai.ac.jp',0);
insert into dmapper values(2,'http://www.sie.dendai.ac.jp',0);
insert into dmapper values(3,'https://www.cse.dendai.ac.jp',0);
insert into rmapper(r_id,d_id) values(1,1);
insert into rmapper(r_id,d_id) values(2,2);
insert into rmapper(r_id,d_id) values(3,3);
insert into white values("dendai.ac.jp");


create table nmapper(n_id int not null primary key auto_increment , noun text);
create table freq(n_id int , r_id int , freq float ,tfidf float, tstamp int default 0 ,primary key(n_id , r_id));
create table place (n_id int , r_id int , num int , place int , primary key(n_id , r_id , num));

create table pagerank(r_id int not null primary key , rank float default 0);

/* 今までに検索されたクエリを全て保持するテーブル  */
create table sword(w1 varchar(8192) not null , w2 varchar(8192) not null , counter int default 1);

/* data から 関連語のテーブルを事前に作る 上のテーブルと本質的に同じ様な用途*/
create table nounrelation(w1 varchar(8192) not null , w2 varchar(8192) not null , weight int default 0);


