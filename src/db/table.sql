
create table dmapper(d_id int primary key not null , name text , vtime int);
create table rmapper(r_id int primary key not null , d_id int  , path varchar(16384) , vtime int , counter int);
create table data(r_id int primary key not null , title text , data text , size int , hash varchar(64) , new boolean);
create table linkr(r_id int primary key not null , num int , child_r_id int );
