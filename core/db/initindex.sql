use searcher;
drop table if exists nmapper;
drop table if exists freq;
drop table if exists place;

drop table if exists pagerank;

update data set new = 1;

create table nmapper(n_id int not null primary key auto_increment , noun text);
create table freq(n_id int , r_id int , freq float ,tfidf float, tstamp int default 0 ,primary key(n_id , r_id));
create table place (n_id int , r_id int , num int , place int , primary key(n_id , r_id , num));

create table pagerank(r_id int not null primary key , rank float default 0);
