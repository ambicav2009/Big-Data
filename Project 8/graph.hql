drop table M_Graph; 

create table M_Graph (
first_one int,
second_one int)

row format delimited fields terminated by ',' stored as textfile;

drop table Res_graph_first;

create table Res_graph_first (
inter_first int,
inter_second int)

row format delimited fields terminated by ',' stored as textfile;

drop table Res_graph_sec;

create table Res_graph_sec (
inter_sec_first int,
inter_sec_second int)

row format delimited fields terminated by ',' stored as textfile;


load data local inpath '${hiveconf:G}' overwrite into table M_Graph;

INSERT OVERWRITE TABLE Res_graph_first

select m.first_one,count(m.first_one) from M_Graph as m GROUP BY m.first_one;

INSERT OVERWRITE TABLE Res_graph_sec

select r.inter_second,count(r.inter_second) from Res_graph_first as r GROUP BY r.inter_second;



Select inter_sec_first, inter_sec_second from Res_graph_sec;
