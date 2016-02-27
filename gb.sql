drop table if exists t1;
create table t1(i int, j int, k int);
insert into t1 values(1, 10, 100);
insert into t1 values(1, 11, 110);
insert into t1 values(2, 20, 200);
insert into t1 values(2, 22, 220);
insert into t1 values(3, 30, 300);
insert into t1 values(3, 33, 330);
select i, sum(j), min(k) from t1 group by i;
