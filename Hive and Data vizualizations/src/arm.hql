CREATE TABLE movie_ratings_raw(mid INT,cid INT,rating INT,date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://cis6930dic/data/'

CREATE TABLE movie_ratings as
SELECT mid,cid,rating
FROM movie_ratings_raw
WHERE rating>4

create table FullFreqView as
select mid, count(cid) as freq, 1 as tmp from movie_ratings group by mid

create table SupportCountView as
select count(distinct cid) as cnt,1 as tmp1  from movie_ratings

create table FISet as
select mid as id,mid from FullFreqView f inner join SupportCountView s on f.tmp=s.tmp1 where freq>=cnt*0.050;

create table FISetFreq as
select id,freq,1 as size 
from (Select mid, count(cid) as freq from movie_ratings group by mid) temp inner join FISet
on FISet.mid=temp.mid;

create table tempFISet as 
select temp.id as id,mid	 
from FIset temp inner join FIsetFreq on temp.id=FIsetFreq.id 
where FIsetFreq.size=1

create table tempSet as
select distinct FISet1.id ID1, FISet2.id ID2 
from tempFISet FISet1 join tempFISet FISet2 
where FISet1.id <FISet2.id

create table temp as
select ID1,ID2,cast(concat(concat(ID1,"0"),ID2) as BigInt)as id,mid as mid from tempSet temp1 inner join FISet temp2 on temp2.id= temp1.ID1;

insert into table temp
select ID1,ID2,cast(concat(concat(ID1,"0"),ID2) as BigInt)as id,mid as mid from tempSet temp1 inner join FISet temp2 on temp2.id= temp1.ID2;

create table tempSetFinal as
select id1,id2,id from (select id1,id2,id,count(distinct mid) as cnt from temp1 tmp group by id1,id2,id) t2 where t2.cnt=2;

create table tempSetEle as
select mid, temp2.id as id from tempFISet temp1 inner join tempSetFinal temp2 on temp1.id=temp2.id2;

insert into table tempSetEle
select mid, temp2.id as id from tempFISet temp1 inner join tempSetFinal temp2 on temp1.id=temp2.id1;

create table dupTempItemList as
select distinct id,mid from tempSetEle;