CREATE TABLE movie_ratings_raw(mid INT,cid INT,rating INT,date ARRAY<INT>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '-'
LOCATION 's3n://spring-2014-ds/movie_dataset/movie_ratings/'

CREATE TABLE movie_title(mid INT,year INT,name String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3n://cis6930dic/data2/'

create table ratingVsPopular as
Select mid,count(distinct cid) popular, avg(rating) rating
from movie_ratings_raw
group by mid
order by popular

INSERT OVERWRITE DIRECTORY 's3n://cis6930dic/out1/'
Select t1.mid,t2.name,t1.popular,t1.rating
from ratingVsPopular t1 inner join movie_title t2
on t1.mid=t2.mid


create table monthly_movie as
select date[0] as year,date[1] as month,mid,avg(rating) as rating,count(cid) popular
from movie_ratings_raw
group by date[0],date[1],mid
order by popular

create table high_ranks as
select year,month,max(rating) rating
from monthly_movie
group by year,month

create table best_monthly as
select m.year,m.month,m.rating,m.mid,m.popular
from monthly_movie m inner join high_ranks h
on m.year=h.year and m.month=h.month and m.rating=h.rating
order by m.year,m.month;

create table high_popular as
select year,month,max(popular) popular
from best_monthly
group by year,month

create table monthly_best as
select m.year,m.month,m.rating,m.mid,m.popular
from best_monthly m inner join high_popular h
on m.year=h.year and m.month=h.month and m.popular=h.popular
order by m.year,m.month;

INSERT OVERWRITE DIRECTORY 's3n://cis6930dic/out3/'
Select m.year,m.month,m.rating,t.name,m.popular
from monthly_best m inner join movie_title t
on m.mid=t.mid

create table high_rating as
Select mid,cid,rating
from movie_ratings_raw
where rating=5

create table hallOfSHame as
select h.mid,r.rating,count (distinct h.cid) no
from high_rating h inner join (select * from ratingVsPopular where rating<=2) r on h.mid=r.mid
group by h.mid,low_rating

INSERT OVERWRITE DIRECTORY 's3n://cis6930dic/out6/'
Select t2.mid,t2.name,t1.no,t1.rating
from hallOfSHame t1 inner join movie_title t2
on t1.mid=t2.mid

INSERT OVERWRITE DIRECTORY 's3n://cis6930dic/out4/'
select date[0] as year,date[1] as month,count(distinct mid) movies,count(distinct cid) ratings
from movie_ratings_raw
group by date[0],date[1]
order by year,month