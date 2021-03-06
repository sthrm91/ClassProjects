CREATE TABLE enron(id string, times string,fromheader String, toheader Array<String>,cc Array<String>,Subject String, context String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LOCATION 's3n://spring-2014-ds/enron_dataset/';

Create Table enron_cc as
select id, times, enron_cc
From enron LATERAL VIEW explode(cc) etab as enron_cc;


insert overwrite table cc_track
select enron_cc, count(*) as Num_cc from enron_cc group by enron_cc order by  Num_cc desc ;


insert overwrite table to_track
select enron_to, count(*) as Num_cc from enron_to group by enron_to order by  Num_cc desc ;


Create Table enron_to as
select id, times, from, enron_to
From enron LATERAL VIEW explode(toheader) etab as enron_to;

insert overwrite directory 's3n://cis6930dic/enronq7/'
select regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',3), count(distinct(id)) as total_mail  from enron_to 
group by regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',3);


Create table outside_mail as
Select id,times,enron_to from enron_to where enron_to not like  ’%enron%’;

insert overwrite directory 's3n://cis6930dic/enronq7/'
select regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',3), count(distinct(id)) as total_mail from outside_mail group by regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] 



insert overwrite directory 's3n://hiveql/enron'
select from_num+ sent+cced, from_num , sent, cced from busy_ppl order by from_num+ sent+cced desc; 
enron_cc



Create Table enr as
select id,fromheader, times, enron_to
From enron LATERAL VIEW explode(toheader) etab as enron_to;


create table comm as
select trim(rtrim(fromheader)) as initiator, trim(rtrim(enron_to)) as dest, count(id) as count from enr 
group by  trim(rtrim(fromheader)),trim(rtrim(enron_to)) ;


insert overwrite directory 's3n://sethuids/graphFinal'
select et.initiator, et.dest, e.count+et. count as count from comm et  join comm e on et.initiator=e.dest and et.dest=e.initiator;

