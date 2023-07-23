use job_data;
alter table `sql project-1 table - sheet1` rename project1;
select * from project1;
ALTER TABLE project1 Modify column ds date; 

select ds, sum(time_spent)/3600 as total_time_in_hour from project1 
where ds between '2020-11-01' and '2020-11-30'
group by ds order by ds;

select *, round(avg(avg_events_per_sec) over(), 5) as avg_rolling_pay from(
select ds, count(`event`) as total_event, sum(time_spent) as time_in_sec, 
count(`event`)/sum(time_spent) as avg_events_per_sec
from project1 group by 1 order by 1)t1;

select *, round(no_of_events/sum(t1.no_of_events) over() *100, 1) as percent_per_lang from 
(select `language`, count(`event`) as no_of_events  from project1 group by `language`)t1;

select * from 
(select *, count(job_id) over(partition by job_id)
 as count_ from project1)t1 where count_ > 1;
 
 SELECT DATA_TYPE FROM .COLUMNS job_data
  WHERE table_name = 'project1' AND COLUMN_NAME = 'ds';










