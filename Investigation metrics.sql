use inves_metric_spike;
SET SQL_SAFE_UPDATES = 0;
update events2 set user_type = NULL where user_type = 0;
SET SQL_SAFE_UPDATES = 1;
select user_id, user_type from events2 where user_type is null;
select * from users;
select * from event_table;
select * from email;


select week(occurred_at) as week_, avg(count_) from(
select occurred_at, user_id, count(event_type)
 over(partition by user_id)as count_ from event_table where event_type = 'engagement')t1 group by 1;
 
 
 
select week(occurred_at) as week_, count(user_id) as new_user from event_table 
where  event_name = 'create_user' group by week_;

select first_date as week_number,
count(id) as total_count,
sum(case when week_ = 0 then 1 else 0 end) as 'week0',
sum(case when week_ = 1 then 1 else 0 end) as 'week1',
sum(case when week_ = 2 then 1 else 0 end) as 'week2',
sum(case when week_ = 3 then 1 else 0 end) as 'week3',
sum(case when week_ = 4 then 1 else 0 end) as 'week4',
sum(case when week_ = 5 then 1 else 0 end) as 'week5',
sum(case when week_ = 6 then 1 else 0 end) as 'week6',
sum(case when week_ = 7 then 1 else 0 end) as 'week7',
sum(case when week_ = 8 then 1 else 0 end) as 'week8',
sum(case when week_ = 9 then 1 else 0 end) as 'week9',
sum(case when week_ = 10 then 1 else 0 end) as 'week10',
sum(case when week_ = 11 then 1 else 0 end) as 'week11',
sum(case when week_ = 12 then 1 else 0 end) as 'week12',
sum(case when week_ = 13 then 1 else 0 end) as 'week13',
sum(case when week_ = 14 then 1 else 0 end) as 'week14',
sum(case when week_ = 15 then 1 else 0 end) as 'week15',
sum(case when week_ = 16 then 1 else 0 end) as 'week16',
sum(case when week_ = 17 then 1 else 0 end) as 'week17',
sum(case when week_ = 18 then 1 else 0 end) as 'week18' from
(select a.id ,b.user_id, b.repeat_date, a.first_date, b.repeat_date - first_date as week_ from(
select user_id as id, week(occurred_at) as first_date from event_table where event_name = 'complete_signup' )a left join
(select distinct(user_id), week(occurred_at) as repeat_date from 
event_table where event_type = 'engagement')b 
on a.id = b.user_id) t1  group by first_date order by first_date;



select week_num, device, no_of_events, avg(no_of_events) 
over(partition by week_num) as avg_events_per_week from( 
select
week(occurred_at) as week_num,
device,
count(event_name) as no_of_events
from event_table where event_type = 'engagement'
group by 1,2)t1 ;

select week(t1.occurred_at), event_type, count(distinct(t1.user_id)) from 
(select user_id, occurred_at, `action`
from email)t1 inner join
(select user_id, occurred_at, event_type
from event_table)t2 on 
t1.user_id = t2.user_id  where event_type is not null group by 1,2;
               
               








