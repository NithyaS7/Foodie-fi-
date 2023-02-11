create schema foodie_fi ;
use foodie_fi;
create table plans(
plan_id SMALLINT unsigned,
plan_name VARCHAR(45) NOT NULL,
price float,
PRIMARY KEY  (plan_id)
);

create table subscriptions(
customer_id SMALLINT unsigned,
plan_id SMALLINT unsigned,
start_date date NOT NULL ,
FOREIGN KEY (plan_id)  REFERENCES plans (plan_id)    ON DELETE CASCADE

);
INSERT INTO plans (
plan_id,plan_name,price)
VALUES
(0, 'trial', 0), 
(1,'basic monthly',9.90),
(2, 'pro monthly',19.90),
(3, 'pro annual', 199),
( 4, 'churn', 0
 );
select* from subscriptions;
insert into subscriptions(
customer_id,plan_id,start_date)
VALUES
( 1,	0,	'2020-08-01'),
(1,	    0,	'2020-08-01'),
(1,	    1,	'2020-08-08'),
(2,	    0,	'2020-09-20'),
(2,	    3,	'2020-09-27'),
(11,    0,	'2020-11-19'),
(11,	4,	'2020-11-26'),
(13,	0,	'2020-12-15'),
(13,	1,	'2020-12-22'),
(13,	2,	'2021-03-29'),
(15,	0,	'2020-03-17'),
(15,	2,	'2020-03-24'),
(15,	4,	'2020-04-29'),
(16,	0,	'2020-05-31'),
(16,	1,	'2020-06-07'),
(16,	3,	'2020-10-21'),
(18,	0,	'2020-07-06'),
(18,	2,	'2020-07-13'),
(19,	0,	'2020-06-22'),
(19,	2,	'2020-06-29'),
(19,	3,	'2020-08-29');

-- inner join
SELECT
  s.customer_id,
  f.plan_id, 
  f.plan_name,  
  s.start_date
FROM foodie_fi.plans AS f
JOIN foodie_fi.subscriptions AS s
  ON f.plan_id = s.plan_id
WHERE 
  s.customer_id IN (1,2,11,13,15,16,18,19)


-- 1. How many customers has Foodie-Fi ever had?
SELECT  COUNT(DISTINCT customer_id) AS total_customers
FROM foodie_fi.subscriptions;

-- 2. What is the monthly distribution of trial plan start_date values for our
--    dataset - use the start of the month as the group by value

SELECT EXTRACT(MONTH FROM start_date) AS months, COUNT(*)
FROM foodie_fi.subscriptions
WHERE plan_id = 0
GROUP BY months
ORDER BY months;

-- 3.  What plan start_date values occur after the year 2020 for our dataset? Show the 
--     breakdown by count of events for each plan_name

SELECT P.plan_id, P.plan_name, count(*) AS count_event FROM plans P JOIN subscriptions S ON P.plan_id = S.plan_id 
WHERE S.start_date > "2020-12-31" GROUP BY P.plan_id, P.plan_name ORDER BY P.plan_id;

-- 4.  What is the customer count and percentage of customers who have churned rounded 
--     to 1 decimal place?

SELECT COUNT(*) AS cust_churn, ROUND(COUNT(*)*100/(SELECT COUNT(DISTINCT customer_id) FROM subscriptions),1) AS percen_churn 
 FROM subscriptions WHERE plan_id = 4;
 
 -- 5. How many customers have churned straight after their initial free trial
--  - what percentage is this rounded to the nearest whole number?

WITH cte_churn AS (SELECT *, LAG(plan_id, 1) OVER(partition by customer_id order by plan_id) AS prev_plan FROM subscriptions) 
 SELECT COUNT(prev_plan) AS cnt_churn, ROUND(COUNT(*)*100/(SELECT COUNT(DISTINCT customer_id) FROM subscriptions),0) AS percn_churn FROM cte_churn
 WHERE plan_id = 4 AND prev_plan=0;

-- 6. What is the number and percentage of customer plans after their initial free trial?
WITH cte_next_plan AS (SELECT *,  LEAD(plan_id, 1) OVER(partition by customer_id order by plan_id) AS next_plan FROM subscriptions)
SELECT next_plan, COUNT(*) AS num_cust, ROUND(COUNT(*) * 100/(SELECT COUNT(DISTINCT customer_id) FROM subscriptions),1) AS perc_next_plan FROM cte_next_plan
WHERE next_plan is not null and plan_id = 0 group by next_plan order by next_plan;

-- 7. What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31?
WITH cte_next_date AS(SELECT *, LEAD(start_date, 1) OVER(partition by customer_id order by start_date) AS next_date FROM subscriptions WHERE start_date <= "2020-12-31"), plans_breakdown AS(
 SELECT plan_id, COUNT(DISTINCT customer_id) AS num_customer FROM cte_next_date WHERE (next_date is not null AND (start_date < "2020-12-31" AND next_date > "2020-12-31")) OR (next_date is null and start_date <"2020-12-31") GROUP BY plan_id)
 SELECT plan_id, num_customer, ROUND(num_customer * 100 / (SELECT COUNT(DISTINCT customer_id) FROM subscriptions), 1) AS perc_customer
 FROM plans_breakdown group by plan_id, num_customer order by plan_id;
 
 -- 8. How many customers have upgraded to an annual plan in 2020?
 SELECT COUNT(customer_id) FROM subscriptions WHERE plan_id = 3 AND start_date <= "2020-12-31";

-- 9. How many days on average does it take for a customer to an annual plan from the day they join Foodie-Fi?

-- This will only find the average of people who have upgraded to annual plan
WITH annual_plan AS( SELECT customer_id, start_date AS Annual_date FROM subscriptions WHERE plan_id = 3), trial_plan AS
(SELECT customer_id, start_date AS trial_date FROM subscriptions WHERE plan_id = 0) SELECT ROUND(AVG(DATEDIFF(annual_date, trial_date)),0) AS avg_upgrade FROM annual_plan ap
JOIN trial_plan tp ON ap.customer_id = tp.customer_id;

-- 10. Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)
WITH annual_plan AS( SELECT customer_id, start_date AS Annual_date FROM subscriptions WHERE plan_id = 3), trial_plan AS
(SELECT customer_id, start_date AS trial_date FROM subscriptions WHERE plan_id = 0), day_period AS( SELECT DATEDIFF(annual_date, trial_date) AS diff FROM trial_plan tp
LEFT JOIN annual_plan ap ON tp.customer_id = ap.customer_id WHERE annual_date is not null),bins AS(SELECT *, FLOOR(diff/30) AS bins FROM day_period) 
SELECT CONCAT((bins*30)+1, '-',(bins+1)*30,'days') AS days, COUNT(diff) AS total from bins group by bins;

-- 11.	How many customers downgraded from a pro monthly to a basic monthly plan in 2020?
WITH next_plan AS( SELECT *, LEAD(plan_id,1) OVER(partition by customer_id order by start_date, plan_id) AS plan FROM subscriptions)
SELECT COUNT(distinct customer_id) AS num_downgrade FROM next_plan np LEFT JOIN plans p ON p.plan_id = np.plan_id
WHERE p.plan_name = "pro monthly" AND np.plan = 1 AND start_date <= "2020-12-31";
-- payment table
create view customer_id1 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id =1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 1 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 2 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 3 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 4 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 1));

create view customer_id2 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 2));

create view customer_id13 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using (plan_id) where plan_id not in (0,4) and customer_id = 13 limit 1));

create view customer_id15 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 15) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 1 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 15));


create view customer_id16 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 16 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 1 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 16 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 2 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 16 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 3 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 16 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 4 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 16 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 5 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 3 and customer_id = 16));


create view customer_id18 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 1 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 2 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 3 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 4 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 5 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id = 2 and customer_id = 18));


create view customer_id19 as (
(select customer_id, plan_id, plan_name, start_date as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 19 limit 1) union all
(select customer_id, plan_id, plan_name, date_add(start_date, interval 1 month) as "payment_date", price as "amount" from plans join subscriptions using(plan_id) where plan_id not in (0,4) and customer_id = 19)) ;


create table payment as (
select *, rank() over(order by payment_date) as "payment_order" from customer_id1 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id2 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id13 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id15 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id16 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id18 union all
select *, rank() over(order by payment_date) as "payment_order" from customer_id19);

select * from payment;