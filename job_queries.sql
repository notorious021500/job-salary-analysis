use jobs;
-- Dataset with 250k rows
select 
    *
from
    jobs.job_salary_prediction_dataset
limit 100;
-- 1 Number of roles(job titles) 
select 
    COUNT(distinct job_title) as total_roles
from
    job_salary_prediction_dataset;
-- 2 Average Salary of Data Analyst 
select 
    avg(salary) as avg_salary
from
    job_salary_prediction_dataset
where
    job_title = 'Data Analyst';
-- 3 Number of jobs present in Dataset
select 
    distinct job_title as roles
from
    job_salary_prediction_dataset;
-- 4 Average salary as per job_title in desc form
select job_title, avg(salary) as avg_salary
from job_salary_prediction_dataset
group by job_title
order by avg_salary desc;
-- 5 Average salary as per job_title for freshers
select job_title, avg(salary) as avg_salary
from job_salary_prediction_dataset
where experience_years = 0
group by job_title 
order by job_title desc;
-- 6 Average salary as per job_title for freshers with skills count
select job_title, avg(skills_count) as avg_skills_count, avg(salary) as avg_salary
from job_salary_prediction_dataset
where experience_years = 0
group by job_title 
order by avg_skills_count desc, job_title desc;
-- 7 Average salary as per job_title for freshers with certifications done
select job_title, round(avg(certifications)) as avg_certf, avg(salary) as avg_salary
from job_salary_prediction_dataset
where experience_years = '0'
group by job_title 
order by avg_certf desc, job_title desc;
-- 8 Average salary as per job title and industry
select job_title, industry, avg(salary) as avg_salary
from job_salary_prediction_dataset
group by job_title,industry
order by avg_salary desc;
-- 9 Average salary as industry
select industry, avg(salary) as avg_salary
from job_salary_prediction_dataset
group by industry
order by industry desc;
-- 10 Average salary as per Countries
select location, round(avg(salary)) as avg_salary
from job_salary_prediction_dataset
where location <> 'Remote'
group by location ;
-- 11 Average salary as per experience
select experience_years, round(avg(salary)) as avg_salary
from job_salary_prediction_dataset
group by experience_years
order by experience_years desc;
-- 12 Average salary divided into years of experience
select 
    case 
        when experience_years < 2 THEN '0-2 years'
        when experience_years BETWEEN 2 AND 5 THEN '2-5 years'
        when experience_years BETWEEN 6 AND 10 THEN '6-10 years'
        else '10+ years'
    end as experience_group,
    avg(salary) as avg_salary
from job_salary_prediction_dataset
group by experience_group;
-- 13 Average salary as per education level
select education_level, avg(salary) as avg_salary
from job_salary_prediction_dataset
group by education_level
order by avg_salary asc;
-- 14 Average salary as per skills_count
select skills_count, round(avg(salary)) as avg_salary
from job_salary_prediction_dataset
group by skills_count
order by skills_count asc;
-- 15 Average salary as per skills_count
select certifications, round(avg(salary)) as avg_salary
FROM job_salary_prediction_dataset
group by certifications
order by certifications asc;
-- 16 Fresher Average salary comparison with certifications and skills_count
select skills_count, certifications, round(avg(salary)) as avg_salary
from job_salary_prediction_dataset
where experience_years = '0'
group by skills_count,certifications
order by skills_count,certifications;
-- 17 Top earners as per job_title
select *
from (
    select *, 
           rank() over (partition by job_title order by salary desc) as rnk
    from job_salary_prediction_dataset
) t
where rnk = 1;
-- 18 Fraud 
select *,
       avg(salary) over (partition by job_title) as avg_salary
from job_salary_prediction_dataset
where salary > 2 * avg(salary) over (partition by job_title);
-- Kpis
select 
    COUNT(*) as total_jobs,
    avg(salary) as avg_salary,
    MAX(salary) as max_salary,
    MIN(salary) as min_salary
from job_salary_prediction_dataset;
-- 19 Top earners 
select *
from (
    select *, ntile(10) over (order by salary desc) as percentile_rank
    from job_salary_prediction_dataset
) t
where percentile_rank = 1;
-- 20 work type distribution 
select *,
    case 
        when remote_work = 'Yes' then 'Remote'
        when remote_work = 'No' then 'On-site'
        else 'Hybrid'
    end as work_type
from job_salary_prediction_dataset;
-- 21 Salary strucutre as per work type
select 
    case 
        when remote_work = 'Yes' then 'Remote'
        when remote_work = 'No' then 'On-site'
        else 'Hybrid'
    end as work_type,
    avg(salary) as avg_salary,
    COUNT(*) as total_jobs
from job_salary_prediction_dataset
group by work_type
order by avg_salary desc;
-- 22 View for remote_work_type
create view remote_salary as
select remote_work,
       avg(salary) as avg_salary,
       COUNT(*) as total_jobs
from job_salary_prediction_dataset
group by remote_work;
-- 23 View for average salary by job_role
create view salary_by_role as
select job_title,
       avg(salary) as avg_salary,
       MAX(salary) as max_salary,
       MIN(salary) as min_salary,
       COUNT(*) as total_jobs
from job_salary_prediction_dataset
group by job_title;
-- 24 View for Job Anomaly 
create view salary_anomaly as
select *
from (
    select *,
           avg(salary) over (partition by job_title) as avg_salary
    from job_salary_prediction_dataset
) t
where salary > 2 * avg_salary;
-- 25 Stored Procedure created for particular job title
DELIMITER //

CREATE PROCEDURE get_salary_by_role(IN role_name VARCHAR(100))
BEGIN
    SELECT *
    FROM job_salary_prediction_dataset
    WHERE job_title = role_name;
END //

DELIMITER ;

call get_salary_by_role('Data Analyst');

DELIMITER //
-- 26 Stored Procedure created for salary range
create procedure get_salary_range(in min_sal int, in max_sal int)
begin
    select *
    from job_salary_prediction_dataset
    where salary between min_sal and max_sal;
end //

DELIMITER ;

call get_salary_range(0,55500);

DELIMITER //
-- 27 Function created to chcek high salary
create function ishigh_salary(salary int)
returns varchar(10)
deterministic
begin
    if salary > 100000 then
        return 'High';
    else
        return 'Low';
    end if ;
end //

DELIMITER ;

select 
    salary, ISHIGH_SALARY(salary)
from
    job_salary_prediction_dataset
limit 10;
-- 28 comparing salary between same employees 
select a.job_title, a.salary as salary1, b.salary as salary2
from job_salary_prediction_dataset a
join job_salary_prediction_dataset b
on a.job_title = b.job_title
and a.salary < b.salary;
-- 29 comparing salary between as per experience  
select a.job_title, a.salary, b.salary as other_salary
from job_salary_prediction_dataset a
join job_salary_prediction_dataset b
on a.experience_years = b.experience_years;