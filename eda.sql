use world_layoffs;

select *
from layoffs_staging2;

-- Time Frame of the Data Set
select max(`date`), min(`date`)
from layoffs_staging2;-- from 11/03/2020 to 06/03/2023

-- Understanding Maximum and Minimum number of Lay Offs
select max(total_laid_off), min(total_laid_off)
from layoffs_staging2; -- Max 12000, Min 3

select max(percentage_laid_off), min(percentage_laid_off)
from layoffs_staging2; -- Max 1, Min 0

-- Companies that got completely out of business
select *
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off DESC;

select count(percentage_laid_off)
from layoffs_staging2
where percentage_laid_off=1;-- 116 Companies went out of Business

-- Top 10 Companies to Layoff Employees
select company,sum(total_laid_off) as total
from layoffs_staging2
group by company
order by total DESC limit 10;-- Above 10k: Amazon, Google, Meta, Salesforce, Microsoft, Philips. Below 10k:Ericsson, Uber, Dell, Booking.com

-- Top 5 Industries to Layoff Employees
select industry,sum(total_laid_off) as total
from layoffs_staging2
group by industry
order by total DESC limit 5;-- Consumer, Retail, Other, Transportation, Finance

-- Bottom 5 Industries to Layoff Employees
select industry,sum(total_laid_off) as total
from layoffs_staging2
where industry is not null
group by industry
order by total ASC limit 5;-- Manufacturing, Fin-Tech, Aerospace, Energy, Legal

-- Top 5 Countries to Layoff Employees
select country,sum(total_laid_off) as total
from layoffs_staging2
group by Country
order by total DESC limit 5;-- United States, India, Netherlands, Sweden, Brazil

-- Bottom 5 Countries to Layoff Employees
with cte_country_total as
(
select country,sum(total_laid_off) as total
from layoffs_staging2
group by Country
order by total ASC
)
select country, total
from cte_country_total
where total is not null limit 5;-- Poland, Chile, New Zealand, Luxemborg, Thailand

-- Yearwise distribution of Layoffs
select YEAR(`date`),sum(total_laid_off) as total
from layoffs_staging2
where YEAR(`date`) is not null
group by YEAR(`date`)
order by 1 DESC;-- 2023: 125k, 2022: 160k, 2021: 15k, 2020: 80k

-- Top 5 Company Stages where layoffs happened
select stage,sum(total_laid_off) as total
from layoffs_staging2
group by stage
order by total DESC;-- Post-IPO, Unknown, Acquired, Series C, Series D 

-- Average Percentage of Layoffs of each Company
select company,avg(percentage_laid_off) as aver
from layoffs_staging2
group by company
order by aver DESC;

-- Distribution of layoffs for each month in a specified year
with cte_rolling_sum as
(
	select substring(`date`,1,7)as Mon, sum(total_laid_off) as summed
    from layoffs_staging2
    where `date` is not null
    group by Mon
    order by Mon
)
select *, sum(summed) over(order by Mon)
from cte_rolling_sum
where substring(Mon,1,4)='2023'
;

-- Top 5 Companies each year with highest layoffs
with cte_company_date as(
select company, substring(`date`,1,4) as dat,sum(total_laid_off) as summed
from layoffs_staging2
group by company,dat
order by 3 desc
),
cte_company_rank as(
select *, dense_rank() over(partition by dat order by summed desc) as rankings
from cte_company_date
where dat is not null)
select *
from cte_company_rank
where rankings<=5;