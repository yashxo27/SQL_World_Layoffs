-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Selecting the Schema
use world_layoffs;

-- Viewing the Database
select *
from layoffs;

-- Creating a new table for working
create table layoffs_staging
like world_layoffs.layoffs;

insert world_layoffs.layoffs_staging 
select *
from world_layoffs.layoffs;

-- Creating a CTE to check for duplicate rows with respect to all columns
with table_with_row as
(select *,
row_number() 
over(partition by company,
				  location,
                  total_laid_off,
                  percentage_laid_off,
                  `date`,
                  stage,
                  country,
                  funds_raised_millions) as row_num
from world_layoffs.layoffs_staging
)
select *
from table_with_row
where row_num>1;

-- Creating a new table where Row Number is added
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert world_layoffs.layoffs_staging2
select *,
row_number() 
over(partition by company,
				  location,
                  total_laid_off,
                  percentage_laid_off,
                  `date`,
                  stage,
                  country,
                  funds_raised_millions) as row_num
from world_layoffs.layoffs_staging;

-- Deleting Duplicate Rows
delete
from world_layoffs.layoffs_staging2
where row_num >1;

-- Standardizing Company Column
update world_layoffs.layoffs_staging2
set company=trim(company);

-- Standardizing Industry Column
select distinct industry
from world_layoffs.layoffs_staging2
order by 1;

update world_layoffs.layoffs_staging2
set industry = "Crypto"
where industry like "Crypto%";

-- Standardizing Country Column
select distinct country
from world_layoffs.layoffs_staging2
order by country;

update world_layoffs.layoffs_staging2
set country = "United States"
where country like "United States%";

-- Converting Date into appropriate Format and Type
update world_layoffs.layoffs_staging2
set `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Removing Null values in Tota and Percentage Laid Off
select *
from world_layoffs.layoffs_staging2
where total_laid_off IS NULL;

select count(row_num)
from world_layoffs.layoffs_staging2
where total_laid_off IS NULL AND percentage_laid_off IS NULL;
-- 361

delete
from world_layoffs.layoffs_staging2
where total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Removing Null and Blank Values in Industry
select *
from world_layoffs.layoffs_staging2
where company in (
	select company
    from world_layoffs.layoffs_staging2
    where industry IS NULL or industry= ''
)
order by company;

select *
from world_layoffs.layoffs_staging2
where industry IS NULL or industry= '';

select t1.industry,t2.industry
from world_layoffs.layoffs_staging2 as t1
join world_layoffs.layoffs_staging2 as t2
	on t1.company=t2.company 
    and t1.location=t2.location
		where (t1.industry IS NULL or t1.industry = '')
		and t2.industry IS NOT NULL and t2.industry not like '';

update world_layoffs.layoffs_staging2 as t1
join world_layoffs.layoffs_staging2 as t2
on t1.company=t2.company
set t1.industry=t2.industry
	where (t1.industry IS NULL or t1.industry = '')
	and t2.industry IS NOT NULL and t2.industry not like '';

-- Dropping Row Number column
ALTER TABLE world_layoffs.layoffs_staging2
drop column row_num;

select *
from world_layoffs.layoffs_staging2;