-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. De-Dupe
-- 2. Standardize
-- 3. NULL / Blank values
-- 4. Remove any unwanted columns


create table layoffs_staging 
like layoffs;

SELECT * 
FROM layoffs_staging;

Insert layoffs_staging
select * 
from layoffs;

with duplicates_cte as 
(
SELECT * ,
row_number() OVER(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,country,stage,funds_raised_millions) as row_num
FROM layoffs_staging
)
select * 
from duplicates_cte
where row_num > 1;


select * 
from layoffs_staging
where company = 'Casper';

with duplicates_cte as 
(
SELECT * ,
row_number() OVER(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,country,stage,funds_raised_millions) as row_num
FROM layoffs_staging
)
select *
from duplicates_cte
where row_num > 1;


create table layoffs_staging_dedupe
like layoffs_staging;

select*
from layoffs_staging_dedupe;

alter table layoffs_staging_dedupe
add column row_num int;

insert layoffs_staging_dedupe
select * ,
row_number() OVER(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,country,stage,funds_raised_millions) as row_num
from layoffs_staging;

drop table layoffs_staging2;

select *
from layoffs_staging_dedupe
where row_num >1;


set sql_safe_updates = 0;

delete
from layoffs_staging_dedupe
where row_num >1;

-- Standardize data
select company ,(trim(company))
from layoffs_staging_dedupe;

update layoffs_staging_dedupe
set company = trim(company);

select count(industry)
from layoffs_staging_dedupe
where industry like 'Crypto%';

update layoffs_staging_dedupe
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging_dedupe;

select distinct location
from layoffs_staging_dedupe
order by 1;

select distinct country
from layoffs_staging_dedupe
order by 1;

select distinct country, trim(country)
from layoffs_staging_dedupe
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging_dedupe
order by 1;

update layoffs_staging_dedupe
set country = trim(trailing '.' from country);

select distinct country
from layoffs_staging_dedupe
order by 1;

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging_dedupe;


update layoffs_staging_dedupe
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging_dedupe;

alter table layoffs_staging_dedupe
modify column `date` date;


select *
from layoffs_staging_dedupe;


-- working with NULL/Bank values



select *
from layoffs_staging_dedupe
where industry is null
or industry = '';

select *
from layoffs_staging_dedupe
where industry = ''
or industry is null;

/*
update layoffs_staging_dedupe
set industry = null
where industry = '';


SELECT * 
FROM layoffs_staging_dedupe t1
join layoffs_staging_dedupe t2
	on t1.company=t2.company
where t1.industry is null
and t2.industry is not null;
    
*/


select *
from layoffs_staging_dedupe
where company = 'Airbnb';

update layoffs_staging_dedupe
set industry = 'Travel'
where company = 'Airbnb';

select *
from layoffs_staging_dedupe
where company = 'Carvana';

update layoffs_staging_dedupe
set industry = 'Transportation'
where company = 'Carvana';

select *
from layoffs_staging_dedupe
where company = 'Juul';

update layoffs_staging_dedupe
set industry = 'Consumer'
where company = 'Juul';

select *
from layoffs_staging_dedupe
where total_laid_off is null
and percentage_laid_off is null;

create table layoffs_staging_with_null_laidoffcols
like layoffs_staging_dedupe;

insert into layoffs_staging_with_null_laidoffcols
select * 
from layoffs_staging_dedupe;

select * from layoffs_staging_with_null_laidoffcols;

delete
from layoffs_staging_dedupe
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging_dedupe;

alter table layoffs_staging_dedupe
drop column row_num;

select * 
from layoffs_staging_dedupe;