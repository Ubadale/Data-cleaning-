-- remove duplicate
-- standardidze the data
-- null values or blank values
-- remove columns or rows,




select*
from layoffs;

create table new_layoffs
like layoffs;

select*
from new_layoffs;

insert new_layoffs
select*
from layoffs;
select*,
row_number() over
(partition by location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_rum
from new_layoffs;

with duplicate_cte AS
(select*,
row_number() over
(partition by location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from new_layoffs)

select*
from duplicate_cte
where row_num >1;

CREATE TABLE `new_layoffs2` (
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


select* 
from new_layoffs2;
insert into new_layoffs2
select*,
row_number() over
(partition by location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from new_layoffs;

select*
from new_layoffs2
where row_num >1;

delete
from new_layoffs2
where row_num >1;

-- standardizing

select distinct company, trim(company)
from new_layoffs2
order by 1;
update new_layoffs2
set company= trim(company);

select *
from new_layoffs2
where industry like 'crypto%';
update new_layoffs2
set industry= 'crypto'
where industry like 'crypto%';

select distinct country, trim(trailing '.' from country)
from new_layoffs2
order by 1;
update new_layoffs2
set country=trim(trailing '.' from country)
where country like 'united state%'
;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from new_layoffs2;
update new_layoffs2
set `date`=str_to_date(`date`, '%m/%d/%Y');

alter table new_layoffs2
modify column `date` date;
select*
from new_layoffs2
where total_laid_off is null
and percentage_laid_off is null;

update new_layoffs2
set industry = null
where industry= '';


select* 
from new_layoffs2
where industry is null
or industry = '';
select*
from new_layoffs2
where company='airbnb';

select t1.industry, t2.industry
from new_layoffs2 t1
	join new_layoffs2 t2
    on t1.company= t2.company
where t1.industry is null
and t2.industry is not null;

update new_layoffs2 t1
join new_layoffs2 t2
    on t1.company= t2.company
    set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

select*
from new_layoffs2
where total_laid_off is null
and percentage_laid_off is null;

delete
from new_layoffs2
where total_laid_off is null
and percentage_laid_off is null;
select*
from new_layoffs2;

alter table new_layoffs2
drop column row_num;
select*
from new_layoffs2;



























































