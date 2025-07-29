-- FOUR STEPS IN DATA CLEANING
-- 1 REMOVE DUPLICATE
-- 2 STANDARDIZE THE DATA
-- 3 REMOVE NULL OR BLANK SPACES
-- REMOVE COLUMNS AND ROWS

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry)
FROM layoffs_staging;

WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
WHERE row_num > 1; 

SELECT *
FROM
	(
    SELECT *,
    ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
    ) AS duplicate_rows
WHERE row_num > 1;


CREATE TABLE `layoffs_staging_1` (
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

SELECT *
FROM layoffs_staging_1;

INSERT INTO layoffs_staging_1
SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_1
WHERE row_num > 1;

DELETE
FROM layoffs_staging_1
WHERE row_num > 1;

-- STANDARDIZE DATE
SELECT *
FROM layoffs_staging_1;

SELECT company
FROM layoffs_staging_1;

SELECT company, TRIM(company)
FROM layoffs_staging_1;

SELECT *
FROM layoffs_staging_1;

UPDATE layoffs_staging_1
SET company = TRIM(company);

SELECT industry
FROM layoffs_staging_1
ORDER BY 1;

SELECT industry
FROM layoffs_staging_1
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging_1
SET industry = 'crypto'
WHERE industry  LIKE 'crypto%';

SELECT DISTINCT industry
FROM layoffs_staging_1
ORDER BY 1;


SELECT DISTINCT country
FROM layoffs_staging_1
ORDER BY 1;

SELECT DISTINCT country, trim(trailing '.' FROM country)
FROM layoffs_staging_1
ORDER BY 1;

UPDATE layoffs_staging_1
SET country = trim(trailing '.' FROM country)
WHERE country LIKE 'united states%';

-- lets change the date format
SELECT `date`
FROM layoffs_staging_1;

SELECT `date` , str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging_1
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- lets change the date data type to DATE
ALTER TABLE layoffs_staging_1
MODIFY COLUMN `date` DATE;


-- 3 NULL VALUES AND BLANK
SELECT DISTINCT industry
FROM layoffs_staging_1;

SELECT *
FROM layoffs_staging_1 
WHERE industry IS NULL OR industry = '';

SELECT DISTINCT company, industry
FROM layoffs_staging_1
ORDER BY 1;

SELECT *
FROM layoffs_staging_1
Where company = 'airbnb';

SELECT *
FROM layoffs_staging_1
Where company = 'carvana';

SELECT *
FROM layoffs_staging_1
WHERE company = 'juul';

UPDATE layoffs_staging_1
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging_1 as l1
JOIN layoffs_staging_1 as l2
	ON l1.company = l2.company
WHERE l1.industry IS NULL AND l2.industry IS NOT NULL;

UPDATE layoffs_staging_1 as l1
JOIN layoffs_staging_1 as l2
	ON l1.company = l2.company
SET l1.industry = l2.industry
WHERE l1.industry IS NULL AND l2.industry IS NOT NULL;


-- Deleting rows and columns
SELECT *
FROM layoffs_staging_1
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;

DELETE
FROM layoffs_staging_1
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;


-- LETS DROP THAT ROW_NUM COLUMN
SELECT *
FROM layoffs_staging_1;

ALTER TABLE layoffs_staging_1
DROP COLUMN row_num;


-- LETS EXPLORE THE DATA
SELECT country, industry
FROM layoffs_staging_1
WHERE country OR industry IS NULL;

SELECT industry, sum(total_laid_off)
FROM layoffs_staging_1
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, sum(total_laid_off)
FROM layoffs_staging_1
WHERE country IS NOT NULL
GROUP BY country
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY 2 DESC;

SELECT company, sum(total_laid_off) as sum_laid_off
FROM layoffs_staging_1
GROUP BY company
ORDER BY 2 DESC;

SELECT `date`, sum(total_laid_off) as sum_laid_off
FROM layoffs_staging_1
GROUP BY `date`
ORDER BY 1 DESC;

-- sum laid off of each day
SELECT  SUBSTRING(`date`, 1, 7), sum(total_laid_off) as sum_laid_off
FROM layoffs_staging_1
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY 1 DESC;

-- max laid off of each day
SELECT SUBSTRING(`date`, 1, 7), MAX(total_laid_off) as max_laid_off
FROM layoffs_staging_1
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY 1 DESC;

-- which company laid off the highest in a particular day
SELECT company,`date`, max(total_laid_off)
FROM layoffs_staging_1
GROUP BY company, `date`
ORDER BY 3 desc;

SELECT company, YEAR(`date`), max(total_laid_off)
FROM layoffs_staging_1
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc;

-- YEARS THAT LAID OFF HIGHEST
SELECT YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging_1
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT stage, sum(total_laid_off)
FROM layoffs_staging_1
GROUP BY stage;

SELECT SUBSTRING(`date`, 1, 7) as months, sum(total_laid_off) as sum_laid_off
FROM layoffs_staging_1
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY months
ORDER BY 1 DESC;

-- ROLLING TOTAL
WITH rolling_cte AS 
(
	SELECT SUBSTRING(`date`, 1, 7) as months, sum(total_laid_off) as sum_laid_off
	FROM layoffs_staging_1
	WHERE substring(`date`, 1, 7) IS NOT NULL
	GROUP BY months
	ORDER BY 1 DESC
)
SELECT months, sum_laid_off, sum(sum_laid_off) OVER(ORDER BY months) as rolling_total_1
FROM rolling_cte;



WITH rank_cte AS
(
SELECT DISTINCT company, total_laid_off,
RANK() OVER(PARTITION BY total_laid_off) as rank_total
FROM layoffs_staging_1
WHERE total_laid_off IS NOT NULL
)
SELECT *
FROM rank_cte
WHERE rank_total > 1;


-- LETS GIVE THEM RANK
-- COMPANIES WITH THE HIGHEST YEAR LAID OFF COMES UP HERE
WITH ranking_cte  (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), sum(total_laid_off) 
FROM layoffs_staging_1
GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) as rank_total
FROM ranking_cte
WHERE total_laid_off AND years IS NOT NULL;


-- COMPANIES WITH THE HIGHEST LAYOFF FOR EACH YEAR COMES UP HERE
WITH ranking_cte  (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), sum(total_laid_off) 
FROM layoffs_staging_1
GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) as rank_total
FROM ranking_cte
WHERE total_laid_off AND years IS NOT NULL
ORDER BY rank_total;


-- first 5 companies thta has the highest layoof in a year
WITH ranking_cte  (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), sum(total_laid_off) 
FROM layoffs_staging_1
GROUP BY company, YEAR(`date`)
), company_cte AS
(
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) as rank_total
FROM ranking_cte
WHERE years AND total_laid_off IS NOT NULL
)
SELECT *
FROM company_cte
WHERE rank_total <= 5;

WITH row_n AS
(
SELECT *,
RANK() OVER(PARTITION BY location, industry, company, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging_1
)
SELECT *
FROM row_n
WHERE row_num > 1;

-- FIRST FIVE INDUSTRY OF EACH YEAR WITH TOTAL LAID OFF
WITH arrange_cte AS
(
SELECT year(`date`) as years, industry, SUM(total_laid_off) as total
FROM layoffs_staging_1
GROUP BY industry, year(`date`)
), arrange_i_cte AS
(
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY  total DESC) as rank_i
FROM arrange_cte
WHERE years IS NOT NULL 
)
SELECT *
FROM arrange_i_cte
WHERE rank_i <=5;


-- MONTHS THEY LAID PEOPLE OFF THE MOST
WITH months_cte AS
(
SELECT year(`date`), substring(`DATE`, 6, 2) as months, SUM(total_laid_off) as sum_total
FROM layoffs_staging_1
WHERE year(`date`) IS NOT NULL
GROUP BY year(`date`), substring(`DATE`, 6, 2)
), months_cte_1 AS
(
SELECT *,
RANK()OVER(PARTITION BY months ORDER BY sum_total DESC) as rank_mo
FROM months_cte
)
SELECT *
FROM months_cte_1
WHERE rank_mo = 1
ORDER BY sum_total DESC;