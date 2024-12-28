SELECT * 
FROM layoffs;

-- Creating a staging table to alter it without affecting raw data.

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;


-- Steps:
-- Removing Duplicates
-- Standardising the data
-- Handling NULL and blank values
-- Removing unnecessary rows and columns


-- 1. Removing Duplicates

-- CTE to identify duplicate rows
WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, `date`, stage, country) AS row_num
	FROM layoffs_staging
	) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;


-- In mysql we cannot directly delete rows within cte (unlike SQL Server).
-- saving results to new table layoffs_staging2 so that we can remove duplicate rows from it.
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


SELECT * 
FROM layoffs_staging2
-- WHERE row_num > 1
;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, `date`, stage, country) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- 2. Standardising the data

-- Removing leading and trailing from rows
SELECT country
FROM layoffs_staging2
WHERE country LIKE ' %' OR country LIKE '% '
;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Combining similar data points

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%' ;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country
;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%' ;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%' ;

-- Converting date in text format to Date format of SQL

SELECT `date`, 
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

SELECT `date`
FROM layoffs_staging2 ;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ;

DESCRIBE layoffs_staging2 ;

-- 3. Handling NULL and blank values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
	OR industry = '';
    
SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '')
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '')
;


-- 4. Removing unnecessary rows and columns

-- removing the temporary row_num column we created
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

-- rows were both total_laid_off and percentage_laid_off are NULL are useless for analysis
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL
;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL
;


-- Final table for analysis:

SELECT *
FROM layoffs_staging2;

-- -------------------------- --
-- EXPLORITORY DATA ANALYSIS  --


-- Time duration of the dataset.
SELECT MAX(`date`), MIN(`date`)
FROM layoffs_staging2 ;


-- Top 10 countries with the highest layoffs
SELECT Country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC
LIMIT 10
;


-- Companies industry wise with the highest layoffs
SELECT company, industry, country, SUM(total_laid_off)
FROM layoffs_staging2
WHERE industry IS NOT NULL
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY industry, SUM(total_laid_off) DESC
;


-- Laidoff each year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`) DESC
;

-- Rolling total of laidoff monthwise 
WITH Rolling_total AS (
SELECT SUBSTRING(`date`, 1 , 7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1 , 7) IS NOT NULL
GROUP BY `Month`
ORDER BY `Month`
)
SELECT `Month`, total_off,
SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_total
;


-- Top 5 companies that laidoff each year
WITH company_year AS 
	(
	SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_off
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
	ORDER BY company
	),
ranking_cte AS
	(
	SELECT company, years, total_off,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_off DESC) AS ranking
	FROM company_year
    WHERE years IS NOT NULL
	)
SELECT * 
FROM ranking_cte
WHERE ranking <= 5
;