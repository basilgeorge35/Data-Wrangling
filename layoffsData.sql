SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;


-- Steps:
-- Removing Duplicates


-- 1. Removing Duplicates

WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, `date`, stage, country) AS row_num
	FROM layoffs_staging
	) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;



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