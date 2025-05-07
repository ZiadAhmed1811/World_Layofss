SELECT *
FROM layoffs;

-- 1. Remove Duplicates

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS ROW_NO
FROM layoffs_staging;

WITH Duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS ROW_NO
FROM layoffs_staging
)
SELECT * 
FROM Duplicate_cte
WHERE ROW_NO > 1 ;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


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
  `ROW_NO` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS ROW_NO
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2
WHERE ROW_NO >1 ;


DELETE 
FROM layoffs_staging2
WHERE ROW_NO >1 ;

-- Standardizing Data

SELECT DISTINCT (company)
FROM layoffs_staging2;


SELECT DISTINCT (company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country
FROM layoffs_staging2
WHERE country like 'United States%';

SELECT DISTINCT country , TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- CHANGE DATE FORMAT FROM TEXT TO TIME SERIES

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULL AND BLANK VALUES


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



-- POPULATE DATA
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '' ;

-- POPULATE DATA

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb' ;

-- FILL BLANKS

SELECT t1.industry , t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE  layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET   t1.industry = t2.industry  
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;



-- REMOVING COLUMNS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN ROW_NO;


-- FINAL RESULT

-- RAW TABLE 
SELECT *
FROM layoffs;

-- CLEANED TABLE 

SELECT *
FROM layoffs_staging2;




















