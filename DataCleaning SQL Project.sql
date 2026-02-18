SELECT * 
FROM world_layoffs.layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT * from layoffs_staging;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1.Checking for duplicates

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *,
		ROW_NUMBER() OVER 
        (PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
        
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
	WITH duplicate_cte AS
    (
    SELECT *,
		ROW_NUMBER() OVER 
        (PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM 
		world_layoffs.layoffs_staging
    )
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';

-- solution is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column

CREATE TABLE `world_layoffs`.`layoffs_staging2` 
(
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` INT DEFAULT NULL,
row_num int
);

INSERT INTO layoffs_staging2

SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- disabling safe update mode as the above query couldnt run:
SET SQL_SAFE_UPDATES = 0;

DELETE FROM world_layoffs.layoffs_staging2 WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1;

-- 2.Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

