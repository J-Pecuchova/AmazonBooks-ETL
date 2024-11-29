-- Vytvorenie databázy
CREATE DATABASE AmazonBooks_DB;

-- Vytvorenie schémy pre staging tabuľky
CREATE SCHEMA AmazonBooks_DB.staging;

USE SCHEMA AmazonBooks_DB.staging;

-- Vytvorenie tabuľky occupations (staging)
CREATE TABLE occupations_staging (
    occupationId INT PRIMARY KEY,
    name VARCHAR(25)
);

-- Vytvorenie tabuľky education_levels (staging)
CREATE TABLE education_levels_staging (
    educationId INT PRIMARY KEY,
    name VARCHAR(45)
);

-- Vytvorenie tabuľky users (staging)
CREATE TABLE users_staging (
    userId INT PRIMARY KEY,
    fName VARCHAR(45),
    lName VARCHAR(45),
    age INT,
    gender VARCHAR(10),
    occupationId INT,
    educationId INT,
    FOREIGN KEY (occupationId) REFERENCES occupations_staging(occupationId),
    FOREIGN KEY (educationId) REFERENCES education_levels_staging(educationId)
);
-- Vytvorenie tabuľky books (staging)
CREATE TABLE books_staging (
    ISBN VARCHAR(30) PRIMARY KEY,
    title VARCHAR(255),
    author VARCHAR(255),
    release_year VARCHAR(10),
    publisher VARCHAR(255)
);

-- Vytvorenie tabuľky ratings (staging)
CREATE TABLE ratings_staging (
    ratingId INT PRIMARY KEY,
    userId INT,
    ISBN VARCHAR(30),
    rating DOUBLE,
    timestamp TIMESTAMP_NTZ,
    FOREIGN KEY (userId) REFERENCES users_staging(userId),
    FOREIGN KEY (ISBN) REFERENCES books_staging(ISBN)
);

-- Vytvorenie my_stage pre .csv súbory
CREATE OR REPLACE STAGE my_stage;


COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO education_levels_staging
FROM @my_stage/education_levels.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @my_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO books_staging
FROM @my_stage/books.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

--- ELT - (T)ransform
-- dim_users
CREATE TABLE dim_users AS
SELECT DISTINCT
    u.userId AS dim_userId,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
    u.gender,
    o.name AS occupation,
    e.name AS education_level
FROM users_staging u
JOIN occupations_staging o ON u.occupationId = o.occupationId
JOIN education_levels_staging e ON u.educationId = e.educationId;

-- dim_date
CREATE TABLE DIM_DATE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(timestamp AS DATE)) AS dim_dateID, 
    CAST(timestamp AS DATE) AS date,                    
    DATE_PART(day, timestamp) AS day,                   
    DATE_PART(dow, timestamp) + 1 AS dayOfWeek,        
    CASE DATE_PART(dow, timestamp) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART(month, timestamp) AS month,              
    CASE DATE_PART(month, timestamp)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS monthAsString,
    DATE_PART(year, timestamp) AS year,                
    DATE_PART(week, timestamp) AS week,               
    DATE_PART(quarter, timestamp) AS quarter           
FROM RATINGS_STAGING
GROUP BY CAST(timestamp AS DATE), 
         DATE_PART(day, timestamp), 
         DATE_PART(dow, timestamp), 
         DATE_PART(month, timestamp), 
         DATE_PART(year, timestamp), 
         DATE_PART(week, timestamp), 
         DATE_PART(quarter, timestamp);


-- dim_time
CREATE TABLE DIM_TIME AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', timestamp)) AS dim_timeID, 
    timestamp,                
    CASE
        WHEN TO_NUMBER(TO_CHAR(timestamp, 'HH24')) = 0 THEN 12
        WHEN TO_NUMBER(TO_CHAR(timestamp, 'HH24')) <= 12 THEN TO_NUMBER(TO_CHAR(timestamp, 'HH24'))
        ELSE TO_NUMBER(TO_CHAR(timestamp, 'HH24')) - 12
    END AS hour,                                                              
    CASE
        WHEN TO_NUMBER(TO_CHAR(timestamp, 'HH24')) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm                                                               
FROM RATINGS_STAGING
GROUP BY timestamp;


-- dim_books
CREATE TABLE DIM_BOOKS AS
SELECT DISTINCT
    ISBN AS dim_bookId,      
    TITLE AS title,          
    AUTHOR AS author,     
    RELEASE_YEAR AS release_year, 
    PUBLISHER AS publisher   
FROM BOOKS_STAGING;


-- fact_ratings
CREATE TABLE FACT_RATINGS AS
SELECT 
    r.ratingId AS fact_ratingID,        -- Unikátne ID hodnotenia
    r.timestamp AS timestamp,   
    r.rating,                           -- Hodnota hodnotenia
    d.dim_dateID AS dateID,             -- Prepojenie s dimenziou dátumov
    t.dim_timeID AS timeID,             -- Prepojenie s dimenziou časov
    b.dim_bookId AS bookID,             -- Prepojenie s dimenziou kníh
    u.dim_userId AS userID              -- Prepojenie s dimenziou používateľov
FROM RATINGS_STAGING r
JOIN DIM_DATE d ON CAST(r.timestamp AS DATE) = d.date -- Prepojenie na základe dátumu
JOIN DIM_TIME t ON r.timestamp = t.timestamp    -- Prepojenie na základe času
JOIN DIM_BOOKS b ON r.ISBN = b.dim_bookId             -- Prepojenie na základe ISBN
JOIN DIM_USERS u ON r.userId = u.dim_userId;          -- Prepojenie na základe používateľa

-- DROP stagging tables
DROP TABLE IF EXISTS books_staging;
DROP TABLE IF EXISTS education_levels_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;


