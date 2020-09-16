-- Filename: VISUALZATION_QUERIES.sql
-- This scipt creates the table to insert crime data into PostgreSQL
-- Authors: Team Data Wizards, Rochester Institute of Technology
        -- Aswathi Manden, ak3793@rit.edu
        -- Diptanu Sarkar, ds9297@rit.edu
        -- Sharath Nagulapally, sn8145@rit.edu


-- No. of incidents vs months:

select date_part('month', crime_date) as mon, count(*) as "incidents"
from cleaned_crime_data
group by 1;

-- Victim age vs crime description:

select victim_age, crime_description, count(*) as "incidents" from cleaned_crime_data 
group by 1,2 order by victim_age, incidents desc;


-- Victim Gender vs city:

select victim_gender, city_name, count(*) as "incidents" from cleaned_crime_data
group by 2,1;

-- Gender vs age:

select victim_gender, victim_age, count(*) as "incidents" from cleaned_crime_data
group by 2,1;

-- Gender vs descent:

select victim_gender, victim_descent, count(*) as "incidents" from cleaned_crime_data
group by 2,1;

-- Area vs count:

select area_name, city_name ,count(*) as "incidents" from cleaned_crime_data group by 2,1;

-- Top 10 Crime descriptions:

select crime_description ,count(*) as "incidents" from cleaned_crime_data 
group by 1 order by incidents desc limit 10;

-- Top 10 Crime premises:

select crime_premise, count(*) as "incidents" from cleaned_crime_data 
group by 1 order by incidents desc limit 10;

-- Time vs crime_description:

select date_part('hour', crime_time) as time, crime_description, count(*) as "incidents" from cleaned_crime_data  
group by 1,2 order by time;

-- Time vs crime_premise:

select date_part('hour', crime_time) as time, crime_premise, count(*) as "incidents" from cleaned_crime_data 
group by 1,2;

