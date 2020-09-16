-- Filename: CREATE_TABLE.sql
-- This scipt creates the table to insert crime data into PostgreSQL
-- Authors: Team Data Wizards, Rochester Institute of Technology
        -- Aswathi Manden, ak3793@rit.edu
        -- Diptanu Sarkar, ds9297@rit.edu
        -- Sharath Nagulapally, sn8145@rit.edu

CREATE TABLE CRIME_DATA (
	ID SERIAL PRIMARY KEY,
	CRIME_DATE DATE, -- Format: MM/DD/YYY
	CRIME_TIME TIME, -- Fromat: HH:MM (24hrs)
	CRIME_CODE INT, -- Three digit offense code- NYC: KY_CD, LA: Premis CD
  CRIME_DESCRIPTION VARCHAR(3000), -- NYC: OFNS_DESC, LA: Crm Cd Desc
	CRIME_PREMISE VARCHAR(3000), -- NYC:PREM_TYP_DESC LA: Premis Desc
  CITY_NAME VARCHAR(500), -- Changed from City code, easier to look-up (Nominal data - NYC / LAC)
  AREA_NAME VARCHAR(500), -- Changed from Area code, easier to look-up (Nominal data)
  LATITUDE DOUBLE PRECISION,
  LONGITUDE DOUBLE PRECISION,
  VICTIM_AGE VARCHAR(20), -- In NYC dataset some ages are range
  VICTIM_GENDER VARCHAR(30),
  VICTIM_DESCENT VARCHAR(200)
);


CREATE TABLE CLEANED_CRIME_DATA (
	ID SERIAL PRIMARY KEY,
	CRIME_DATE DATE, -- Format: MM/DD/YYY
	CRIME_TIME TIME, -- Fromat: HH:MM (24hrs)
	CRIME_CODE INT, -- Three digit offense code- NYC: KY_CD, LA: Premis CD
  CRIME_DESCRIPTION VARCHAR(3000), -- NYC: OFNS_DESC, LA: Crm Cd Desc
	CRIME_PREMISE VARCHAR(3000), -- NYC:PREM_TYP_DESC LA: Premis Desc
  CITY_NAME VARCHAR(500), -- Changed from City code, easier to look-up (Nominal data - NYC / LAC)
  AREA_NAME VARCHAR(500), -- Changed from Area code, easier to look-up (Nominal data)
  LATITUDE DOUBLE PRECISION,
  LONGITUDE DOUBLE PRECISION,
  VICTIM_AGE VARCHAR(20), -- In NYC dataset some ages are range
  VICTIM_GENDER VARCHAR(30),
  VICTIM_DESCENT VARCHAR(200)
);