"""
Filename: import_nyc_data.py
This file imports the NYPD data to PostgreSQL DB

CSCI 720 : Big Data Analytics - Phase III
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""

# Importing the required libraries
import csv
import psycopg2
import sys
from datetime import datetime

# To increase system field size to maximum
csv.field_size_limit(sys.maxsize)


def load_nypd_data_from_file(file_name, connection, crime_date_limit_start, crime_date_limit_end):
    """
    This method loads NYPD data from CSV to PostgreSQL DB

    :param file_name:
    :param connection:
    :param crime_date_limit_start:
    :param crime_date_limit_end:

    :return: none
    """
    cursor = connection.cursor()
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        # Skip column headers
        next(reader)
        for row in reader:
            try:
                crime_date = str(row[1]).strip()
                if crime_date is not '' and datetime.strptime(crime_date_limit_start, '%m/%d/%Y') <= \
                        datetime.strptime(crime_date, '%m/%d/%Y') < datetime.strptime(crime_date_limit_end, '%m/%d/%Y'):
                    # Index according to the NYPD data
                    crime_time = row[2] if row[2] != '' else '00:00:00'
                    crime_code = row[7]
                    crime_desc = row[8]
                    crime_premise = row[15]
                    city_name = 'nyc'
                    area_name = row[13]
                    latitude = row[27] if row[27] != '' else 0
                    longitude = row[28] if row[28] != '' else 0
                    victim_age = row[32]
                    victim_gender = row[34]
                    victim_descent = row[33]
                    # Insert the data
                    cursor.execute("INSERT INTO crime_data 	(CRIME_DATE, CRIME_TIME, CRIME_CODE, CRIME_DESCRIPTION, "
                                   "CRIME_PREMISE, CITY_NAME, AREA_NAME, LATITUDE, LONGITUDE, VICTIM_AGE, "
                                   "VICTIM_GENDER, VICTIM_DESCENT) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                   (crime_date, crime_time, crime_code, crime_desc, crime_premise, city_name, area_name,
                                    latitude, longitude, victim_age, victim_gender, victim_descent))
            except Exception as e:
                print("Exception or error occurred in data formatting. Message: " + str(e))


def load_data(dataset_file_name, database_name, user, password, crime_date_limit_start, crime_date_limit_end):
    """
    Load the data to PostgreSQL Db

    :param dataset_file_name:
    :param database_name:
    :param user:
    :param password:
    :param crime_date_limit_start:
    :param crime_date_limit_end:

    :return: none
    """
    try:
        # Set up db connection
        connection = psycopg2.connect(database=database_name, user=user, password=password,
                                      host="127.0.0.1", port="5432")
        connection.autocommit = True
        print("Database connection established successfully")
        # Load the data
        load_nypd_data_from_file(dataset_file_name, connection, crime_date_limit_start, crime_date_limit_end)
        print("All Data inserted successfully from : " + str(dataset_file_name))
    except Exception as e:
        print("DATA INSERTION FAILED: Rollback database to previous state. Message: " + str(e))
    finally:
        if connection:
            connection.close()
            print("Database connection is closed")


# Change your details here

if __name__ == "__main__":
    dataset_file_name = 'NYPD_Complaint_Data.csv'
    database_name = 'bda_project'
    user = 'postgres'
    password = '12345'
    crime_date_limit_start = '01/01/2019'
    crime_date_limit_end = '01/01/2020'
    load_data(dataset_file_name, database_name, user, password, crime_date_limit_start, crime_date_limit_end)
