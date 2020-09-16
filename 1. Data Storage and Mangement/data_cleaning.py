"""
Filename: data_cleaning.py
This file reads the data from the merged table, and push cleaned
data into a new table.

CSCI 720 : Big Data Analytics - Phase III
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""

# Importing the required libraries
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from pandas import DataFrame
from nltk.stem import PorterStemmer

# Setting environment variables
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def data_loading(DATABSE_URI):
    """
    The method loads the data from PostgreSQL table database and return dataframe

    :param DATABSE_URI:
    :return: data, engine
    """
    engine = create_engine(DATABSE_URI).connect()
    rows = engine.execute("SELECT * FROM crime_data; ")
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()
    return data, engine


def data_cleaning(data):
    """
    This method cleans the dataset.

    :param data: raw dataset
    :return: cleaned dataset
    """
    data.dropna(inplace=True)

    """
    Cleaning for victim_descent column.
    """
    templist = ['WHITE HISPANIC', 'ASIAN / PACIFIC ISLANDER', 'BLACK HISPANIC', 'AMERICAN INDIAN/ALASKAN NATIVE',
                'UNKNOWN', 'Z', 'A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'W', 'Z', '', 'X', 'V', 'U', 'S',
                'P', 'O', '-']
    templist2 = ['HISPANIC', 'ASIAN', 'HISPANIC', 'WHITE', np.nan, np.nan, 'ASIAN', 'BLACK', np.nan, np.nan, np.nan,
                 np.nan, 'HISPANIC', np.nan, np.nan, np.nan, np.nan, 'WHITE', np.nan, np.nan, np.nan, np.nan, np.nan,
                 np.nan, np.nan, np.nan, np.nan]
    for i in range(len(templist)):
        data['victim_descent'] = data['victim_descent'].replace(templist[i], templist2[i])

    """
    Cleaning for victim_gender column.
    """
    templist = ['', 'H', 'N', 'X', 'D', 'E']
    for i in range(len(templist)):
        data['victim_gender'] = data['victim_gender'].replace(templist[i], np.nan)

    """
    Cleaning for victim_age column.
    """
    data.dropna(inplace=True)
    nyc = data[data.city_name == 'nyc']
    la = data[data.city_name == 'la']
    check = ['<18', '18-24', '25-44', '45-64', '65+']
    nyc_age_filter = nyc[nyc.victim_age.isin(check)]
    for i in range(-9, 100):
        if i < 18:
            la['victim_age'] = la['victim_age'].replace(str(i), '<18')
        elif 18 <= i <= 24:
            la['victim_age'] = la['victim_age'].replace(str(i), '18-24')
        elif 25 <= i <= 44:
            la['victim_age'] = la['victim_age'].replace(str(i), '25-44')
        elif 45 <= i <= 64:
            la['victim_age'] = la['victim_age'].replace(str(i), '45-64')
        else:
            la['victim_age'] = la['victim_age'].replace(str(i), '65+')

    """
    Merging cleaned nyc and lac datasets.
    """
    final_dataset = pd.concat([nyc_age_filter, la])
    final_dataset["crime_description"] = final_dataset["crime_description"].str.lower()
    final_dataset["crime_premise"] = final_dataset["crime_premise"].str.lower()
    final_dataset["city_name"] = final_dataset["city_name"].str.lower()
    final_dataset["area_name"] = final_dataset["area_name"].str.lower()
    final_dataset["victim_gender"] = final_dataset["victim_gender"].str.lower()
    final_dataset["victim_descent"] = final_dataset["victim_descent"].str.lower()
    final_dataset["latitude"] = final_dataset["latitude"].round(decimals=4)
    final_dataset["longitude"] = final_dataset["longitude"].round(decimals=4)
    final_dataset["crime_description"] = final_dataset["crime_description"].apply(stem_sentences)
    return final_dataset


def stem_sentences(sentence):
    tokens = sentence.split()
    porter_stemmer = PorterStemmer()
    stemmed_tokens = [porter_stemmer.stem(token) for token in tokens]
    return stemmed_tokens[0].replace(',', '') if len(stemmed_tokens) > 0 else ""


def data_insertion(dataset):
    """
    This method inserts the cleaned data into the table.

    :param dataset: cleaned dataset
    :return: None
    """
    try:
        # Set up db connection
        connection = psycopg2.connect(database='bda_project', user='postgres', password='12345',
                                      host="127.0.0.1", port="5432")
        connection.autocommit = True
        cursor = connection.cursor()
        print("Database connection established successfully.")
        for i in dataset.index:
            # Load the data
            cursor.execute("INSERT INTO cleaned_crime_data (CRIME_DATE, CRIME_TIME, CRIME_CODE, CRIME_DESCRIPTION, "
                           "CRIME_PREMISE, CITY_NAME, AREA_NAME, LATITUDE, LONGITUDE, VICTIM_AGE, VICTIM_GENDER, "
                           "VICTIM_DESCENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (str(dataset["crime_date"][i]), str(dataset["crime_time"][i]), str(dataset["crime_code"][i]),
                            str(dataset["crime_description"][i]), str(dataset["crime_premise"][i]),
                            str(dataset["city_name"][i]), str(dataset["area_name"][i]), str(dataset["latitude"][i]),
                            str(dataset["longitude"][i]), str(dataset["victim_age"][i]),
                            str(dataset["victim_gender"][i]), str(dataset["victim_descent"][i])))
        print("All Data inserted successfully!")
    except Exception as e:
        print("DATA INSERTION FAILED: Rollback database to previous state. Message: " + str(e))
    finally:
        if connection:
            connection.close()
            print("Database connection is closed.")


def main():
    """
    The main method to run the cleaning task.

    :return: None
    """
    DATABSE_URI = 'postgresql+psycopg2://postgres:12345@localhost/bda_project'
    data, engine = data_loading(DATABSE_URI)
    final_dataset = data_cleaning(data)
    print("Data cleaning successful! ")
    data_insertion(final_dataset)


if __name__ == '__main__':
    main()
