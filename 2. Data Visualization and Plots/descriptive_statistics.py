"""
Filename: descriptive_statistics.py
This file reads the data from the cleaned table and prepares the data for visualisation and pairwise comparison.

CSCI 720 : Big Data Analytics - Phase III
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""


# Importing the required libraries
import psycopg2
from pandas import DataFrame
from sqlalchemy import create_engine
import xlsxwriter
import pandas as pd
import matplotlib.pyplot as plt


def crime_description_time(engine, conn):
    rows = engine.execute("select date_part('hour', crime_time) as time, crime_description, count(*) as \"incidents\" "
                          "from public.cleaned_crime_data group by 1,2;")
    cur = conn.cursor()
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()

    crime_description = []
    incidents = []
    times = []
    df = pd.DataFrame()
    for time in range(24):
        grouped_by_time = data.groupby(['time']).get_group(time)
        max = grouped_by_time.loc[grouped_by_time['incidents'].idxmax()]
        crime_description.append(max['crime_description'])
        incidents.append(max['incidents'])
        times.append(time)

    df['times'] = pd.Series(times)
    df['crime_description'] = pd.Series(crime_description)
    df['incidents'] = pd.Series(incidents)

    df.to_excel('crime_description_time.xlsx', engine='xlsxwriter')

    plt.bar(times, incidents)
    plt.show()


def crime_premise_time(engine, conn):
    rows = engine.execute("select date_part('hour', crime_time) as time, crime_premise, count(*) as \"incidents\" "
                          "from public.cleaned_crime_data group by 1,2;")
    cur = conn.cursor()
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()

    crime_premise = []
    incidents = []
    times = []
    df = pd.DataFrame()
    for time in range(24):
        grouped_by_time = data.groupby(['time']).get_group(time)
        max = grouped_by_time.loc[grouped_by_time['incidents'].idxmax()]
        crime_premise.append(max['crime_premise'])
        incidents.append(max['incidents'])
        times.append(time)

    df['times'] = pd.Series(times)
    df['crime_premise'] = pd.Series(crime_premise)
    df['incidents'] = pd.Series(incidents)

    df.to_excel('crime_premise_time.xlsx', engine='xlsxwriter')


def crime_desc_age(engine, conn):
    rows = engine.execute("select victim_age, crime_description, count(*) as \"incidents\" from public.cleaned_crime_data"
                          " group by 1,2;")
    cur = conn.cursor()
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()

    victim_age = ["<18","18-24","25-44", "45-64","65+"]
    crime_description = []
    incidents = []
    ages = []
    df = pd.DataFrame()
    for age in victim_age:
        grouped_by_time = data.groupby(['victim_age']).get_group(age)
        max = grouped_by_time.loc[grouped_by_time['incidents'].idxmax()]
        crime_description.append(max['crime_description'])
        incidents.append(max['incidents'])
        ages.append(age)

    df['ages'] = pd.Series(ages)
    df['crime_description'] = pd.Series(crime_description)
    df['incidents'] = pd.Series(incidents)

    df.to_excel('crime_desc_age.xlsx', engine='xlsxwriter')

def pairwise_time_desc(engine, conn):
    rows = engine.execute("select date_part('hour', crime_time) as time, crime_description from public.cleaned_crime_data"
                          " where city_name ='la' limit 500;")
    cur = conn.cursor()
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()

    r = calculate_r(data, "time", "crime_description")
    print("r value for time and crime_description: ", r)

def pairwise_time_premise(engine, conn):
    rows = engine.execute("select date_part('hour', crime_time) as time, crime_premise from public.cleaned_crime_data"
                          " where city_name ='la' limit 500;")
    cur = conn.cursor()
    data = DataFrame(rows.fetchall())
    data.columns = rows.keys()

    r = calculate_r(data, "time", "crime_premise")
    print("r value for time and crime_premise: " ,r)

def calculate_r(data, col_x, col_y):
    word_to_num = dict([(y, x + 1) for x, y in enumerate(sorted(set(data[col_y].tolist())))])
    data.replace(word_to_num.keys(), word_to_num.values(), inplace=True)

    x_mean = data[col_x].mean()
    Sx = data[col_x].std()
    y_mean = data[col_y].mean()
    Sy = data[col_y].std()
    x_sum = 0
    y_sum = 0
    n = 0
    for index, row in data.iterrows():
        x_sum += row[col_x] - x_mean
        y_sum += row[col_y] - y_mean
        n += 1
    r = (x_sum / Sx) * (y_sum / Sy) / (n - 1)
    return r


def main():
    """
    The main method of execution

    """
    conn = psycopg2.connect(dbname='bda_project', user='postgres', host='localhost', password='12345')
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/bda_project').connect()

    crime_description_time(engine, conn)
    crime_premise_time(engine, conn)
    crime_desc_age(engine, conn)
    pairwise_time_desc(engine, conn)
    pairwise_time_premise(engine, conn)

if __name__ == '__main__':
    main()
