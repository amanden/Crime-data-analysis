"""
Filename: association_rules.py
This file reads the data from the cleaned tables and applies association mining
rules.

CSCI 720 : Big Data Analytics - Phase III
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""

# Importing the required libraries
from sqlalchemy import create_engine
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
pd.set_option('display.max_columns', None)
desired_width = 3000
pd.set_option('display.width', desired_width)


def one_hot_encode(val):
    """
    One hot encoding for the crime category

    :param val:
    :return: 0/1
    """
    if val >= 1:
        return 1
    return 0


def association_rule(data_frame, city_name):
    """
    Apply apriori algorithm for association rule mining

    :param data_frame:
    :param city_name:
    :return:
    """
    basket_data = (data_frame[data_frame['city_name'] == city_name]
              .groupby(['crime_date', 'crime_description'])['quantity']
              .sum().unstack().reset_index().fillna(0)
              .set_index('crime_date'))
    basket_encoded = basket_data.applymap(one_hot_encode)
    freq_itemset = apriori(basket_encoded, min_support=0.05, use_colnames=True)
    rules = association_rules(freq_itemset, metric="lift", min_threshold=1)
    rules = rules.sort_values(['confidence', 'lift'], ascending=[False, False])
    rules = rules[(rules['confidence'] == 1)]
    rules = rules.sort_values(['support'], ascending=[False])
    print(rules[:5])


def main():
    """
    The main method to run the cleaning task.

    :return: None
    """
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/bda_project').connect()
    age_ranges = ['<18', '18-24', '25-44', '45-64', '65+']
    city_names = ['nyc', 'la']
    for city in city_names:
        for age in age_ranges:
            data_frame = pd.read_sql_query("SELECT crime_date, crime_description,city_name, 1 as quantity FROM "
                                           "cleaned_crime_data where city_name = '" + str(city) +
                                           "' and victim_age = '" + str(age) + "' limit 20000; ", engine)
            print("\nINFO: Dataset loaded for ", city, " - ", age)
            association_rule(data_frame, city)
            print("\n --------------------------------------- \n")


if __name__ == '__main__':
    main()
