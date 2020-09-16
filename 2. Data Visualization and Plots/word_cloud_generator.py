"""
Filename: word_cloud_generator.py
This file reads the data from the merged table, and push cleaned
data into a new table.

CSCI 720 : Big Data Analytics - Phase II
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""

# Importing the required libraries
from sqlalchemy import create_engine
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import pandas as pd


def data_loading(DATABSE_URI):
    """
    The method loads the data from PostgreSQL table database and return dataframe

    :param DATABSE_URI:
    :return: data, engine
    """
    engine = create_engine(DATABSE_URI).connect()
    data_frame = pd.read_sql_query('''SELECT crime_premise FROM cleaned_crime_data; ''', engine)
    return data_frame


def word_cloud_generator(data_frame):
    str_premise = ''
    for val in data_frame.crime_premise:
        tokens = val.strip().split('[-.,/*()& ]+')
        for i in range(len(tokens)):
            tokens[i] = tokens[i]
        str_premise += " ".join(tokens) + " "
    word_cloud = WordCloud(width=800, height=800, max_words=500, background_color='white', stopwords=STOPWORDS,
                           min_font_size=10).generate(str_premise)
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(word_cloud)
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig("crime_premise.png", dpi=800)
    plt.show()


def main():
    """
    The main method to run the cleaning task.

    :return: None
    """
    DATABSE_URI = 'postgresql+psycopg2://postgres:12345@localhost/bda_project'
    data_frame = data_loading(DATABSE_URI)
    word_cloud_generator(data_frame)


if __name__ == '__main__':
    main()
