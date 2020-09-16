"""
Filename: kmeans_clustering.py
This file reads the data from the cleaned tables and k-means clustering.

CSCI 720 : Big Data Analytics - Phase III
Project: Analysis of crime data in New York City and Los Angeles

Authors: Team Data Wizards, Rochester Institute of Technology
        - Aswathi Manden, ak3793@rit.edu
        - Diptanu Sarkar, ds9297@rit.edu
        - Sharath Nagulapally, sn8145@rit.edu
"""

# Importing the libraries
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sklearn import metrics
from sklearn.cluster import KMeans
import numpy as np


def dataLoading():
    """
    Loads the data for clustering.
    """
    connection = psycopg2.connect(dbname='temporaryDatabase', user='postgres', host='localhost', password='1234')
    connection.autocommit = True
    cursor = connection.cursor()
    print("Database connection established successfully.")
    cursor.execute("SELECT latitude, longitude from cleaned_crime_data WHERE city_name = 'nyc';")
    print("Performing kmeans clustering for nyc city")
    kmeansclustering(pd.DataFrame(list(cursor.fetchall())), "NYC")
    cursor.execute("SELECT latitude, longitude from cleaned_crime_data WHERE city_name = 'la';")
    print("Performing kmeans clustering for la city")
    kmeansclustering(pd.DataFrame(list(cursor.fetchall())), "LA")


def kmeansclustering(data, name):
    """
    This method performs k-means clustering for 10K datapoints.

    :param data:
    :param name:
    """
    data = data[:10000]
    distortions = []
    silhouette = {}
    dataa = np.array(data)
    for i in range(2, 22, 2):
        km = KMeans(
            n_clusters=i, init='random',
            n_init=10, max_iter=300,
            tol=1e-04, random_state=0
        ).fit(data)
        labels = km.labels_
        silhouette[i] = metrics.silhouette_score(data, labels, metric='euclidean')
        distortions.append(km.inertia_)
        plt.scatter(dataa[:, 0], dataa[:, 1], c=labels, cmap='rainbow')
        plt.scatter(km.cluster_centers_[:, 0], km.cluster_centers_[:, 1], color='black')
        plt.savefig('cluster.png')
        plt.show()
        plt.clf()
    plt.plot(range(2, 22, 2), distortions, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('sum of squared error')
    plt.title(name+" Elbow Point Graph")
    plt.savefig('elbowpoint'+name+".png")
    plt.clf()


if __name__ == '__main__':
    dataLoading()
