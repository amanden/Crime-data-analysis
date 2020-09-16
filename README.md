# Big Data Analytics: Analysis of crime data in New York City and Los Angeles

Team Data Wizards, Rochester Institute of Technology
  - Aswathi Manden
  - Diptanu Sarkar
  - Sharath Nagulapally
  
# Steps to set-up and run the project

  - Download the New York City crime dataset from: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
  - Download the Los Angeles crime dataset from: https://data.lacity.org/A-Safe-City/Crime-Data-from-2010-to-2019/63jg-8b9z/data
  - Download and set-up ```PostgreSQL Database```: https://www.postgresql.org
  
Python and Packages:
  - ```Python v3.7```
  - Install packages ```psycopg2```, ```csv```, ```sqlalchemy```, ```pandas```,  ```matplotlib```, ```sklearn```, ```numpy```, ```mlxtend```, ```wordcloud```.
  
  
 1. Move the downloaded datasets to the folder with project files. Then change the database connection details and filenames in the ```main()``` of ```import_lac_data.py``` and ```import_nyc_data.py```. Run the scripts to load the data. 

2. Change the database credentials in the ```main()``` of ```data_cleaning.py``` and run to clean the data. 
3. Change the database credentials in the ```main()``` of ```descriptive_statistics.py``` and run to prepare the data for 
visualization and pairwise comparison. 
4. Change the database credentials in the ```main()``` of ```word_cloud_generator.py``` and run to generate the word cloud on crime_premise attribute values. 
5. Change the database credentials in the ```main()``` of ```kmeans_clustering.py``` and run to performs k-means clustering for 10K datapoints.
6. Change the database credentials in the ```main()``` of ```association_rules.py``` and run to apply association mining rules.

You can also download the PostgreSQL dump of the final extracted dataset from:  https://drive.google.com/file/d/1pLWvyxb1TndTu6EUo07W9gEr2jDSPSt1/view?usp=sharing

The dump data includes two tables - 

   1. ```crime_data``` : Merged raw data from the two datasources. 
   2. ```cleaned_crime_data``` : Merged and cleaned data from the ```cleaned_crime_data``` table.
