# Overview and mark guideline

This project is about a deep dive analysis into the public  [yelp review dataset](https://www.kaggle.com/yelp-dataset/yelp-dataset), to to help businesses enhance future performance.  

In general, we adpoted several pyspark scripts, to clean, gather, and process the dataset, and utilized various AWS services such as Redshift, S3 bucket to pick up designed results from queries. And one of the common BI software, SuperSet, was used to visualize the results. 

As for the algorithm of the project, we applied several algorithms such as LFM, LSTM, etc, to dive the relation between actors involved. 

All the projects are cloud-based, which means all of our project files were run through the cluster. EC2 instance and Colab were used to test our script validation, which is extremely helpful when we dealt with dataset with size of 11G, over 9 million rows but limited local laptop resource.

Finally, [one website](https://yelp-big-data-1n2acbksm-yhs2.vercel.app/landing.html) powerd by Bootstrap is also deployed public, to integrate the vision results derived from proposed scripts.



# How to run

All general packages related to big data such as the pyspark, pandas, numpy should be pre-installed. More detail could refer to [this](RUNNING.txt)



# The structure of the project 

The structure of the projects would be listed as:

+ :file_folder: ETL: Store various scipts, which clean, filter and SQL queries to load the file into Redshift and superset

+ :file_folder: analyze_data: Stores Pyspark scripts, which transpose the dataset into desired form.
+ :file_folder: prediction: We mainly accomplish 3 tasks to solve the initial problem we proposed: Review prediction, business setting and software recommendation system, which are written in review classification.py, folder review classificaiton, folder assessment
+ :file_folder: visualization: the visualization results are stored in this folder
+ :file_folder: webpage: the webpage code
+ :page_with_curl: running.txt: Running command line



