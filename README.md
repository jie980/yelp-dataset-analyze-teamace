# Overview and mark guideline

This project is about a deep dive analysis into the public  [yelp review dataset](https://www.kaggle.com/yelp-dataset/yelp-dataset), to to help businesses enhance future performance.  

In general, we adpoted several pyspark scripts, to clean, gather, and process the dataset, and utilized various AWS services such as Redshift, S3 bucket to pick up designed results from queries. And one of the common BI software, SuperSet, is used to visualize the results. 

As for the algorithm of the project, we applied several algorithms such as LFM, LSTM, etc, to dive the relation between actors involved. 

All the projects are cloud-based, which means all of our project work are run through the cluster. EC2 instance and Colab are used to test our script validation, which is extremely helpful when we deal with dataset with size of 11G, 9 million rows but limited local laptop resource.

Finally, [one website](https://yelp-big-data-1n2acbksm-yhs2.vercel.app/landing.html) powerd by Bootstrap is also deployed public, to integrate the vision results derived from proposed scripts.



# How to run

All general packages related to big data such as the pyspark, pandas, numpy should be pre-installed. More detail could refer to ...



# The structure of the project 



