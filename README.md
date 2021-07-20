# Spark-Data-Analysis

# workload1: Data Analysis with Spark RDD API
## Introduction
There are two main aims in this workload1. 
* `Controversial Trending Videos Identification`  
Aim: Find out the top 10 videos with fastest growth of dislikes number between its first and last trending appearances. Listing a video as trending would help it attract more views. However, not all trend- ing videos are liked by viewers. For some video, listing it as trending would increase its dislikes number more than the increase of its likes number. This workload aims to iden- tify such videos. 
* `Category and Trending Correlation`  
Aim: Find out the average country number for videos in each category. Some videos are trending in multiple countries. We are interested to know if there is any correlation between video category and trending popularity among countries. For instance, we may expect to see a common set of trending music videos in many countries and a distinctive set of trending political videos in each country. 

## Dataset
The dataset is adapted from Trending Youtube Video Statistics data from Kaggle.The dataset contains several months’ records of daily top trending YouTube video in the following ten countries: Canada,France, Germany, India,Japan, Mexico, Russia, South Ko- rea, United Kingdom and United States of America. There are up to 200 trending videos listed per day.n the original data set, each country’s data is stored in a separate CSV file, with each row representing a trending video record. If a video is listed as trending in multiple days, each trending appearance has its own record. The category names are stored in a few separate JSON files. 
>Note: One video_id can have multiple trending appearances. For example, video_id `QwZT7T-TXT0` shows in both US and GB countries.

The results is a CSV file AllVideos.csv with 8 columns and no header row. The columns are as follows. The trending date column has the date format: yy.dd.mm
`video_id,trending_date,category,views,likes,dislikes,country`
<br />
<br /> 
<br />
<br />

# workload2: Spark Data Analytics with Spark Dataframe API
## Introduction
There are two main aims in this workload1.   
* `aim one`:  
Find the top 5 users with similar interest as a given user id. We measure a user’s interest based on the tweets they have replied or retweeted. We can represent each user as a tokenized document, with all tokens as ids of the tweets the user has replied or retweeted. Assume we have a sample data set with five tweets: t1,t2,t3,t4, t5 and five users: u1,u2,u3,u4, u5, table 1 shows the reply/retweet relation from user to tweet. The last column shows the corresponding document representation of each user. For instance, u1 has retweeted t3, t5 and replied t4,t5, the document repre- sentation is (t3,t4,t5,t5). To implement the workload, we need to to transform each user document represen- tation into a vector and use Cosine similarity to compute the similarity between two user vectors. SparkML provides a number of feature extractors such as TF-IDF, Word2Vec and more. we are asked to experiment with two feature extractors and compare the execution performance in local and EMR cluster (minimum 3 nodes) setting. we can use any user id in the data set as the given user id. For each feature extractor, we can set your own parameter values.
<br />
  
* `aim two`:  
We consider two groups of users. One group contains users that appear in the user id field of a tweet object, we call them tweet users; another group contains users mentioned in a tweet, we call them mention users. In this workload, you are asked to use collaborative filtering algorithm in SparkML library to recommend top 5 mention users to each tweet user in the data set. Collaborative filtering aims to fill in the missing entries of a user-item association ma- trix. We treat tweet users as ‘user’ and mention users as ‘item’ in this setting. The rating is computed as number of times a tweet user mentions a mention user. You are asked to prepare the raw data in the format as required by the collaborative filter algorithm and build a model to perform the recommendation. 
> Note: This workload should run in EMR cluster. The `submit_client.sh` is the EMR cluster setting.
<br />
  
## Dataset
The dataset contains tweet objects downloaded from Twitter using Tweeter standard search API. The downloaded tweet objects are stored in a single JSON file. A
tweet object may refer to a general tweet, a retweet or a reply to a tweet. A general tweet is “a message posted to Twitter containing text, photos, a GIF, and/or video” . A reply is a response to another person’s tweet . “A Retweet is a re-posting of a Tweet.”   
<br />

The common fields in all tweet objects are:  
* id: the unique id of the tweet.  
* created at: the date and time the tweet is created.  
* text: the textual content of the tweet. 
* user id: the id of the user who created the tweet.  
* retweet count: retweet count of the tweet or its parent tweet if the tweet is a retweet • favorite count: favorite count of the tweet.  
  
The optional fields are:  
* retweet id: included only in retweet object. It contains the id of the tweet it re-posts.  
* retweet user id: included only in retweet object. It contains the user id of the tweet it re-posts.  
* replyto id: included only in reply object. It contains the id of the tweet it responds to.  
* replyto userid: included only in reply object. It contains the user id of the tweet it responds to.  
* user mentions: included only if the tweet text mentions one or more users. It is an array of {id, indices} showing the id of the users mentioned in the tweet text, as well as the location this user is mentioned.  
* hash tags: included only if the tweet text contains one or more hash tags. It is an array of {tag, indices} showing the hash tag appearing in the tweet text and the location it appears.  
<br />

## Method
### Load data:  
```
spark.read.option(”multiline”,”true”).json(file path)
```
### Design:

* Aim 1:  
![image](https://github.com/germaine-wu/Spark-Data-Analysis/blob/main/image/The%20overall%20data%20flow%20of%20aim1.png)  
<p align="center"> Figure 1. The overall data flow of aim 1 </p>

* Aim 2:  
![image](https://github.com/germaine-wu/Spark-Data-Analysis/blob/main/image/The%20overall%20data%20flow%20of%20aim2.png)  
<p align="center"> Figure 2. The overall data flow of aim 2 </p>


## Performance analysis

### Aim 1:
From Table 1, we can see the workload runs on both local and EMR machine, and the workload is submitted in either cluster or client mode. The local machine points to running script on Jupiter Notebook container. The EMR machine is set to be a master node and two core nodes. There is total three executors and each executor has four cores. The executor memory is 9486M, and drive memory is 2048M. The performance and execution speed of EMR is much higher than that of Local, because distributed computing can improve efficiency. The execution time of Client Mode is shorter than that of Cluster Mode, indicating that Client Mode is better in this project.  

![image](https://github.com/germaine-wu/Spark-Data-Analysis/blob/main/image/The%20environment%20configurations%20of%20aim1.png)  
<p align="center"> Table 1. The environment configurations of aim 2 </p>  

![image](https://github.com/germaine-wu/Spark-Data-Analysis/blob/main/image/The%20summary%20of%20executors.png)  
<p align="center"> Figure 3. The summary of executors </p>

### Aim 2:
As same as aim one, the aim two runs on both local and EMR machine.  
![image](https://github.com/germaine-wu/Spark-Data-Analysis/blob/main/image/The%20environment%20configurations%20of%20aim2.png)  
<p align="center"> Table 2. The environment configurations of aim 2 </p>

## A brief conclusion

The EMR machine has more efficient project execution than local machine, and client mode has less time consuming compared with cluster mode. For workload one, Word2Vec has a better ability to calculate and distinguish the difference between vector features, compared to TF- IDF.
