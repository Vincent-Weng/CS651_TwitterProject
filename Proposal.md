### "Realtime" Twitter trends based on a stream of tweets (Spark Streaming+Flume+Kafka)

*By Han Weng (h5weng, 20737611) and Hao Dong\* (h45dong, 20757585)*

\*From CS631

#### Introduction

In this project, we anticipate to build a simple “twitter” engine to support extracting trends based on some realtime tweets data. The trends we get can be used in two situations:

- Display the real-time trends in the past minute. This will be updated on an every-minute basis, and is an indicator of the popular topics discussed in the past minutes' tweets. It does not require user input. However, we may aggregate the trends based on location and age so we are able to target a specific group of users.
- Auto search completion. In this application we accept user to input one or two words (or even half word) and provide search advice (like google and real twitter does).

#### Data source

We have found a data set of 1.5 million **real tweets** from Apr 6 to Jun 16 in 2009 (including `tweet_id, timestamp, username, text` and some other fields we will not use). To make the project more interesting, we are also going to add some more **simulated columns**: `city, age` (generate randomly). In order to make this more like a “real-time streaming” job, we will change the time stamps of the data and squash them into one hour. That's to say we will have 25k tweets every minute or around 400 tweets every second. So far we do not have an idea of how fast our engine will be able to run so in case of data jam, we will try our best to cache the data using some middleware (for example Kafka), or we will have to decrease the number of tweets we fit in every minute. 

#### Implementation

We plan to focus more on the learning and build of data streaming architecture design, so less effort will be devoted to the algorithm part. First, we will generate event using the dataset we mentioned above and write it into Flume, which collects the events and data, and passes that into Kafka. Finally, Spark Streaming consumes the data from kafka and performs the precessing according to the user input (most likely from command line). In the trend extraction job, we may use some NLP tools to extract the popular topics, and also re-use the algorithm from A1 and A2 (wordcount, bigram and PMI), which will be definitely helpful in search completion and suggestion.

#### Tools and platforms

- Apache Spark (particularly Spark Streaming)
- Apache Flume
- Apache Kafka