# Apache Storm Tweet Processor
Multiple Apache Storm applications to process tweets

## Twitter API

TwitterSampleSpout instantiation requires several authentication parameters which you should get from your Twitter account. You need to provide consumerKey, consumerSecret, accessToken, accessTokenSecret. For more information you can check [here](https://www.youtube.com/watch?v=svoUK2DmGmw).
You can start by filling your credentials in PrintSampleStream.java topology. It creates a spout to emit a stream of tweets and a bolt to print every streamed tweet.

## Collect tweets and write into HDFS
A part of this application creates a new topology which collects around 500000 english tweets that contain certain keywords provided in the application.

The topology is as follows : 
1. TwitterSampleSpout (Connects to twitter API and emits tweets that match keywords)
2. TweetCountBolt (Keeps a count of tweets emitted so far by the TwitterSampleSpout. Kills the topology after processing 500000 tweets).
3. HdfsBolt (Writes tuples to HDFS)
4. FileWriterBolt (Writes tuples to local FS in local mode)

Output File Path in HDFS in cluster mode: /user/ubuntu/tweets
Output File Path in local FS in local mode: /home/ubuntu/tweets

## Filtering tweets with certain hashtags and keywords

In this application a new topology is created which provides the following functionality.
Every 30 seconds it collects all the tweets which are in english and have certain hashtags (provided in the application) and have the friendsCount field which satisfies friendsCount<N (N is sampled randomly from a list provided in the application) and emits the top 50% most common words which are non stop words.

The topology is as follows : 

1. TwitterSampleSpout (Connects to twitter API and emits tweets that match keywords)
2. HashtagSpout (Emits randomly sampled list of 20 hashtags every 30 seconds)
3. FriendsCountSpout (Emits a random friends count N every 30 seconds)
4. FriendsCountFilterBolt (filters tweets which do not satisfy the criteria friendsCount<N)
5. HashtagBolt (filters tweets which do not have any hashtags emitted by HashtagSpout)
6. StopWordBolt (splits tweets into words and removes stop words. Emits non-stop words from the tweets)
7. WordCountBolt (aggregates the counts of every word and emits <word,count,timestamp> tuple)
8. PopularWordBolt (sorts the counts and emits the top 50% popular words in a time interval)
9. HdfsBolt (Writes tuples to HDFS)
10. PopularWordsPrinterBolt (used in local mode to output popular words)
11. TweetPrinterBolt (used in local mode to output tweets)

Output File Path in HDFS in cluster mode: /user/ubuntu/tweet_hashtags and /user/ubuntu/popularWords
Output File Path in local FS in local mode: /home/ubuntu/popularWords.txt and /home/ubuntu/tweets_hashtag.txt

## Usage
Modify the shell scipts provided to suit your environment.

Command being executed in the shell scripts : 

`~/software/apache-storm-1.0.2/bin/storm jar storm-starter-1.0.2.jar storm.starter.CS838Assignment2PartC2 <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <output_dir> <isClusterMode> <path_to_stop_words>`
