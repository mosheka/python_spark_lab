#Open lab for Python and Spark 

Your task this time is to create a users retention solution based on Spark. 
We will first collect the data, and then we will analyze the data and find patterns that will help us detect users that should have bonuses.

##Spark Batch Analysis: Creating and analysing an offline datastore
Your target is to read a ORC formated data store and detect users that had significant number of loses in their last games (lets say, 70% loses in last 10 games).

1. Create connection to ORC based on: https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.2/bk_spark-guide/content/ch_orc-spark.html
1. Create a new table that could store: user id, action date, game type, result (amount of win/loss)
1. Insert random values for 1000 users and for each 100 games w/ results between -10 and 10 using http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html
1. Find the users that in their last 10 games lost 70% of their games using RDD
1. Do the same using DataFrames
1. Compare the performance of both cases

##Spark Streaming Analysis: Detection on the fly
Your target is to monitor a Kafka stream and detect users that had significant number of loses in their last games (lets say, 70% loses in last 10 games).
Use for the following as a guidence:  https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/stateful_network_wordcount.py

1. Create a simulator, using Kafka producer, that generates streams of actions (user id, date, game, result) that will be sent to Kafka w/ results between -10 and 10 (user id, game type, result). Take a look here: https://github.com/dpkp/kafka-python
1. Create a spark micro job based on a Kafka consumer saves the streams to ORC file:
 1. Use the following example for Kafka consumer: http://rustyrazorblade.com/2015/05/spark-streaming-with-python-and-kafka/
1. Create a Spark streaming micro job that runs in a 5 min sliding window and detects users that had significant loses during that time (use reduceByKeyAndWindow as described here: http://spark.apache.org/docs/latest/streaming-programming-guide.html)
1. Create a Spark streaming micro job that detects users in this situation even if that happened before the sliding window timeframe using updateStateByKey (see more https://github.com/apache/spark/blob/v2.1.0/examples/src/main/python/streaming/stateful_network_wordcount.py)

##The Retention Game

1. Assuming your participant has the following behavior:
 1. They have 1000 units, and they cannot play if they reach 0 (no credit)
 1. They are willing to play on 10 units in every game in a zero/double game
 1. The player win rate in each game is 48%
 1. If they win they have 80% to continue
 1. If they have two wins (or more in a row), they have 90% to continue
 1. If they lose, they have 60% to continue
 1. If they lose twice (or more in a row) they have 40% to continue
 1. If they get a bonus of $10, they consider it as a win
1. Create an algorithm that will best alocate bonuses to maximize revenue
1. Impelement it using Spark streaming and provide revenue based on 1000 players and maximum of 1000 games per user. The winner will be the one with best revenue. 