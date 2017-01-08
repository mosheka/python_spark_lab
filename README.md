# python_spark_lab
Open lab for Python, Spark Streaming and HBase using HappyBase 

Your task this time is creating a near real time users retention solution based on Spark Streaming and HBase. Your target is to monitor a Kafka stream and detect users that had significant number of loses in their last games (lets say, 70% loses in last 10 games).

Implementation should be done using https://github.com/wbolster/happybase

Use for the following as a guidence: https://github.com/wbolster/happybase/blob/master/doc/user.rst and https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/stateful_network_wordcount.py

1. Create a simulator that generates streams of actions (user, game, result) that will be sent to Kafka w/ results between -10 and 10
1. Create a micro job that saves the streams to HBase
1. Create a Spark streaming micro job that runs in a sliding window and detects users that had significant loses during that time
1. Create a Spark streaming micro job that detects users in this situation even if that happened last week
1. Create a Spark streaming micro job that detects users in this situation using take with state
1. Create connection to HBase
1. Create a new table that could store: user id, action date, game type, result (amount of win/loss)