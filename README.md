# stix-generator
This app is for aggregate event from HDFS and store the result in MongoDB

## Config
set the mongodb uri in `Stix.scala` \
Set the hdfs uri in `Stix.scala`

## compile
compile to jar using sbt
```
sbt assembly
```

## Store to HDFS (optional)
Store jar to hdfs, if want to run Spark in cluster mode \
example:
```
hdfs dfs -put -f target/scala-2.11/IdsToStix-assembly-0.1.jar hdfs://localhost:9000/user/hduser/job/
```

## Run
```
spark-submit --class Stix --master spark://master:7077 --executor-memory 2G --total-executor-cores 2 hdfs://127.0.0.1:9000/user/hduser/job/IdsToStix-assembly-0.1.jar
```