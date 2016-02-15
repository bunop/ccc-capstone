
# Cloud computing capstone - Part 1

## Requirements

* subversion
* [ant (HOWTO install)][install-ant]: get [binary][ant-binary]
* jna
* java devel (if you can'f find: [tools.jar][tools-jar])
* pigz

[install-ant]: http://xmodulo.com/how-to-install-apache-ant-on-centos.html
[ant-binary]: http://mirror.nohup.it/apache//ant/binaries/apache-ant-1.9.6-bin.tar.gz
[tools-jar]: http://stackoverflow.com/questions/5730815/unable-to-locate-tools-jar

### Install maven

As a superuser do:

```
$ cd /opt/
$ wget http://mirrors.gigenet.com/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -zxvf apache-maven-3.3.9-bin.tar.gz
$ ln -s apache-maven-3.3.9 maven
```

Edit the file `/etc/profile.d/maven.sh`:

```
export M2_HOME=/opt/maven
export M2=$M2_HOME/bin
PATH=$M2:$PATH
```

Logout, and test maven installation

```
$ mvn -version
```

[install-maven]: http://johnathanmarksmith.com/linux/centos7/java/maven/programming/project%20management/2014/10/08/how-to-install-maven-323-on-centos7/

### Registering different types of storage

Follow this [guide][install-piggybank]:

```
$ cd /home/ec2-user/capstone
$ mkdir piggy_bank
$ cd piggy_bank
$ svn checkout http://svn.apache.org/repos/asf/pig/trunk/ .
$ ant
$ cd contrib/piggybank/java
$ ant
```

Next, you have to refister piggybank.jar and refer to the appropriate module, Ex:

```
grunt> REGISTER /home/ec2-user/capstone/piggy_bank/contrib/piggybank/java;
grunt> data = LOAD 'my.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
  AS (...);
```

More info [here][reading-csv-in-pig]

[install-piggybank]: https://cwiki.apache.org/confluence/display/PIG/PiggyBank
[reading-csv-in-pig]: http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma

### Install Cassandra

Add the `datastax` repository to your repos `/etc/yum.repos.d/datastax.repo` as suggested
[here][datastax-cassandra]:

```
[datastax]
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0
```

Install Cassandra

```
$ yum install dsc20
```

Manage Cassandra services

```
$ service cassandra start
$ service cassandra status
$ service cassandra stop
```

Edit `/etc/cassandra/conf/cassandra.yaml`. listen_address and rpc_address have to been
set to hostname, or ip address, as suggested [here][configure-cassandra]

```
236c236
<           - seeds: "127.0.0.1"
---
>           - seeds: "node2,node3"
308c308
< listen_address: localhost
---
> listen_address: master
357c357,358
< rpc_address: localhost
---
> rpc_address: master
>
```

Test cassandra installation:

```
$ nodetool status
$ cqlsh ambari
```

More info in [cassandra GettingStarted][GettingStarted-cassandra]

[datastax-cassandra]: https://docs.datastax.com/en/cassandra/2.0/cassandra/install/installRHEL_t.html
[install-cassandra]: http://www.liquidweb.com/kb/how-to-install-cassandra-on-centos-6/
[configure-cassandra]: http://wiki.apache.org/cassandra/MultinodeCluster10
[GettingStarted-cassandra]: http://wiki.apache.org/cassandra/GettingStarted

### Using IPython with Spark

1. Create an iPython notebook profile for our Spark configuration:

```
$ ipython profile create spark
```

2. Create a file in `$HOME/.ipython/profile_spark/startup/00-pyspark-setup.py` and add the following:

```python
import os
import sys

# Configure the environment
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/hdp/current/spark-client'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))

# Described here: http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/
sys.path.insert(0, os.path.join(SPARK_HOME, "python/lib/py4j-0.8.2.1-src.zip"))
execfile(os.path.join(SPARK_HOME, 'python/pyspark/shell.py'))
```

3. Start up an IPython shell with the profile we just created.

```
$ ipython --profile spark
```

In your notebook, you should see the variables we just created.

```python
print SPARK_HOME
```

More info on installing Ipython-notebook with spark could be found [here][setting-ipython-notebook],
[here][notebook-cloudera] and [here][spark-python]. Tips to submit application using
yarn could be found [here][spark-submitting]

[setting-ipython-notebook]: http://nbviewer.jupyter.org/gist/fperez/6384491/00-Setup-IPython-PySpark.ipynb
[notebook-cloudera]: http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/
[spark-python]: https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python
[spark-submitting]: http://spark.apache.org/docs/latest/submitting-applications.html

### Install pyspark-cassandra

```
$ curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
$ yum install sbt ipython
$ git clone https://github.com/bigstepinc/pyspark-cassandra.git
$ cd pyspark-cassandra
$ git submodule update --init --recursive
$ make dist
$ export PYSPARK_CASSANDRA=/home/ec2-user/capstone/pyspark-cassandra/target
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
    --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
    --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
    --conf spark.cassandra.connection.host=master pyspark-shell"
$ ipython --profile spark
```

Example on pyspark_cassandra dataframe could be found [here][pyspark-dataframe-cassanra].

[official-pypsark_cassandra]: https://github.com/TargetHolding/pyspark-cassandra.git
[centos-6-pyspark_cassandra]: https://github.com/bigstepinc/pyspark-cassandra.git
[pyspark-dataframe-cassanra]: http://rustyrazorblade.com/2015/05/on-the-bleeding-edge-pyspark-dataframes-and-cassandra/

## Processing and filtering Aviation dataset

### Mount DATA volume

Discover volumes:

```
$ sudo lsblk /dev/xvdf
$ blkid /dev/xvdf1
```

Add the following lines to `/etc/fstab` (warn: if you detach this volume, the instance will
fail at startup, even if device is not mounted in auto mode):

```
#
# /etc/fstab
# Created by anaconda on Mon Nov  9 20:20:10 2015
#
# Accessible filesystems, by reference, are maintained under '/dev/disk'
# See man pages fstab(5), findfs(8), mount(8) and/or blkid(8) for more info
#
UUID=379de64d-ea11-4f5b-ae6a-0aa50ff7b24d /                       xfs     defaults        0 0

# Data volume
# UUID=cb8fc03c-d634-4a2e-944a-7fe61b9a37fd /mnt/data/              ext4    defaults,users,noauto               0 0
```

Mount manually the volume:

```
$ sudo mkdir /mnt/data
$ sudo mount -t ext4 -O rw,defaults,user,noauto UUID=cb8fc03c-d634-4a2e-944a-7fe61b9a37fd /mnt/data/
```

### Mount SWAP

Follow [this][add-swap] guide:

```
$ sudo dd if=/dev/zero of=/swapfile bs=1024 count=4194304
$ sudo mkswap /swapfile
$ sudo swapon /swapfile
```

Add the following line in `/etc/fstab`:

```
# Swap file
/swapfile               swap                    swap    defaults        0 0
```

[add-swap]: https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/3/html/System_Administration_Guide/s1-swap-adding.html

### Manage permission

The first time, you need to add user to hdfs group. Then create hdfs directory and
manage permissions, as stated [here][hadoop-fs-mkdir]

```
$ sudo usermod -aG hdfs $(whoami)
$ sudo -u hdfs hadoop fs -mkdir -p /user/paolo/capstone/airline_origin_destination/raw_data/
$ sudo -u hdfs hadoop fs -chown -R $(whoami) /user/paolo/
```

[hadoop-fs-mkdir]: http://stackoverflow.com/questions/22676593/hadoop-fs-mkdir-permission-denied

## Loading Origin / Destination dataset

**WARNING: data not used**

Set directory to data preparation

```
$ cd ~/capstone/data_preparation
```

Then call python script to create raw_data directory:

```
$ python putHDFS.py --input_path=/mnt/data/aviation/airline_origin_destination/ \
  --output_path=/user/paolo/capstone/airline_origin_destination/raw_data/ 2>&1 | tee \
  output_airline_origin_destination.log
```

Listing directory contents:

```
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/raw_data/
```

Set directory to `~/capstone/origin_destination`

```
$ cd ~/capstone/origin_destination
```

Call a *pig* script passing input directory and output file:

```
$ pig -x mapreduce -p input=/user/paolo/capstone/airline_origin_destination/raw_data/ \
  -p filtered=/user/paolo/capstone/airline_origin_destination/filtered_data \
  load_origin_destination.pig
```

List resuts in *hadoop FS*:

```
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/popular/
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/filtered_data.gz/
```

Dump results on screeen:

```
$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/popular/part-r-00000
$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/filtered_data/part-m-00000 | head
```

## Processing and filtering Ontime data

Set directory to data preparation, and create the `raw_data` directory:

```
$ cd ~/capstone/data_preparation
$ hadoop fs -mkdir -p /user/paolo/capstone/airline_ontime/raw_data/
```

Then call python script to create `raw_data` directory:

```
$ python putHDFS.py --input_path=/mnt/data/aviation/airline_ontime/ \
  --output_path=/user/paolo/capstone/airline_ontime/raw_data/ 2>&1 | tee output_airline_ontime.log
```

Listing directory contents:

```
$ hadoop fs -ls /user/paolo/capstone/airline_ontime/raw_data/
```

Set directory to `~/capstone/ontime`

```
$ cd ~/capstone/ontime
```

Call a pig script passing input directory and output file:

```
$ pig -x mapreduce -p input=/user/paolo/capstone/airline_ontime/raw_data/ \
  -p filtered=/user/paolo/capstone/airline_ontime/filtered_data \
  load_ontime.pig
```

List resuts in *hadoop FS*:

```
$ hadoop fs -ls /user/paolo/capstone/airline_ontime/filtered_data
```

Dump results on screeen:

```
$ hadoop fs -cat /user/paolo/capstone/airline_ontime/filtered_data/part-m-00000 | head
```

## Get lookup table for ontime dataset

Download data:

```
$ wget http://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_AIRLINE_ID -O /mnt/data/capstone/aviation/lookup/Lookup_AirlineID.csv
```

Creating directories in *hadoop file system*:

```
$ hadoop fs -mkdir -p /user/paolo/capstone/lookup/
```

Put *origin-destination* data in *hadoop filesystem*

```
$ hadoop fs -put /mnt/data/aviation/lookup/Lookup_AirlineID.csv  /user/paolo/capstone/lookup/
```

Listing directory contents:

```
$ hadoop fs -ls /user/paolo/capstone/lookup/
```

## 1.1) Rank the top 10 most popular airports by numbers of flights to/from the airport.

Set directory to `~/capstone/ontime`. Call a *pig* script passing input directory and output file

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  get_top10.pig
```

Here's the top10 airport:

```
(ORD,12449354)
(ATL,11540422)
(DFW,10799303)
(LAX,7723596)
(PHX,6585532)
(DEN,6273787)
(DTW,5636622)
(IAH,5480734)
(MSP,5199213)
(SFO,5171023)
```

## 1.2) Rank the top 10 airlines by on-time arrival performance.

Set directory to `~/capstone/ontime` and call:

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ top10_airlines.pig
$ spark-submit --master yarn --executor-cores=4 --num-executors 6 top10_airlines.py
```

Here's the top10 airlines:

```
(19690,-1.01180434574519)
(19678,1.1569234424812056)
(19391,1.4506385127822803)
(20295,4.747609195734892)
(20384,5.3224309999287875)
(20436,5.465881148819851)
(19386,5.557783392671835)
(19393,5.5607742598815735)
(20304,5.736312463662878)
(20363,5.8671846616957595)
```

## 2.1) Rank carriers by airports

>Clarification 1/27/2016: For questions 1 and 2 below, we are asking you to find,
for each airport, the top 10 carriers and destination airports from that airport
with respect to on-time departure performance. We are not asking you to rank the
overall top 10 carriers/airports.

Create cassandra tables:

```
USE capstone;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
```

Set directory to `~/capstone/ontime` and call:

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  -p results=/user/paolo/capstone/airline_ontime/top10_carriersByAirport/ top10_carriersByAirport.pig
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 top10_carriersByAirport.py
```

Provide the results using the following airport codes:

* CMI (University of Illinois Willard Airport)
* BWI (Baltimore-Washington International Airport)
* MIA (Miami International Airport)
* LAX (Los Angeles International Airport)
* IAH (George Bush Intercontinental Airport)
* SFO (San Francisco International Airport)

```
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'CMI';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'BWI';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'MIA';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'LAX';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'IAH';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SFO';
```

Or you can use a CQL script:

```
$ cqlsh -f top10_carriersByAirport.cql master > top10_carriersByAirport.txt
```

## 2.2) Rank airport by airports

Create cassandra tables:

```
USE capstone;
CREATE TABLE airportsbyairport ( origin TEXT, destination TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
```

Set directory to `~/capstone/ontime` and call:

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  -p results=/user/paolo/capstone/airline_ontime/top10_airportsByAirport/ top10_airportsByAirport.pig
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 top10_airportsByAirport.py
```

Provide the results using the following airport codes.

CMI (University of Illinois Willard Airport)
BWI (Baltimore-Washington International Airport)
MIA (Miami International Airport)
LAX (Los Angeles International Airport)
IAH (George Bush Intercontinental Airport)
SFO (San Francisco International Airport)

You can use a CQL script:

```
$ cqlsh -f top10_airportsByAirport.cql master > top10_airportsByAirport.txt
```

## 2.3) Rank carriers by path

Create cassandra tables:

```
USE capstone;
CREATE TABLE carriersbypath ( origin TEXT, destination TEXT, airlineid INT, airline TEXT, arrdelay FLOAT, PRIMARY KEY(origin, destination, arrdelay));
```

Set directory to `~/capstone/ontime` and call:

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  -p results=/user/paolo/capstone/airline_ontime/top10_carriersByPath/ top10_carriersByPath.pig
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 top10_carriersByPath.py
```

You can use a CQL script:

```
$ cqlsh -f top10_carriersByPath.cql master > top10_carriersByPath.txt
```

## 3.1) Rank airport by popularity

Set directory to `~/capstone/ontime`. Call a *pig* script passing input directory and output file

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  -p popular=/user/paolo/capstone/airline_ontime/popular/ get_popular.pig
```

Dump results on screeen:

```
$ hadoop fs -ls /user/paolo/capstone/airline_ontime/popular
$ hadoop fs -cat /user/paolo/capstone/airline_ontime/popular/part-m-00000 | head
$ hadoop fs -get /user/paolo/capstone/airline_ontime/popular/part-m-00000 $PWD/airportByPopularity.csv
```

## 3.2) Find best path

Tom wants to travel from airport X to airport Z. However, Tom also wants to stop
at airport Y for some sightseeing on the way. More concretely, Tom has the following
requirements (see Task 1 Queries for specific queries):

* The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
  For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
* Tom wants his flights scheduled to depart airport X before 12:00 PM local time and
  to depart airport Y after 12:00 PM local time.
* Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16:
  assume you know the actual delay of each flight).

Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month
(dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy
constraints (a) and (b) and have the best individual performance with respect to
constraint (c), if such flights exist.

Set directory to `~/capstone/ontime`. Call a *pig* script passing input directory and output file

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  -p popular=/user/paolo/capstone/airline_ontime/popular/ <pig_script>
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 --driver-memory=3G --executor-memory=4G best2paths.py
$ cqlsh -f best2paths.cql master > best2paths.txt
```

## Storing data into Cassandra

### Creating keyspace

```
CREATE KEYSPACE capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};
```

### Creating tables

Here, a combined primary key in order to store origin and depdelay in order

```
USE capstone;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
CREATE TABLE airportsbyairport ( origin TEXT, destination TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
CREATE TABLE carriersbypath ( origin TEXT, destination TEXT, airlineid INT, airline TEXT, arrdelay FLOAT, PRIMARY KEY(origin, destination, arrdelay));
CREATE TABLE best2path (startdate TEXT, flightnum1 INT, origin1 TEXT, dest1 TEXT, departure1 TIMESTAMP, arrival1 TIMESTAMP, arrdelay1 FLOAT, flightnum2 INT, origin2 TEXT, dest2 TEXT, departure2 TIMESTAMP, arrival2 TIMESTAMP, arrdelay2 FLOAT, PRIMARY KEY(origin1, dest1, dest2, startdate));
```

### Quering Cassandra

Examples of queries in Cassandra:

```
USE capstone;
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'DEN';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'DEN';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'DEN';
SELECT startdate, flightnum1, origin1, dest1, departure1, arrival1, arrdelay1, flightnum2, origin2, dest2, departure2, arrival2, arrdelay2 FROM best2path LIMIT 10;
SELECT startdate,
       flightnum1,
       origin1,
       dest1,
       departure1,
       arrival1,
       arrdelay1,
       flightnum2,
       origin2,
       dest2,
       departure2,
       arrival2,
       arrdelay2
  FROM best2path
 WHERE origin1 = 'SAT' AND
       dest1 = 'BNA' AND
       dest2 = 'AUS' AND
       startdate = '03/06/2008';
```

### Dump cassandra table:

```
USE capstone;
COPY best2path (startdate, flightnum1, origin1, dest1, departure1, arrival1, arrdelay1, flightnum2, origin2, dest2, departure2, arrival2, arrdelay2 ) TO 'best2path.csv' ;
```

### Load cassandra table:

```
USE capstone
COPY best2path (startdate, flightnum1, origin1, dest1, departure1, arrival1, arrdelay1, flightnum2, origin2, dest2, departure2, arrival2, arrdelay2 ) FROM 'best2path.csv';
```

# Cloud computing capstone - Part 2

## Testing Kafka installation

Inspect `/usr/hdp/current/kafka-broker/config/server.properties` for *zookeper* and
*listener* addresses:

```
$ grep ip-172-31-29-45.eu-central-1.compute.internal /usr/hdp/current/kafka-broker/config/server.properties                
listeners=PLAINTEXT://ip-172-31-29-45.eu-central-1.compute.internal:6667
zookeeper.connect=ip-172-31-29-45.eu-central-1.compute.internal:2181,ip-172-31-29-47.eu-central-1.compute.internal:2181,ip-172-31-29-46.eu-central-1.compute.internal:2181
```

In order to enable topic deletion, edit `/usr/hdp/current/kafka-broker/config/server.properties`
in such way:

```
$ delete.topic.enable=true
```

In ambari, this modification in transient. Every time kafka is restarted, ambari will
define a default configuration. To make such modifications persistent, you have to
edit a new kakfa configuration in ambari web service. You can add more than one kafka
broker.

To create a topic, specify the *zookeper* address. You can use node names defined in
`/etc/hosts`:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master:2181 \
  --replication-factor 1 --partitions 1 --topic test
```
List all topics:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper master:2181
```

Create a producer. Specify *zookeper* and *listener* addresses, and topic name:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list node4:6667 \
  --topic test
```

Type some text on the screen. It will reach the *consumer*. You can also `cat` a file
and stream it into kafka:

```
$ hadoop fs -cat /user/paolo/capstone/airline_ontime/test/test.csv | \
  /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list node4:6667 --topic test
```

In another terminal, launch
the *consumer*. Remember to specify *zookeeper* address:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper master:2181 \
  --topic test --from-beginning
```

You should see all the typed text in the *producer* window. Delete a topic:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic test --zookeeper localhost:2181
```

Quering zookeper:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-consumer-offset-checker.sh --zookeeper master:2181 --group spark-streaming-consumer --topic ontime
$ /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list node4:6667 --topic ontime --time -2
$ /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list node4:6667 --topic ontime --time -1
```

### Testing pyspark with kafka

You need a running *producer* since this script doesn't read a topic from beginning.
Next, you have to launch spark with the appropriate `spark-streaming-kafka-assembly`
in which your spark version appears. [here][kafka-assembly] is the address in which
libraries are. Pay attention since there is a [issue] with scala 2.11, so get the 2.10
libraries. You can get download and cache locally libraries by giving such command:

```
$ cd /home/ec2-user/capstone/streaming
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 \
  kafka_wordcount.py master:2181 test
```

[kafka-assembly]: https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-assembly_2.10/1.5.2/
[kafka-scala-issure]: https://groups.google.com/forum/#!topic/adam-developers/j5bzgpK5-aU

### Install the Kafka Python driver

In order to create *producer* and *consumer*, you can install a python package:

```
$ yum install python-setuptools
$ easy_install pip
$ git clone https://github.com/dpkp/kafka-python.git
$ cd kafka-python
$ pip install .
$ yum install gcc-c++
$ pip install -U setuptools
$ pip install pydoop
```

## Prepare topic for exercises

Empty the `topic` and create a new topic. Then pass filtered data from HDFS with
the python `kafka-producer.py` script:

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic ontime --zookeeper master:2181
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master:2181 \
  --replication-factor 1 --partitions 1 --topic ontime
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper master:2181
$ cd ~/capstone/ontime
$ python kafka-producer.py
```

## 1.1) Rank the top 10 most popular airports by numbers of flights to/from the airport.

Set directory to `~/capstone/ontime`. Reset temporary files and topics

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic top10_airports --zookeeper master:2181
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 \
  --partitions 1 --topic top10_airports
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_airports/
$ hadoop fs -rm -r -skipTrash /user/ec2-user/intermediate/top10_airports/
```

Call a *python* script:

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 \
  --master yarn --executor-cores=3 --num-executors 4 --driver-memory=3G --executor-memory=6G \
  top10_airports.py
```

When there is no new output, interrupt the python script and then put the intermediate
results on new kafka topic:

```
$ python kafka-producer.py -d /user/paolo/intermediate/top10_airports/ -t top10_airports
```

Then call the final script:

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 \
  --master yarn --executor-cores=3 --num-executors 4 --driver-memory=3G --executor-memory=6G \
  top10_airports.2.py
```

Here's the top10 airport

```
(ORD,12449354)
(ATL,11540422)
(DFW,10799303)
(LAX,7723596)
(PHX,6585532)
(DEN,6273787)
(DTW,5636622)
(IAH,5480734)
(MSP,5199213)
(SFO,5171023)
```

## 1.2) Rank the top 10 airlines by on-time arrival performance.

Set directory to `~/capstone/ontime`. Reset temporary files and topics

```
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic top10_airlines --zookeeper master:2181
$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 \
  --partitions 1 --topic top10_airlines
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_airlines/
$ hadoop fs -rm -r -skipTrash /user/ec2-user/intermediate/top10_airlines/
```

Call a *python* script:

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 \
  --master yarn --executor-cores=3 --num-executors 4 --driver-memory=3G --executor-memory=6G \
  top10_airline.py
```

When there is no new output, interrupt the python script and then put the intermediate
results on new kafka topic:

```
$ python kafka-producer.py -d /user/paolo/intermediate/top10_airlines/ -t top10_airlines
```

Then call the final script:

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.3.1 \
  --master yarn --executor-cores=3 --num-executors 4 --driver-memory=3G --executor-memory=6G \
  top10_airlines.2.py
```

Here's the top10 airlines:

```
(19393,5.849497681607419)
```

## 2.1) Rank carriers by airports

>Clarification 1/27/2016: For questions 1 and 2 below, we are asking you to find,
for each airport, the top 10 carriers and destination airports from that airport
with respect to on-time departure performance. We are not asking you to rank the
overall top 10 carriers/airports.

Dump cassandra table:

```
USE capstone;
COPY carriersbyairport (origin, depdelay, airline, airlineid ) TO 'carriersbyairport.csv' ;
```

Create cassandra tables:

```
USE capstone;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, rank INT, PRIMARY KEY(origin, rank ));
```

Set directory to `~/capstone/ontime` and call:

```
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_carriersByAirport
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 \
  --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 top10_carriersByAirport.py
```

Provide the results using the following airport codes:

* SRQ
* CMH
* JFK
* SEA
* BOS

```
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SRQ';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'CMH';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'JFK';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SEA';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'BOS';
```

Or you can use a CQL script:

```
$ cqlsh -f top10_carriersByAirport.cql master > top10_carriersByAirport.txt
```

## 2.2) Rank airport by airports

Dump cassandra table:

```
USE capstone;
COPY airportsbyairport (origin, depdelay, destination ) TO 'airportsbyairport.csv' ;
```


Create cassandra tables:

```
USE capstone;
CREATE TABLE airportsbyairport ( origin TEXT, destination TEXT, depdelay FLOAT, rank INT, PRIMARY KEY(origin, rank ));
```

Set directory to `~/capstone/ontime` and call:

```
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_airportsByAirport
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 \
  --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 top10_airportsByAirport.py
```

Provide the results using the following airport codes.

* SRQ
* CMH
* JFK
* SEA
* BOS

You can use a CQL script:

```
$ cqlsh -f top10_airportsByAirport.cql master > top10_airportsByAirport.txt
```

## 2.3) Rank carriers by path

Dump cassandra table:

```
USE capstone;
COPY carriersbypath (origin, destination, arrdelay, airline, airlineid ) TO 'carriersbypath.csv' ;
```

Create cassandra tables:

```
USE capstone;
CREATE TABLE carriersbypath ( origin TEXT, destination TEXT, airlineid INT, airline TEXT, arrdelay FLOAT, rank INT, PRIMARY KEY(origin, destination, rank ));
```

Set directory to `~/capstone/ontime` and call:

```
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_carriersByPath
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 \
  --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 top10_carriersByPath.py
```

You can use a CQL script:

```
$ cqlsh -f top10_carriersByPath.cql master > top10_carriersByPath.txt
```

## 3.2) Find best path

Tom wants to travel from airport X to airport Z. However, Tom also wants to stop
at airport Y for some sightseeing on the way. More concretely, Tom has the following
requirements (see Task 1 Queries for specific queries):

* The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
  For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
* Tom wants his flights scheduled to depart airport X before 12:00 PM local time and
  to depart airport Y after 12:00 PM local time.
* Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16:
  assume you know the actual delay of each flight).

Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month
(dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy
constraints (a) and (b) and have the best individual performance with respect to
constraint (c), if such flights exist.

Dump cassandra table:

```
USE capstone;
COPY best2path (origin1, dest1, dest2, startdate, arrdelay1, arrdelay2, arrival1, arrival2, departure1, departure2, flightnum1, flightnum2, origin2 ) TO 'best2path.csv' ;
```

Create cassandra tables:

```
USE capstone;
CREATE TABLE best2path (startdate TEXT, flightnum1 INT, origin1 TEXT, dest1 TEXT, departure1 TIMESTAMP, arrival1 TIMESTAMP, arrdelay1 FLOAT, flightnum2 INT, origin2 TEXT, dest2 TEXT, departure2 TIMESTAMP, arrival2 TIMESTAMP, arrdelay2 FLOAT, PRIMARY KEY(origin1, dest1, dest2, startdate));
```

Set directory to `~/capstone/ontime`. Call a *python* script passing input directory and output file

```
$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/best2paths
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar \
  --driver-class-path ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5.jar  \
  --py-files ${PYSPARK_CASSANDRA}/pyspark_cassandra-0.1.5-py2.7.egg \
  --conf spark.cassandra.connection.host=master"
$ spark-submit $PYSPARK_SUBMIT_ARGS --master yarn --executor-cores=3 --num-executors 4 \
  --driver-memory=3G --executor-memory=4G --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.2 \
  best2paths.py
$ cqlsh -f best2paths.cql master > best2paths.txt
```
