
# Cloud computing capstone (part 1)

## Requirements

* subversion
* [ant (HOWTO install)][install-ant]: get [binary][ant-binary]
* jna

[install-ant]: http://xmodulo.com/how-to-install-apache-ant-on-centos.html
[ant-binary]: http://mirrors.muzzy.it/apache//ant/binaries/apache-ant-1.9.6-bin.tar.gz

### Registering different types of storage

Follow this [guide][install-piggybank]:

```
$ cd /home/paolo/capstone
$ mkdir piggy_bank
$ cd piggy_bank
$ svn checkout http://svn.apache.org/repos/asf/pig/trunk/ .
$ ant
$ cd contrib/piggybank/java
$ ant
```

Next, you have to refister piggybank.jar and refer to the appropriate module, Ex:

```
grunt> REGISTER /home/paolo/capstone/piggy_bank/contrib/piggybank/java;
grunt> data = LOAD 'my.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
  AS (...);
```

More info [here][reading-csv-in-pig]

[install-piggybank]: https://cwiki.apache.org/confluence/display/PIG/PiggyBank
[reading-csv-in-pig]: http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma

### Install Cassandra

Add the `datastax` repository to your repos `/etc/yum.repos.d/datastax.repo`:

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

[install-cassandra]: http://www.liquidweb.com/kb/how-to-install-cassandra-on-centos-6/

### Install pyspark-cassandra

```
$ curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
$ yum install sbt
$ git clone https://github.com/bigstepinc/pyspark-cassandra.git
$ cd pyspark-cassandra
$ git submodule update --init --recursive
$ make dist
$ export PYSPARK_ROOT=/home/paolo/capstone/pyspark-cassandra/target
$ export PYSPARK_SUBMIT_ARGS="--jars ${PYSPARK_ROOT}/pyspark_cassandra-0.1.5.jar  \
    --driver-class-path ${PYSPARK_ROOT}/pyspark_cassandra-0.1.5.jar  \
    --py-files ${PYSPARK_ROOT}/pyspark_cassandra-0.1.5-py2.6.egg \
    --conf spark.cassandra.connection.host=127.0.0.1"
$ ipython --profile spark
```

Example on pyspark_cassandra dataframe could be found [here][pyspark-dataframe-cassanra].

[official-pypsark_cassandra]: https://github.com/TargetHolding/pyspark-cassandra.git
[centos-6-pyspark_cassandra]: https://github.com/bigstepinc/pyspark-cassandra.git
[pyspark-dataframe-cassanra]: http://rustyrazorblade.com/2015/05/on-the-bleeding-edge-pyspark-dataframes-and-cassandra/


## Processing and filtering Origin/Destination data

Set directory to data preparation **(WARNING ONLY 1000 rows were considered)**

```
$ cd ~/capstone/data_preparation
```

Then call python script to create raw_data directory:

```
$ python putHDFS.py --input_path=/mnt/data/aviation/airline_origin_destination/ \
  --output_path=/mnt/data/raw_data/airline_origin_destination/ 2>&1 | tee \
  output_airline_origin_destination.log
```
Set directory to `~/capstone/origin_destination`

```
$ cd ~/capstone/origin_destination
```

Creating directories in hadoop file system:

```
$ hadoop fs -mkdir -p /user/paolo/capstone/airline_origin_destination/raw_data/
```

Put *origin-destination* data in *hadoop filesystem*:

```
$ hadoop fs -put /mnt/data/raw_data/airline_origin_destination/* /user/paolo/capstone/airline_origin_destination/raw_data/
```

Listing directory contents:

```
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/raw_data/
```

Call a *pig* script passing input directory and output file:

```
$ pig -x mapreduce -p input=/user/paolo/capstone/airline_origin_destination/raw_data/ \
  -p output=/user/paolo/capstone/airline_origin_destination/top_10 \
  -p filtered=/user/paolo/capstone/airline_origin_destination/filtered_data/ \
  load_origin_destination.pig
```

List resuts in *hadoop FS*:

```
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/top_10/
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/filtered_data/
```

Dump results on screeen:

```
$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/top_10/part-r-00000
$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/filtered_data/part-m-00000 | head
```

**TODO**: Each file has the first record (header)


### Processing filtered data and dump top 10 airports

Set directory to `~/capstone/origin_destination`

```
$ cd ~/capstone/origin_destination
```

Call a *pig* script passing input directory and output file

```
$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_origin_destination/filtered_data/ \
  get_top10.pig
```

## Processing and filtering Origin/Destination data

Set directory to data preparation **(WARNING ONLY 1000 rows were considered)**

```
$ cd ~/capstone/data_preparation
```

Then call python script to create `raw_data` directory:

```
$ python putHDFS.py --input_path=/mnt/data/aviation/airline_ontime/ \
  --output_path=/mnt/data/raw_data/airline_ontime/ 2>&1 | tee output_airline_ontime.log
```

Set directory to `~/capstone/ontime`

```
$ cd ~/capstone/ontime
```

Creating directories in *hadoop file system*:

```
$ hadoop fs -mkdir -p /user/paolo/capstone/airline_ontime/raw_data/
```

Put *origin-destination* data in *hadoop filesystem*

```
$ hadoop fs -put /mnt/data/raw_data/airline_ontime/* /user/paolo/capstone/airline_ontime/raw_data/
```

Listing directory contents:

```
$ hadoop fs -ls /user/paolo/capstone/airline_ontime/raw_data/
```

Call a pig script passing input directory and output file:

```
$ pig -x mapreduce -p input=/user/paolo/capstone/airline_ontime/raw_data/ \
  -p filtered=/user/paolo/capstone/airline_ontime/filtered_data/ \
  load_ontime.pig
```

List resuts in *hadoop FS*:

```
$ hadoop fs -ls /user/paolo/capstone/airline_ontime/filtered_data/
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

## Using IPython with Spark

1. Create an iPython notebook profile for our Spark configuration:

```
$ ipython profile create spark
```

2. Create a file in `$HOME/.config/ipython/profile_spark/startup/00-pyspark-setup.py` and add the following:

```python
import os
import sys

# Configure the environment
if 'SPARK_HOME' not in os.environ:
  os.environ['SPARK_HOME'] = '/usr/hdp/current/spark-client'

  # Create a variable for our root path
  SPARK_HOME = os.environ['SPARK_HOME']

  # Add the PySpark/py4j to the Python Path
  sys.path.insert(0, os.path.join(SPARK_HOME, "python", "build"))
  sys.path.insert(0, os.path.join(SPARK_HOME, "python"))

  # Described here: http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/
  sys.path.insert(0, os.path.join(SPARK_HOME, "/python/lib/py4j-0.8.2.1-src.zip"))
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
[here][notebook-cloudera] and [here][spark-python]

[setting-ipython-notebook]: http://nbviewer.jupyter.org/gist/fperez/6384491/00-Setup-IPython-PySpark.ipynb
[notebook-cloudera]: http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/
[spark-python]: https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python


## Storing data into Cassandra

### Creating keyspace

```
CREATE KEYSPACE capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

### Creating tables

Here, a combined primary key in order to store origin and depdelay in order

```
USE capstone;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
CREATE TABLE airportsbyairport ( origin TEXT, destination TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
CREATE TABLE carriersbypath ( origin TEXT, destination TEXT, airlineid INT, airline TEXT, arrdelay FLOAT, PRIMARY KEY(origin, destination, arrdelay));
CREATE TABLE best2path (flightnum1 INT, origin1 TEXT, dest1 TEXT, departure1 TIMESTAMP, arrival1 TIMESTAMP, arrdelay1 FLOAT, flightnum2 INT, origin2 TEXT, dest2 TEXT, departure2 TIMESTAMP, arrival2 TIMESTAMP, arrdelay2 FLOAT, PRIMARY KEY(origin1, dest1, dest2));
```

### Quering Cassandra

For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

```
USE capstone;
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'DEN';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'DEN';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'DEN';
SELECT flightnum1, origin1, dest1, departure1, arrival1, arrdelay1, flightnum2, origin2, dest2, departure2, arrival2, arrdelay2 FROM best2path LIMIT 10;
```
