
#################################################################################
# Requirements                                                                  #
#################################################################################

* subversion
* ant:

#################################################################################
# Registering different types of storage                                        #
# https://cwiki.apache.org/confluence/display/PIG/PiggyBank                     #
#################################################################################



#################################################################################
# Processing and filtering Origin/Destination data                              #
#################################################################################

Set directory to data preparation (WARNING ONLY 1000 rows were considered)

$ cd ~/capstone/data_preparation

Then call python script to create raw_data directory:

$ python putHDFS.py --input_path=/mnt/data/aviation/airline_origin_destination/ --output_path=/mnt/data/raw_data/airline_origin_destination/ 2>&1 | tee output_airline_origin_destination.txt

Set directory to ~/capstone/origin_destination

$ cd ~/capstone/origin_destination

Creating directories in hadoop file system

$ hadoop fs -mkdir -p /user/paolo/capstone/airline_origin_destination/raw_data/

Put origin-destination data in hadoop filesystem

$ hadoop fs -put /mnt/data/raw_data/airline_origin_destination/* /user/paolo/capstone/airline_origin_destination/raw_data/

Listing directory contents

$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/raw_data/

Call a pig script passing input directory and output file

$ pig -x mapreduce -p input=/user/paolo/capstone/airline_origin_destination/raw_data/ \
  -p output=/user/paolo/capstone/airline_origin_destination/top_10 \
  -p filtered=/user/paolo/capstone/airline_origin_destination/filtered_data/ \
  load_origin_destination.pig

List resuts in hadoop FS:

$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/top_10/
$ hadoop fs -ls /user/paolo/capstone/airline_origin_destination/filtered_data/

Dump results on screeen:

$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/top_10/part-r-00000
$ hadoop fs -cat /user/paolo/capstone/airline_origin_destination/filtered_data/part-m-00000 | head

Processing filtered data and dump top 10 airports
-------------------------------------------------

Set directory to ~/capstone/origin_destination

$ cd ~/capstone/origin_destination

Call a pig script passing input directory and output file

$ pig -x mapreduce -p filtered=/user/paolo/capstone/airline_origin_destination/filtered_data/ \
  get_top10.pig

#################################################################################
# Processing and filtering Origin/Destination data                              #
#################################################################################

Set directory to data preparation (WARNING ONLY 1000 rows were considered)

$ cd ~/capstone/data_preparation

Then call python script to create raw_data directory:

$ python putHDFS.py --input_path=/mnt/data/aviation/airline_ontime/ --output_path=/mnt/data/raw_data/airline_ontime/ 2>&1 | tee output_airline_ontime.txt

Set directory to ~/capstone/ontime

$ cd ~/capstone/ontime

Creating directories in hadoop file system

$ hadoop fs -mkdir -p /user/paolo/capstone/airline_ontime/raw_data/

Put origin-destination data in hadoop filesystem

$ hadoop fs -put /mnt/data/raw_data/airline_ontime/* /user/paolo/capstone/airline_ontime/raw_data/

Listing directory contents

$ hadoop fs -ls /user/paolo/capstone/airline_ontime/raw_data/

Call a pig script passing input directory and output file: