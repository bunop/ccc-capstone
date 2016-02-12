from pyspark.streaming import StreamingContext
dvc = [[-0.1, -0.1], [0.1, 0.1], [1.1, 1.1], [0.75, 0.75], [0.9, 0.9]]
dvc = [sc.parallelize(i, 1) for i in dvc]
ssc = StreamingContext(sc, 2.0)
input_stream = ssc.queueStream(dvc)

def get_output(rdd):
    rdd_data = rdd.collect()
    if 0.75 in rdd_data:
    	print "Ending marker found", rdd_data
    	ssc.stop()
    else:
    	print "Not found ending marker. Continuing"
    	print rdd_data

input_stream.foreachRDD(get_output)
ssc.start()