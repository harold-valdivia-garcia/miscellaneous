#!/bin/bash

#echo $0
#echo $1
#exit 0
local_training_set=$1
hadoop_training_dir=training-dir
hadoop_first_output_dir=bayes-first-output-dir
hadoop_prediction_dir=bayes-testing-prediction-dir

#Remove 
echo -e "Initializing....\n\n"
hadoop fs -rm -R $hadoop_training_dir
hadoop fs -rm -R $hadoop_first_output_dir
hadoop fs -rm -R $hadoop_prediction_dir
echo -e "\n\n\n\n"

# Put the data
echo -e "Loading the training-set...\n\n"
hadoop fs -mkdir $hadoop_training_dir
hadoop fs -put $local_training_set $hadoop_training_dir/

# Show the data
hadoop fs -cat $hadoop_training_dir/* | head -n 60 |tail -n 40
echo -e "\n\n\n\n"

# Run hadoop
echo -e "Count the frequencies....\n\n"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-files miscellaneous \
	-mapper "miscellaneous/bayes_counter.py" \
	-reducer "miscellaneous/bayes_reducer.py" \
	-input $hadoop_training_dir \
	-output $hadoop_first_output_dir 
echo -e "\n\n\n\n"

# Show the first 
hadoop fs -cat $hadoop_first_output_dir/* | head -n 40
echo -e "\n\n\n\n"

# Run hadoop
echo -e "Predict the testing set....\n\n"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-D mapred.reduce.tasks=0 \
	-D mapred.map.tasks=1 \
	-files miscellaneous \
	-mapper "miscellaneous/bayes_classify.py" \
	-input $hadoop_first_output_dir \
	-output $hadoop_prediction_dir 
echo -e "\n\n\n\n"

hadoop fs -cat $hadoop_prediction_dir/* | tail -n 40
#echo -e "\n\n"

exit 0
