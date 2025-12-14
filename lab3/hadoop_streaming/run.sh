export CSV_PATH="/home/adymeda/code/bigdata/data/steam_reviews.csv"
export OUT_PATH="/home/adymeda/code/bigdata/lab3/hadoop_streaming/out"

hadoop jar "/DATA/AppData/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar" -D mapreduce.framework.name=local -D fs.defaultFS=file:/// -D mapreduce.job.reduces=1 -input $CSV_PATH -output $OUT_PATH -mapper "python mapper.py" -reducer "python reducer.py"