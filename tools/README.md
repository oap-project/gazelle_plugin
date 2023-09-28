# Spark Eventlog Analyzer
The pyspark script to analyze Gazelle's eventlog

## Prequisites

### Jupyter Installation
```
pip3 install jupyter
```

### Notebook Installation
```
pip3 install notebook
```

### iPyKernel
```
pip3 install ipykernel
```
### FindSpark
```
pip3 install findspark
```

### Matplotlib
```
pip3 install matplotlib
```

### Seaborn
```
pip3 install seaborn
```

### Pandasql
```
pip3 install pandasql
```

### PyHDFS
```
pip3 install pyhdfs
```

### PyArrow
```
pip3 install pyarrow
```

# Eventlog Analyzer Tools
The eventlog analyzer includes sparklog.ipynb and gazelle_analysis.ipynb.
Please put them into jupyter directory.
## sparklog.ipynb
sparklog.ipynb is the function definition for spark eventlog analyzer

## gazelle_analysis.ipynb
gazelle_analysis is the main program to call sparklog.ipynb and load the eventlog from hdfs.

##How it works:
Launch gazelle_analysis.ipynb as main script

##Parameters:
In Analysis:generate_trace_view, the url for display.
In App_Log_Analysis:get_basic_state, the url for display.
In App_Log_Analysis:get_app_info, the url for display.
In show_rst function, the url for html.
In pyhdfs, the url for HDFS hosts.

##To run in in commandline:
jupyter nbconvert --execute --to notebook --inplace --allow-errors --ExecutePreprocessor.timeout=-1 ./gazelle_analysis.ipynb --template classic

##To convert into HTML:
jupyter nbconvert --to html ./gazelle_analysis.ipynb --output ./gazelle_analysis.html --template classic

# Tools to collect sar information and generation trace view(.json):
You can also use below files to collect sar information.
The purpose for this tool is to generate a json file with sar information.
After the json has been generated, you can use catapult to view your json file.

monitor.py: main program to collect sar information, it must be started before your application and stopped after your application.
post_process.sh: Post process the sar files after the monitor stop.
run_example.sh: An example to teach you how to use monitor.py with your application.
template.ipynb: A template to use for generating trace view(.json).

## Usage
Before run the tool, please make sure to set up some settings including
In monitor.py,
clients:the nodes in your cluster.
base_dir: the base directory name to put logs.
local_profile_dir: the local location to put logs.
hdfs_address: the hdfs address to copy all the logs to hdfs.

In sparklog.ipynb,
Please replace sr124 to the master in your cluster and use to process the logs.
Please replace sr525 to the catapult server. 

You can check run_example.sh to see how to use the script to collect sar information.
Please add below command in your script:
```
appid=`yarn application -list 2>&1 | tail -n 1 | awk -F"\t" '{print $1}'`
rm -f log/memory*.csv
python3 ./monitor.py start $appid
$run_your_query
python3 ./monitor.py stop $appid "spark_logs"
```
