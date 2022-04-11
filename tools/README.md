### Spark Eventlog Analyzer
The pyspark script to analyze Gazelle's eventlog

## Prequisites

# Jupyter Installation
```
pip3 install jupyter
```

# Notebook Installation
```
pip3 install notebook
```

# iPyKernel
```
pip3 install ipykernel
```
# FindSpark
```
pip3 install findspark
```

# Matplotlib
```
pip3 install matplotlib
```

# Seaborn
```
pip3 install seaborn
```

# Pandasql
```
pip3 install pandasql
```

# PyHDFS
```
pip3 install pyhdfs
```

# PyArrow
```
pip3 install pyarrow
```

### Put below two .ipynb in the jupyter root directory
## sparklog.ipynb
sparklog.ipynb is the function definition for spark eventlog analyzer

## gazelle_analysis.ipynb
gazelle_analysis is the main program to call sparklog.ipynb and load the eventlog from hdfs.

###How it works:
Launch gazelle_analysis.ipynb as main script

###Parameters:
In Analysis:generate_trace_view, the url for display.
In App_Log_Analysis:get_basic_state, the url for display.
In App_Log_Analysis:get_app_info, the url for display.
In show_rst function, the url for html.
In pyhdfs, the url for HDFS hosts.



###To run in in commandline:
jupyter nbconvert --execute --to notebook --inplace --allow-errors --ExecutePreprocessor.timeout=-1 ./gazelle_analysis.ipynb --template classic

###To convert into HTML:
jupyter nbconvert --to html ./gazelle_analysis.ipynb --output ./gazelle_analysis.html --template classic
