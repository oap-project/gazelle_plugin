# OAP Installation Guide

This document introduces how to install OAP and its dependencies on your cluster nodes by ***Conda***. 
Follow steps below on ***every node*** of your cluster to set right environment for each machine.

## Contents
  - [Prerequisites](#prerequisites)
  - [Installing OAP](#installing-oap)
  - [Configuration](#configuration)

### Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29, CentOS 7.6 (kernel 4.18.16) and Ubuntu 20.04 (kernel 5.4.0-65-generic).  

- **Conda Requirements**   
Install Conda on your cluster nodes with below commands and follow the prompts on the installer screens.:
```bash
$ wget -c https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
$ chmod +x Miniconda2-latest-Linux-x86_64.sh 
$ bash Miniconda2-latest-Linux-x86_64.sh 
```
For changes to take effect, ***close and re-open*** your current shell. 
To test your installation,  run the command `conda list` in your terminal window. A list of installed packages appears if it has been installed correctly.

### Installing OAP

Create a Conda environment and install OAP Conda package.

```bash
$ conda create -n oapenv -c conda-forge -c intel -y oap=1.5.0.spark32
```

Once finished steps above, you have completed OAP dependencies installation and OAP building, and will find built OAP jars under `$HOME/miniconda2/envs/oapenv/oap_jars`



###  Configuration

Once finished steps above, make sure libraries installed by Conda can be linked by Spark, please add the following configuration settings to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.executorEnv.LD_LIBRARY_PATH   $HOME/miniconda2/envs/oapenv/lib
spark.executor.extraLibraryPath     $HOME/miniconda2/envs/oapenv/lib
spark.driver.extraLibraryPath       $HOME/miniconda2/envs/oapenv/lib
spark.executorEnv.CC                $HOME/miniconda2/envs/oapenv/bin/gcc
spark.executor.extraClassPath       $HOME/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
spark.driver.extraClassPath         $HOME/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
```

Then you can follow the corresponding feature documents for more details to use them.






