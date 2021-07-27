# Gazelle on Kubernetes
This README contains the script for dockerfile and the script to run thrift server for benchmark like TPC-H, TPC-DS, ...etc.

## Building Spark Image
There are two methods to build Gazelle Docker Image

### Prerequisite
Building Spark Base Docker Image
```
docker build --tag spark-centos:3.1.1 .
```

### Method1: Building OAP Docker Image including Gazelle and other OAP projects
```
docker build --tag oap-centos:1.2 .
```

### Method2: Building Gazelle Docker Image only

TBD

## Run Spark on Kubernetes
Before doing this, we assume you have setup Kubernetes enironment and it worked properly. All the tool scripts are under "spark" folder.
We tested these scripts in Minikube environment. If you are using other Kubernetes distributions, you may need to make some changes to work properly.

### Create Spark User and Assign Cluster Role
Spark running on Kubernetes needs edit role of your Kubernetes cluster to create driver or executor pods.
Go to spark folder and execute the following command to create "spark" user and assign the role. Make sure you have logged in Kubernetes and have administor role of the cluster.
```
sh ./spark-kubernetes-prepare.sh
```

### Run Spark/OAP Job in Cluster mode
In Kubernetes, you can run Spark/OAP job using spark-submit in Cluster mode at any node which has access to your Kubernetes API server.

You can edit spark configuration files in spark/conf directory

#### Run Spark Pi Job
You can run a Spark Pi job for a simple testing of the enironment is working. Execute the following command. If you are running on the master node,  you can ignore the --master parameter.
For example:
```
sh ./spark-pi.sh --master localhost:8443  --image oap-centos:1.1.1  --spark_conf ./conf
