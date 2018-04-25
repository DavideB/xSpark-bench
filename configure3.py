from enum import Enum
import json
import os
from pathlib import Path
#from util.utils import string_to_bool
'''
from config import Heuristic, AZ_KEY_NAME, AZ_PUB_KEY_PATH, AZ_PRV_KEY_PATH, \
    AWS_ACCESS_ID, AWS_SECRET_KEY, AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID,\
    ROOT_DIR, CLUSTERS_CFG_FILENAME, CLUSTERS_CFG_PATH, \
    PROVIDER, AZ_LOCATION, AZ_SIZE, AZ_IMAGE, AZ_VHD_IMAGE, \
    AZ_RESOURCE_GROUP, AZ_STORAGE_ACCOUNT, AZ_SA_SKU, AZ_SA_KIND, AZ_NETWORK, \
    AZ_SUBNET, AZ_SECURITY_GROUP, DATA_AMI, CREDENTIAL_PROFILE, REGION, \
    KEY_PAIR_PATH, SECURITY_GROUP, PRICE, INSTANCE_TYPE, NUM_INSTANCE, \
    EBS_OPTIMIZED, REBOOT, KILL_JAVA, NUM_RUN, PROCESS_ON_SERVER, INSTALL_PYTHON3, \
    CLUSTER_ID, TAG, HDFS_MASTER, SPARK_SEQ_HOME, SPARK_2_HOME, C_SPARK_HOME, \
    SPARK_HOME, LOG_LEVEL, UPDATE_SPARK, UPDATE_SPARK_MASTER, UPDATE_SPARK_DOCKER, \
    ENABLE_EXTERNAL_SHUFFLE, LOCALITY_WAIT, LOCALITY_WAIT_NODE, LOCALITY_WAIT_PROCESS, \
    LOCALITY_WAIT_RACK, CPU_TASK, RAM_DRIVER, RAM_EXEC, OFF_HEAP, OFF_HEAP_BYTES, \
    CORE_VM, CORE_HT_VM, DISABLE_HT, ALPHA, BETA, DEADLINE, MAX_EXECUTOR, OVER_SCALE,\
    K, TI, T_SAMPLE, CORE_QUANTUM, CORE_MIN, CPU_PERIOD, RUN, SYNC_TIME, PREV_SCALE_FACTOR, \
    BENCH_NUM_TRIALS, BENCHMARK_PERF, BENCHMARK_BENCH, BENCH_CONF, TERMINATE, HDFS, \
    HADOOP_CONF, HADOOP_HOME, DELETE_HDFS, HEURISTIC, STAGE_ALLOCATION, CORE_ALLOCATION,  \
    DEADLINE_ALLOCATION, CONFIG_DICT, BENCH_LINES, PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, \
    TEMPORARY_STORAGE, UPDATE_SPARK_BENCH, UPDATE_SPARK_PERF, SPARK_PERF_FOLDER, \
    CLUSTER_MAP, VAR_PAR_MAP, INPUT_RECORD, NUM_TASK, SCALE_FACTOR
'''
import config as c

class Config(object):
    """
    Configuration class for cSpark test benchmark
    """    
    class Heuristic(Enum):
        CONTROL = 0
        FIXED = 1
        CONTROL_UNLIMITED = 2
    
    REGION = "us-west-2"                                #"""Region of AWS to use"""
    DATA_AMI = {"eu-west-1": {"ami": 'ami-bf61fbc8',    #"""AMI id for region and availability zone"""
                              "az": 'eu-west-1c',
                              "keypair": "simone",
                              "price": "0.0035"},
                "us-west-2": {"ami": 'ami-7f5ff81f',
                              "snapid": "snap-4f38bf1c",
                              "az": 'us-west-2c',
                              "keypair": "simone2",
                              "price": "0.015"}}
    PROVIDER = "AZURE"                      #"""Provider to be used"""
    AZ_KEY_NAME = "id_rsa"                  #""" name of Azure private key """
    AZ_PUB_KEY_PATH = "id_rsa.pub"          #""" name of Azure public key """
    AZ_PRV_KEY_PATH = ""                    #""" path of Azure private key """
    AWS_ACCESS_ID = ""                      #""" Azure access id """
    AWS_SECRET_KEY = ""                     #""" Azure secret key """
    AZ_APPLICATION_ID = ""                  #""" Azure application id """
    AZ_SECRET = ""                          #""" Azure secret """
    AZ_SUBSCRIPTION_ID = ""                 #""" Azure subscription id """
    AZ_TENANT_ID = ""                       #""" Azure tenant id """
    KEY_PAIR_PATH = "" + DATA_AMI[REGION]["keypair"] + ".pem"       #""" AWS keypair path """
    AZ_LOCATION = "westeurope"              #"""AZURE Datacenter Location"""
    AZ_SIZE = "Standard_D14_v2_Promo"       #"""AZURE VM Size"""                         
    AZ_IMAGE = "Canonical:UbuntuServer:14.04.5-LTS:14.04.201703230" #"""AZURE VM Image"""
    AZ_VHD_IMAGE = {"StorageAccount": "sparkdisks",                 #"""AZURE VHD Image"""
                    "BlobContainer": "vhd",
                    "Name": "vm-os.vhd"}
    AZ_RESOURCE_GROUP = "spark"             #"""AZURE Resource Group"""
    AZ_STORAGE_ACCOUNT = "sparkdisks"       #"""AZURE Storage Group"""
    AZ_SA_SKU = "standard_lrs"              #"""AZURE Storage SKU"""
    AZ_SA_KIND = "storage"                  #"""AZURE Storage Kind"""               
    AZ_NETWORK = "cspark-vnet2"             #"""AZURE Virtual Network"""
    AZ_SUBNET = "default"                   #"""AZURE Subnet"""
    AZ_SECURITY_GROUP = "cspark-securitygroup2" #"""AZURE Security Group"""
    CREDENTIAL_PROFILE = "cspark"           #"""Credential profile name of AWS"""
    SECURITY_GROUP = "spark-cluster"        #"""Security group of the instance"""
    PRICE = DATA_AMI[REGION]["price"]
    INSTANCE_TYPE = "m3.medium"             #"""Instance type"""
    NUM_INSTANCE = 0                        #"""Number of instance to use"""
    EBS_OPTIMIZED = True if "r3" not in INSTANCE_TYPE else False
    REBOOT = False                              #"""Reboot the instances of the cluster"""
    KILL_JAVA = True                           #"""Kill every java application on the cluster"""
    NUM_RUN = 1                             #"""Number of run to repeat the benchmark"""                          
    PROCESS_ON_SERVER = False               #"""Download benchmark logs and generate profiles and plots on server """
    INSTALL_PYTHON3 = True                  #"""Install Python3 on cspark master"""
    CLUSTER_ID = "CSPARKWORK"               #"""Id of the cluster with the launched instances"""
    TAG = [{"Key": "ClusterId", 
            "Value": CLUSTER_ID}]           
    HDFS_MASTER = "10.0.0.4"                # use private ip for azure!              
    SPARK_SEQ_HOME = "/opt/spark-seq/"      # "sequential" Spark home directory                     
    SPARK_2_HOME = "/opt/spark/"            # regular Spark home directory
    C_SPARK_HOME = "/usr/local/spark/"      # "controlled" spark home directory
    SPARK_HOME = C_SPARK_HOME               #Location of Spark in the ami"""
    LOG_LEVEL = "INFO"
    UPDATE_SPARK = False                    #"""Git pull and build Spark of all the cluster"""
    UPDATE_SPARK_MASTER = True              #"""Git pull and build Spark only of the master node"""
    UPDATE_SPARK_DOCKER = False             #"""Pull the docker image in each node of the cluster"""
    ENABLE_EXTERNAL_SHUFFLE = "true"
    LOCALITY_WAIT = 0
    LOCALITY_WAIT_NODE = 0
    LOCALITY_WAIT_PROCESS = 1
    LOCALITY_WAIT_RACK = 0
    CPU_TASK = 1
    RAM_DRIVER = "100g"
    RAM_EXEC = '"100g"' if "r3" not in INSTANCE_TYPE else '"100g"'
    OFF_HEAP = False
    if OFF_HEAP:
        RAM_EXEC = '"30g"' if "r3" not in INSTANCE_TYPE else '"70g"'
    OFF_HEAP_BYTES = 30720000000
    CORE_VM = 10                            # max 16
    CORE_HT_VM = 10                         # max 16
    DISABLE_HT = False
    if DISABLE_HT:
        CORE_HT_VM = CORE_VM
    ALPHA = 0.95
    BETA = 0.33
    DEADLINE = 37600
    MAX_EXECUTOR = 8
    OVER_SCALE = 2
    K = 50
    TI = 12000
    T_SAMPLE = 1000
    CORE_QUANTUM = 0.05
    CORE_MIN = 0.0
    CPU_PERIOD = 100000    
    
    # BENCHMARK
    RUN = True
    SYNC_TIME = 1
    PREV_SCALE_FACTOR = 0                   #"""*Important Settings* if it is equals to SCALE_FACTOR no need to generate new data on HDFS"""
    BENCH_NUM_TRIALS = 1 
    BENCHMARK_PERF = [                      #"""Spark-perf benchmark to execute"""
            # "scala-agg-by-key",
            # "scala-agg-by-key-int",
            # "scala-agg-by-key-naive",
            # "scala-sort-by-key"#,
            # "scala-sort-by-key-int",
            # "scala-count",
            # "scala-count-w-fltr",
        ]
    BENCHMARK_BENCH = [                     #"""Spark-bench benchmark to execute"""
             "PageRank"#,
            # "DecisionTree",
            # "KMeans",
            # "SVM"
        ]
    if len(BENCHMARK_PERF) + len(BENCHMARK_BENCH) > 1 or len(BENCHMARK_PERF) + len(
            BENCHMARK_BENCH) == 0:
        print("ERROR BENCHMARK SELECTION")
        exit(1)
    # config: (line, value)
    BENCH_CONF = {                          #"""Setting of the supported benchmark"""
        "scala-agg-by-key": {
            "ScaleFactor": 5
        },
        "scala-agg-by-key-int": {
            "ScaleFactor": 5
        },
        "scala-agg-by-key-naive": {
            "ScaleFactor": 5
        },
        "scala-sort-by-key": {
            "ScaleFactor": 13,
    #        "skew": 0,
            "num-partitions": 100,
            "unique-keys": 100,
            "reduce-tasks": 100
        },
        "scala-sort-by-key-int": {
            "ScaleFactor": 25
        },
        "scala-count": {
            "ScaleFactor": 25
        },
        "scala-count-w-fltr": {
            "ScaleFactor": 25
        },
        "PageRank": {
            "NUM_OF_PARTITIONS": (3, 1000),
            # "NUM_OF_PARTITIONS": (3, 10),
            "numV": (2, 3000000),  # 7000000
            # "numV": (2, 50000),
            "mu": (4, 3.0),
            "sigma": (5, 0.0),
            "MAX_ITERATION": (8, 1),
            "NumTrials": 1
        },
        "KMeans": {
            # DataGen
            "NUM_OF_POINTS": (2, 100000000),
            "NUM_OF_CLUSTERS": (3, 10),
            "DIMENSIONS": (4, 10),
            "SCALING": (5, 0.6),
            "NUM_OF_PARTITIONS": (6, 1000),
            # Run
            "MAX_ITERATION": (8, 1)
        },
        "DecisionTree": {
            "NUM_OF_PARTITIONS": (4, 1000),
            "NUM_OF_EXAMPLES": (2, 50000000),
            "NUM_OF_FEATURES": (3, 6),
            "NUM_OF_CLASS_C": (7, 10),
            "MAX_ITERATION": (21, 1)
        },
        "SVM": {
            "NUM_OF_PARTITIONS": (4, 1000),
            "NUM_OF_EXAMPLES": (2, 100000000),
            "NUM_OF_FEATURES": (3, 5),
            "MAX_ITERATION": (7, 1)
        }
    }
    if len(BENCHMARK_PERF) > 0:
        SCALE_FACTOR = BENCH_CONF[BENCHMARK_PERF[0]]["ScaleFactor"]
        INPUT_RECORD = 200 * 1000 * 1000 * SCALE_FACTOR
        NUM_TASK = SCALE_FACTOR
    else:
        SCALE_FACTOR = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_PARTITIONS"][1]
        NUM_TASK = SCALE_FACTOR
        try:
            INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_EXAMPLES"][1]
        except KeyError:
            try:
                INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_POINTS"][1]
            except KeyError:
                INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["numV"][1]
    BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]][
        "NumTrials"] = BENCH_NUM_TRIALS

    TERMINATE = False                       # Terminate instance after benchmark
    # HDFS
    HDFS = 1 if HDFS_MASTER == "" else 0    # TODO Fix this variable for plot
    HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"
    HADOOP_HOME = "/usr/local/lib/hadoop-2.7.2"
    DELETE_HDFS = 1 if SCALE_FACTOR != PREV_SCALE_FACTOR else 0
    
    # HEURISTICS
    HEURISTIC = Heuristic.CONTROL_UNLIMITED
    
    # KMEANS
    STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    CORE_ALLOCATION = [14.6552, 1.3668, 4.2932, 3.2259, 5.9839, 3.1770, 5.2449, 2.5064, 6.5889, 2.6935, 5.9204, 2.8042,
                       9.6728, 1.7509, 3.8915, 0.7313, 12.2620, 3.1288]
    DEADLINE_ALLOCATION = [18584, 1733, 5444, 4090, 7588, 4028, 6651, 3178, 8355, 3415, 7507, 3556, 12266, 2220, 4934, 927,
                           15549, 3967]
    
    # SVM
    # STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23]
    # CORE_ALLOCATION = [0.3523, 7.9260, 0.6877, 1.3210, 0.5275, 4.1795, 7.8807, 6.6329, 2.2927, 2.9942, 3.2869, 3.2016,
    #                    0.8377]
    # DEADLINE_ALLOCATION = [953, 21451, 1861, 3575, 1427, 11312, 21329, 17952, 6205, 8103, 8896, 8665, 2267]
    
    # AGG BY KEY
    # STAGE_ALLOCATION = [0, 1]
    # CORE_ALLOCATION = [6.3715 , 2.2592]
    # DEADLINE_ALLOCATION = [84158, 29841]
    
    # STAGE_ALLOCATION = None
    # CORE_ALLOCATION = None
    # DEADLINE_ALLOCATION = None

    CONFIG_DICT = {
        "Provider": PROVIDER,
        "Benchmark": {
            "Name": BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0],
            "Config": BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]]
        },
        "Deadline": DEADLINE,
        "Control": {
            "Alpha": ALPHA,
            "Beta": BETA,
            "OverScale": OVER_SCALE,
            "MaxExecutor": MAX_EXECUTOR,
            "CoreVM": CORE_VM,
            "K": K,
            "Ti": TI,
            "TSample": T_SAMPLE,
            "CoreQuantum": CORE_QUANTUM,
            "Heuristic": HEURISTIC.name,
            "CoreAllocation": CORE_ALLOCATION,
            "DeadlineAllocation": DEADLINE_ALLOCATION,
            "StageAllocation": STAGE_ALLOCATION
        },
        "Aws": {
            "InstanceType": INSTANCE_TYPE,
            "HyperThreading": not DISABLE_HT,
            "Price": PRICE,
            "AMI": DATA_AMI[REGION]["ami"],
            "Region": REGION,
            "AZ": DATA_AMI[REGION]["az"],
            "SecurityGroup": SECURITY_GROUP,
            "KeyPair": DATA_AMI[REGION]["keypair"],
            "EbsOptimized": EBS_OPTIMIZED,
            "SnapshotId": DATA_AMI[REGION]["snapid"]
        },
        "Azure": {
            "NodeSize": AZ_SIZE,
            "NodeImage": AZ_VHD_IMAGE,
            "Location": AZ_LOCATION,
            "PubKeyPath": AZ_PUB_KEY_PATH,
            "ClusterId": CLUSTER_ID,
            "ResourceGroup": AZ_RESOURCE_GROUP,
            "StorageAccount": {"Name": AZ_STORAGE_ACCOUNT,
                               "Sku": AZ_SA_SKU,
                               "Kind": AZ_SA_KIND},
            "Network": AZ_NETWORK,
            "Subnet": AZ_SUBNET,
            "SecurityGroup": AZ_SECURITY_GROUP
        },
        "Spark": {
            "ExecutorCore": CORE_VM,
            "ExecutorMemory": RAM_EXEC,
            "ExternalShuffle": ENABLE_EXTERNAL_SHUFFLE,
            "LocalityWait": LOCALITY_WAIT,
            "LocalityWaitProcess": LOCALITY_WAIT_PROCESS,
            "LocalityWaitNode": LOCALITY_WAIT_NODE,
            "LocalityWaitRack": LOCALITY_WAIT_RACK,
            "CPUTask": CPU_TASK,
            "SparkHome": SPARK_HOME
        },
        "HDFS": bool(HDFS),
    }
    # Line needed for enabling/disabling benchmark in spark-perf config.py
    BENCH_LINES = {"scala-agg-by-key": ["225", "226"],
                   "scala-agg-by-key-int": ["229", "230"],
                   "scala-agg-by-key-naive": ["232", "233"],
                   "scala-sort-by-key": ["236", "237"],
                   "scala-sort-by-key-int": ["239", "240"],
                   "scala-count": ["242", "243"],
                   "scala-count-w-fltr": ["245", "246"]}
    PRIVATE_KEY_PATH = KEY_PAIR_PATH if PROVIDER == "AWS_SPOT" \
    else AZ_PRV_KEY_PATH if PROVIDER == "AZURE" \
    else None
    PRIVATE_KEY_NAME = DATA_AMI[REGION]["keypair"] + ".pem" if PROVIDER == "AWS_SPOT" \
        else AZ_KEY_NAME if PROVIDER == "AZURE" \
        else None
    TEMPORARY_STORAGE = "/dev/xvdb" if PROVIDER == "AWS_SPOT" \
        else "/dev/sdb1" if PROVIDER == "AZURE" \
        else None
    UPDATE_SPARK_BENCH = True
    UPDATE_SPARK_PERF = False
    SPARK_PERF_FOLDER = "spark-perf"
    CLUSTER_MAP = {
        'hdfs': 'CSPARKHDFS',
        'spark': 'CSPARKWORK',
        'generic': 'ZOT'
    }
    VAR_PAR_MAP = {
        'pagerank': {
            'var_name': 'num_v',
            'default': (2, 10000000),
            'profile_name': 'Spark_PageRank_Application'
        },
        'kmeans': {
            'var_name': 'num_of_points',
            'default': (2, 10000000),
            'profile_name': 'Spark_KMeans_Example'
        },
        'sort_by_key': {
            'var_name': 'scale_factor',
            'default': (0, 15),
            'profile_name': 'TestRunner__sort-by-key'
        },
        'SVM': {
            'var_name': 'scale_factor',
            'default': (2, 10000000),
            'profile_name': 'SVM_Classifier_Example'
        },
        "scala-agg-by-key": {
            'profile_name': ''
        },
        "scala-agg-by-key-int": {
            'profile_name': ''
        },
        "scala-agg-by-key-naive": {
            'profile_name': ''
        },
        "scala-sort-by-key": {
            'var_name': 'scale_factor',
            'default': (0, 15),
            'profile_name': 'TestRunner__sort-by-key'
        },
        "scala-sort-by-key-int": {
            'profile_name': ''
        },
        "scala-count": {
            'profile_name': ''
        },
        "scala-count-w-fltr": {
            'profile_name': ''
        },
        "PageRank": {
            'var_name': 'num_v',
            'default': (2, 10000000),
            'profile_name': 'Spark_PageRank_Application'
        },
        "KMeans": {
            'var_name': 'num_of_points',
            'default': (2, 10000000),
            'profile_name': 'Spark_KMeans_Example'
        },
        "DecisionTree": {
            'profile_name': ''
        }  
    }
    #SCALE_FACTOR = "" 
    #NUM_TASK = ""
    #INPUT_RECORD = "" 
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CLUSTERS_CFG_FILENAME = 'cfg_clusters.ini'
    CLUSTERS_CFG_PATH = os.path.join(ROOT_DIR, CLUSTERS_CFG_FILENAME)
    
    cfg_dict = {    "AzKeyName": c.AZ_KEY_NAME,
                    "AzPubKeyPath": c.AZ_PUB_KEY_PATH,
                    "AzPrvKeyPath": c.AZ_PRV_KEY_PATH,
                    "AwsAccessId": c.AWS_ACCESS_ID, 
                    "AwsSecretKey": c.AWS_SECRET_KEY,
                    "AzApplicationId": c.AZ_APPLICATION_ID,
                    "AzSecret": c.AZ_SECRET,
                    "AzSubscriptionId": c.AZ_SUBSCRIPTION_ID,
                    "AzTenantId": c.AZ_TENANT_ID,
                    "KeyPairPath": c.KEY_PAIR_PATH,
                    "AzLocation": c.AZ_LOCATION,
                    "AzSize": c.AZ_SIZE,
                    "AzImage": c.AZ_IMAGE,
                    "AzVhdImage": c.AZ_VHD_IMAGE,
                    "AzResourceGroup": c.AZ_RESOURCE_GROUP,
                    "AzStorageAccount": c.AZ_STORAGE_ACCOUNT,
                    "AzSaSku": c.AZ_SA_SKU,
                    "AzSaKind": c.AZ_SA_KIND,
                    "AzNetwork": c.AZ_NETWORK,
                    "AzSubnet": c.AZ_SUBNET,
                    "AzSecurityGroup": c.AZ_SECURITY_GROUP,
                    "DataAmi": c.DATA_AMI,
                    "CredentialProfile": c.CREDENTIAL_PROFILE,
                    "Region": c.REGION,
                    "SecurityGroup": c.SECURITY_GROUP,
                    "Price": c.PRICE,
                    "InstanceType": c.INSTANCE_TYPE,
                    "NumInstance": c.NUM_INSTANCE,
                    "EbsOptimized": c.EBS_OPTIMIZED,
                    "Reboot": c.REBOOT,
                    "KillJava": c.KILL_JAVA,
                    "NmRun": c.NUM_RUN,
                    "ProcessOnServer": c.PROCESS_ON_SERVER,
                    "InstallPython3": c.INSTALL_PYTHON3,
                    "ClusterId": c.CLUSTER_ID,
                    "Tag": c.TAG,
                    "HdfsMaster": c.HDFS_MASTER,
                    "SparkSeqHome": c.SPARK_SEQ_HOME,
                    "Spark2Home": c.SPARK_2_HOME,
                    "CSparkHome": c.C_SPARK_HOME,
                    "SparkHome": c.SPARK_HOME,
                    "LogLevel": c.LOG_LEVEL,
                    "UpdateSpark": c.UPDATE_SPARK,
                    "UpdateSparkMaster": c.UPDATE_SPARK_MASTER,
                    "UpdateSparkDocker": c.UPDATE_SPARK_DOCKER,
                    "EnableExternalShuffle": c.ENABLE_EXTERNAL_SHUFFLE,
                    "LocalityWait": c.LOCALITY_WAIT,
                    "LocalityWaitNode": c.LOCALITY_WAIT_NODE,
                    "LocalityWaitProcess": c.LOCALITY_WAIT_PROCESS,
                    "LocalityWaitRack": c.LOCALITY_WAIT_RACK,
                    "CpuTask": c.CPU_TASK,
                    "RamDriver": c.RAM_DRIVER,
                    "RamExec": c.RAM_EXEC,
                    "OffHeap": c.OFF_HEAP,
                    "OffHeapbytes": c.OFF_HEAP_BYTES,
                    "CoreVM": c.CORE_VM,
                    "CoreHTVM": c.CORE_HT_VM,
                    "DisableHT": c.DISABLE_HT,
                    "Alpha": c.ALPHA,
                    "Beta": c.BETA,
                    "Deadline": c.DEADLINE,
                    "MaxExecutor": c.MAX_EXECUTOR,
                    "OverScale": c.OVER_SCALE,
                    "K": c.K,
                    "Ti": c.TI,
                    "TSample": c.T_SAMPLE,
                    "CoreQuantum": c.CORE_QUANTUM,
                    "CoreMin": c.CORE_MIN,
                    "CpuPeriod": c.CPU_PERIOD,    
                    "Run": c.RUN,
                    "SyncTime": c.SYNC_TIME,
                    "PrevScaleFactor": c.PREV_SCALE_FACTOR,
                    "BenchNumTrials": c.BENCH_NUM_TRIALS, 
                    "BenchmarkPerf": c.BENCHMARK_PERF,
                    "BenchmarkBench": c.BENCHMARK_BENCH,
                    "BenchConf": c.BENCH_CONF,
                    "Terminate": c.TERMINATE,
                    "Hdfs": c.HDFS,
                    "HadoopConf": c.HADOOP_CONF,
                    "HadoopHome": c.HADOOP_HOME,
                    "DeleteHdfs": c.DELETE_HDFS,
                    "Heuristic": c.HEURISTIC.name,
                    "StageAllocation": c.STAGE_ALLOCATION,
                    "CoreAllocation": c.CORE_ALLOCATION,
                    "DeadlineAllocation": c.DEADLINE_ALLOCATION,
                    "ConfigDict": c.CONFIG_DICT,
                    "BenchLines": c.BENCH_LINES,
                    "PrivateKeyPath": c.PRIVATE_KEY_PATH,
                    "PrivateKeyName": c.PRIVATE_KEY_NAME,
                    "TemporaryStorage": c.TEMPORARY_STORAGE,
                    "UpdateSparkBench": c.UPDATE_SPARK_BENCH,
                    "UpdateSparkPerf": c.UPDATE_SPARK_PERF,
                    "SparkPerfFolder": c.SPARK_PERF_FOLDER,
                    "ClusterMap": c.CLUSTER_MAP,
                    "VarParMap": c.VAR_PAR_MAP,
                    "ScaleFactor": c.SCALE_FACTOR, 
                    "NumTask": c.NUM_TASK,
                    "NumRecord": c.INPUT_RECORD, 
                    "RootDir": c.ROOT_DIR, 
                    "ClustersCfgFilename": c.CLUSTERS_CFG_FILENAME, 
                    "ClustersCfgPath": c.CLUSTERS_CFG_PATH
                    }
    
    def __init__(self):
        self.config_credentials("credentials.json")
        self.config_setup("setup.json")
        self.config_control("control.json")
        self.update_config_parms(self)
        print("Config instance initialized.")

    def config_experiment(self, filepath, cfg):
        exp_file = Path(filepath)
        if exp_file.exists():
            experiment = json.load(open(filepath))
            if len(experiment) > 0:
                cfg["experiment"] = {}
                keys = experiment.keys()
                benchmark = ""
                is_benchmark_bench = False
                is_benchmark_perf = False
                if "ReuseDataset" in keys:
                    self.cfg_dict["DeleteHdfs"] = c.DELETE_HDFS = not experiment["ReuseDataset"]
                    self.cfg_dict["PrevScaleFactor"] = c.PREV_SCALE_FACTOR = c.NUM_TASK if experiment["ReuseDataset"] else 0
                    cfg["experiment"]["reusedataset"] = str(experiment["ReuseDataset"])
                if "Deadline" in keys:
                    self.cfg_dict["Deadline"] = c.DEADLINE = experiment["Deadline"]
                    cfg["experiment"]["deadline"] = str(experiment["Deadline"])
                if "BenchmarkConf" in keys and "BenchmarkName" in keys:
                    cfg["BenchmarkConf"] = {}
                    benchmark = experiment["BenchmarkName"]
                    cfg["experiment"]["benchmarkname"] = experiment["BenchmarkName"]
                    is_benchmark_bench = benchmark in ["PageRank", "KMeans", "DecisionTree", "SVM"]
                    is_benchmark_perf = benchmark in ["scala-agg-by-key", "scala-agg-by-key-int", 
                                                              "scala-agg-by-key-naive", "scala-sort-by-key", 
                                                              "scala-sort-by-key-int", "scala-count", 
                                                              "scala-count-w-fltr"]
                    for k_bench_conf in experiment["BenchmarkConf"].keys():
                        mapped_parm = self.exp_par_map(k_bench_conf)
                        if is_benchmark_bench and k_bench_conf != "NumTrials":
                            self.cfg_dict["BenchConf"][benchmark][mapped_parm] = c.BENCH_CONF[benchmark][mapped_parm] = (c.BENCH_CONF[benchmark][mapped_parm][0], experiment["BenchmarkConf"][k_bench_conf])
                        elif is_benchmark_perf or is_benchmark_bench and k_bench_conf == "NumTrials":
                            self.cfg_dict["BenchConf"][benchmark][mapped_parm] = c.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][k_bench_conf]
                        #if is_benchmark_bench and k_bench_conf != "NumTrials":
                        #    BENCH_CONF[benchmark][k_bench_conf] = (BENCH_CONF[benchmark][k_bench_conf][0], experiment["BenchmarkConf"][k_bench_conf])
                        #elif is_benchmark_perf or is_benchmark_bench and k_bench_conf == "NumTrials":
                        #    BENCH_CONF[benchmark][k_bench_conf] = experiment["BenchmarkConf"][k_bench_conf]
                    #BENCH_CONF[benchmark] = experiment["BenchmarkConf"]
                    keys = c.BENCH_CONF[benchmark].keys()
                    for key in keys:
                        inv_mapped_parm = self.exp_inverse_par_map(key)
                        cfg['BenchmarkConf'][inv_mapped_parm] = str(c.BENCH_CONF[benchmark][key][1]) \
                            if key != "NumTrials" else str(c.BENCH_CONF[benchmark][key])
                    self.cfg_dict["BenchmarkBench"] = c.BENCHMARK_BENCH = [benchmark] if is_benchmark_bench else []
                    self.cfg_dict["BenchmarkPerf"] = c.BENCHMARK_PERF = [benchmark] if is_benchmark_perf else []
                #if "SyncTime" in keys:
                #    SYNC_TIME = experiment["SyncTime"]
                #if "PrevScaleFactor" in keys:
                #    PREV_SCALE_FACTOR = experiment["PrevScaleFactor"]
                #if "BenchNumTrials" in keys:
                #    BENCH_NUM_TRIALS = experiment["BenchNumTrials"]            
                #if "BenchmarkPerf" in keys:
                #    BENCHMARK_PERF = experiment["BenchmarkPerf"]
                #if "BenchmarkBench" in keys:
                #    BENCHMARK_BENCH = experiment["BenchmarkBench"]
                
                #if "BenchmarkConf" in keys:
                #    BENCH_CONF = experiment["BenchmarkConf"]  else BENCH_CONF
                #    k_BenchmarkConf = experiment["BenchmarkConf"].keys()
                #    print("config_experiment: " + str(BENCHMARK_BENCH + BENCHMARK_PERF))
                #    for bench in BENCHMARK_BENCH + BENCHMARK_PERF:
                #        if bench in k_BenchmarkConf:
                #            BENCH_CONF[bench] = experiment["BenchmarkConf"][bench]
                                                              
                self.update_config_parms(self)
                
            else: 
                print("Experiment file: "+ filepath + " is empty: using defaults")
        else: 
            print("Experiment file: "+ filepath + " not found: exiting program")
            exit(-1)
    
    def update_exp_parms(self, cfg):
        benchmark = ""
        if "experiment" in cfg:
            if "reusedataset" in cfg["experiment"]:
                self.cfg_dict["DeleteHdfs"] = c.DELETE_HDFS = not cfg.getboolean('experiment', 'reusedataset')
                self.cfg_dict["PrevScaleFactor"] = c.PREV_SCALE_FACTOR = c.NUM_TASK if cfg.getboolean('experiment', 'reusedataset') else 0
            if "deadline" in cfg["experiment"]:
                self.cfg_dict["Deadline"] = c.DEADLINE = cfg["experiment"]["deadline"]
        if "BenchmarkConf" in cfg:
            if "benchmarkname" in cfg:
                benchmark = cfg["benchmarkname"]
                is_benchmark_bench = benchmark in ["PageRank", "KMeans", "DecisionTree", "SVM"]
                is_benchmark_perf = benchmark in ["scala-agg-by-key", "scala-agg-by-key-int", 
                                                          "scala-agg-by-key-naive", "scala-sort-by-key", 
                                                          "scala-sort-by-key-int", "scala-count", 
                                                          "scala-count-w-fltr"]
                for p_bench_conf in cfg["BenchmarkConf"]:
                    mapped_parm = self.exp_lowercase_to_camelcase_par_map(p_bench_conf)
                    if is_benchmark_bench and p_bench_conf != "NumTrials":
                        self.cfg_dict["BenchConf"][benchmark][mapped_parm] = c.BENCH_CONF[benchmark][mapped_parm] = (c.BENCH_CONF[benchmark][mapped_parm][0], experiment["BenchmarkConf"][p_bench_conf])
                    elif is_benchmark_perf or is_benchmark_bench and p_bench_conf == "NumTrials":
                        self.cfg_dict["BenchConf"][benchmark][mapped_parm] = c.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][p_bench_conf]
                self.cfg_dict["BenchmarkBench"] = c.BENCHMARK_BENCH = [benchmark] if is_benchmark_bench else []
                self.cfg_dict["BenchmarkPerf"] = c.BENCHMARK_PERF = [benchmark] if is_benchmark_perf else []
        
        
        self.update_config_parms(self)
    
    def config_credentials(self, filepath):
        credentials_file = Path(filepath)
        if credentials_file.exists():
            credentials = json.load(open("credentials.json"))
            if len(credentials) > 0:
                keys = credentials.keys()
                if "AzTenantId" in keys: 
                    self.cfg_dict["AzTenantId"] = c.AZ_TENANT_ID = credentials["AzTenantId"]
                if "AzSubscriptionId" in keys:
                    self.cfg_dict["AzSubscriptionId"] = c.AZ_SUBSCRIPTION_ID = credentials["AzSubscriptionId"]
                if "AzApplicationId" in keys:
                    self.cfg_dict["AzApplicationId"] = c.AZ_APPLICATION_ID = credentials["AzApplicationId"]
                if "AzSecret" in keys:
                    self.cfg_dict["AzSecret"] = c.AZ_SECRET = credentials["AzSecret"]
                if "PubKeyPath" in keys:
                    self.cfg_dict["PubKeyPath"] = c.AZ_PUB_KEY_PATH = credentials["PubKeyPath"]
                if "PrvKeyPath" in keys:
                    self.cfg_dict["PrvKeyPath"] = c.AZ_PRV_KEY_PATH = credentials["PrvKeyPath"] 
                if "AwsAccessId" in keys:
                    self.cfg_dict["AwsAccessId"] = c.AWS_ACCESS_ID = credentials["AwsAccessId"]
                if "AwsSecretId" in keys:
                    self.cfg_dict["AwsSecretId"] = c.AWS_SECRET_KEY = credentials["AwsSecretId"]
                if "KeyPairPath" in keys:
                    self.cfg_dict["KeyPairPath"] = c.KEY_PAIR_PATH = credentials["KeyPairPath"]
                print("Configuration from " + filepath + " done")                    
            else: 
                print("credentials file: " + filepath + "  is empty: using defaults")
        else: 
            print("credentials file:  " + filepath + "  found: using defaults")
                    
    def config_setup(self, filepath):
        setup_file = Path(filepath)
        if setup_file.exists():
            setup = json.load(open("setup.json"))
            if len(setup) > 0:
                keys = setup.keys()
                if "Provider" in keys:
                    self.cfg_dict["Provider"] = c.PROVIDER = setup["Provider"]
                if "ProcessOnServer" in keys:
                    #print("PROCESS_ON_SERVER b4: "+ str(PROCESS_ON_SERVER))
                    self.cfg_dict["ProcessOnServer"] = c.PROCESS_ON_SERVER = setup["ProcessOnServer"]
                    print("PROCESS_ON_SERVER aft: "+ str(c.PROCESS_ON_SERVER))
                if "InstallPython3" in keys:
                    self.cfg_dict["InstallPython3"] = c.INSTALL_PYTHON3 = setup["InstallPython3"]
                #if "NumInstance" in keys: NUM_INSTANCE = setup["NumInstance"]
                #if "Reboot" in keys: REBOOT = setup["Reboot"]
                #if "KillJava" in keys: KILL_JAVA = setup["KillJava"]
                #if "NumRun" in keys: NUM_RUN = setup["NumRun"]
                #if "ClusterId" in keys: CLUSTER_ID = setup["ClusterId"]
                #if "Tag" in keys: TAG = setup["Tag"]
                #if "HdfsMaster" in keys: HDFS_MASTER = setup["HdfsMaster"]
                #if "Terminate" in keys: TERMINATE = setup["Terminate"]
                #if "HadoopConf" in keys: HADOOP_CONF = setup["HadoopConf"]
                #if "HadoopHome" in keys: HADOOP_HOME = setup["HadoopHome"]
                #if "UpdateSparkBench" in keys: UPDATE_SPARK_BENCH = setup["UpdateSparkBench"]
                #if "UpdateSparkPerf" in keys: UPDATE_SPARK_PERF = setup["UpdateSparkPerf"]
                #if "SparkPerfFolder" in keys: SPARK_PERF_FOLDER = setup["SparkPerfFolder"]
                #if "ClusterMap" in keys: CLUSTER_MAP = setup["ClusterMap"]
                #if "VarParMap" in keys: VAR_PAR_MAP = setup["VarParMap"]
                #if "BenchLines" in keys: BENCH_LINES = setup["BenchLines"]
                #if "HDFS" in keys: HDFS = setup["HDFS"]
                if "Credentials" in keys:
                    k_Credentials = setup["Credentials"].keys()
                    if "AzTenantId" in k_Credentials: 
                        self.cfg_dict["AzTenantId"] = c.AZ_TENANT_ID = setup["Credentials"]["AzTenantId"]
                    if "AzSubscriptionId" in k_Credentials:
                        self.cfg_dict["AzSubscriptionId"] = c.AZ_SUBSCRIPTION_ID = setup["Credentials"]["AzSubscriptionId"]
                    if "AzApplicationId" in k_Credentials:
                        self.cfg_dict["AzApplicationId"] = c.AZ_APPLICATION_ID = setup["Credentials"]["AzApplicationId"]
                    if "AzSecret" in k_Credentials:
                        self.cfg_dict["AzSecret"] = c.AZ_SECRET = setup["Credentials"]["AzSecret"]
                    if "PubKeyPath" in k_Credentials:
                        self.cfg_dict["PubKeyPath"] = c.AZ_PUB_KEY_PATH = setup["Credentials"]["PubKeyPath"]
                    if "PrvKeyPath" in k_Credentials:
                        self.cfg_dict["PrvKeyPath"] = c.AZ_PRV_KEY_PATH = setup["Credentials"]["PrvKeyPath"] 
                    if "AwsAccessId" in k_Credentials:
                        self.cfg_dict["AwsAccessId"] = c.AWS_ACCESS_ID = setup["Credentials"]["AwsAccessId"]
                    if "AwsSecretId" in k_Credentials:
                        self.cfg_dict["AwsSecretId"] = c.AWS_SECRET_KEY = setup["Credentials"]["AwsSecretId"]
                    if "KeyPairPath" in k_Credentials:
                        self.cfg_dict["KeyPairPath"] = c.KEY_PAIR_PATH = setup["Credentials"]["KeyPairPath"]
                
                if "VM" in keys:
                    k_VM = setup["VM"].keys()
                    if "Core" in k_VM:
                        self.cfg_dict["CoreVM"] = c.CORE_VM = setup["VM"]["Core"]
                    if "Memory" in k_VM:
                        self.cfg_dict["RamExec"] = c.RAM_EXEC = '"' + setup["VM"]["Memory"] + '"'
                
                if "Azure" in keys:
                    k_Azure = setup["Azure"].keys()
                    if "ResourceGroup" in k_Azure:
                        self.cfg_dict["AzResourceGroup"] = c.AZ_RESOURCE_GROUP = setup["Azure"]["ResourceGroup"]
                    if "SecurityGroup" in k_Azure:
                        self.cfg_dict["AzSecurityGroup"] = c.AZ_SECURITY_GROUP = setup["Azure"]["SecurityGroup"]
                    if "StorageAccount" in k_Azure:
                        k_Azure_StorageAccount = setup["Azure"]["StorageAccount"].keys()
                        if "Name" in k_Azure_StorageAccount:
                            self.cfg_dict["AzStorageAccount"] = c.AZ_STORAGE_ACCOUNT = setup["Azure"]["StorageAccount"]["Name"]
                        if "Sku" in k_Azure_StorageAccount:
                            self.cfg_dict["AzSaSku"] = c.AZ_SA_SKU = setup["Azure"]["StorageAccount"]["Sku"]
                        if "Kind" in k_Azure_StorageAccount:
                            self.cfg_dict["AzSaKind"] = c.AZ_SA_KIND = setup["Azure"]["StorageAccount"]["Kind"]
                    if "Subnet" in k_Azure:
                        self.cfg_dict["AzSubnet"] = c.AZ_SUBNET = setup["Azure"]["Subnet"]
                    if "NodeSize" in k_Azure:
                        self.cfg_dict["AzSize"] = c.AZ_SIZE = setup["Azure"]["NodeSize"]
                    if "Network" in k_Azure:
                        self.cfg_dict["AzNetwork"] = c.AZ_NETWORK = setup["Azure"]["Network"]
                    if "Location" in k_Azure:
                        self.cfg_dict["AzLocation"] = c.AZ_LOCATION = setup["Azure"]["Location"]
                    if "ImageName" in k_Azure:
                        self.cfg_dict["AzImageName"] = c.AZ_IMAGE = setup["Azure"]["ImageName"] 
                    if "NodeImage" in k_Azure:
                        self.cfg_dict["AzVhdImage"] = c.AZ_VHD_IMAGE = setup["Azure"]["NodeImage"]
                    #if "AzKeyName" in k_Azure: AZ_KEY_NAME = setup["Azure"]["AzKeyName"] #not needed: keyname extracted from credentials
                    
                if "Aws" in keys:
                    k_Aws = setup["Aws"].keys()
                    if "Region" in k_Aws:
                        self.cfg_dict["Region"] = c.REGION = setup["Aws"]["Region"]
                    if "SecurityGroup" in k_Aws:
                        self.cfg_dict["SecurityGroup"] = c.SECURITY_GROUP = setup["Aws"]["SecurityGroup"]
                    if "Price" in k_Aws:
                        self.cfg_dict["Price"] = c.PRICE = setup["Aws"]["Price"]
                    if "InstanceType" in k_Aws:
                        self.cfg_dict["InstanceType"] = c.INSTANCE_TYPE = setup["Aws"]["InstanceType"]
                    if "EbsOptimized" in k_Aws:
                        self.cfg_dict["EbsOptimized"] = c.EBS_OPTIMIZED = setup["Aws"]["EbsOptimized"]
                    #if "AMI" in k_Aws: DATA_AMI = setup["Aws"]["AMI"]
                    #if "CredentialProfile" in k_Aws: CREDENTIAL_PROFILE = setup["Aws"]["CredentialProfile"]
                    
                if "Spark" in keys:
                    k_Spark = setup["Spark"].keys()
                    if "Home" in keys:
                        self.cfg_dict["Spark2Home"] = c.SPARK_2_HOME = setup["Spark"]["Home"]
                    if "ExternalShuffle" in keys:
                        self.cfg_dict["EnableExternalShuffle"] = c.ENABLE_EXTERNAL_SHUFFLE = setup["Spark"]["ExternalShuffle"]
                    if "LocalityWait" in keys:
                        self.cfg_dict["LocalityWait"] = c.LOCALITY_WAIT = setup["Spark"]["LocalityWait"]
                    if "LocalityWaitNode" in keys:
                        self.cfg_dict["LocalityWaitNode"] = c.LOCALITY_WAIT_NODE = setup["Spark"]["LocalityWaitNode"]
                    if "LocalityWaitProcess" in keys:
                        self.cfg_dict["LocalityWaitProcess"] = c.LOCALITY_WAIT_PROCESS = setup["Spark"]["LocalityWaitProcess"]
                    if "LocalityWaitRack" in keys:
                        self.cfg_dict["LocalityWaitRack"] = c.LOCALITY_WAIT_RACK = setup["Spark"]["LocalityWaitRack"]
                    if "CpuTask" in keys:
                        self.cfg_dict["CpuTask"] = c.CPU_TASK = setup["Spark"]["CpuTask"]
                
                if "xSpark" in keys:
                    k_xSpark = setup["xSpark"].keys()
                    if "Home" in k_xSpark:
                        self.cfg_dict["CSparkHome"] = c.C_SPARK_HOME = setup["xSpark"]["Home"]
                    #C_SPARK_HOME = setup["Spark"]["CSparkHome"] if "CSparkHome" in keys else C_SPARK_HOME
                    #SPARK_SEQ_HOME = setup["Spark"]["SparkSeqHome"] if "SparkSeqHome" in keys else SPARK_SEQ_HOME
                    #SPARK_2_HOME = setup["Spark"]["Spark2Home"] if "Spark2Home" in keys else SPARK_2_HOME
                    #LOG_LEVEL = setup["Spark"]["LogLevel"] if "LogLevel" in keys else LOG_LEVEL
                    #UPDATE_SPARK = setup["Spark"]["UpdateSpark"] if "UpdateSpark" in keys else UPDATE_SPARK
                    #UPDATE_SPARK_MASTER = setup["Spark"]["UpdateSparkMaster"] if "UpdateSparkMaster" in keys else UPDATE_SPARK_MASTER
                    #UPDATE_SPARK_DOCKER = setup["Spark"]["UpdateSparkDocker"] if "UpdateSparkDocker" in keys else UPDATE_SPARK_DOCKER
                    #RAM_DRIVER = setup["Spark"]["RamDriver"] if "RamDriver" in keys else RAM_DRIVER
                    #OFF_HEAP = setup["Spark"]["OffHeap"] if "OffHeap" in keys else OFF_HEAP
                    #OFF_HEAP_BYTES = setup["Spark"]["OffHeapBytes"] if "OffHeapBytes" in keys else OFF_HEAP_BYTES
                    #DISABLE_HT = not setup["Spark"]["HyperThreading"] if "HyperThreading" in keys else DISABLE_HT
                    
                if "ControlledMode" in keys:
                    if setup["ControlledMode"]:
                        if "xSpark" in keys and "Home"in setup["xSpark"]:
                            self.cfg_dict["SparkHome"] = c.SPARK_HOME = setup["xSpark"]["Home"] 
                    elif "Spark" in keys and "Home"in setup["Spark"]:
                            self.cfg_dict["SparkHome"] = c.SPARK_HOME = setup["Spark"]["Home"]
                    
                print("Configuration from " + filepath + " done")                    
            else: 
                print("Setup file: " + filepath + "  is empty: using defaults")
        else: 
            print("Setup file:  " + filepath + "  found: using defaults")
    
    def config_control(self, filepath):
        control_file = Path(filepath)
        if control_file.exists():
            control = json.load(open(filepath))
            if len(control) > 0:
                keys = control.keys()
                if "Alpha" in keys:
                    self.cfg_dict["Alpha"] = c.ALPHA = control["Alpha"]
                if "Beta" in keys:
                    self.cfg_dict["Beta"] = c.BETA = control["Beta"]
                if "OverScale" in keys:
                    self.cfg_dict["OverScale"] = c.OVER_SCALE = control["OverScale"]
                if "K" in keys:
                    self.cfg_dict["K"] = c.K = control["K"]
                if "Ti" in keys:
                    self.cfg_dict["Ti"] = c.TI = control["Ti"]
                if "TSample" in keys:
                    self.cfg_dict["TSample"] = c.T_SAMPLE  = control["TSample"]
                if "Heuristic" in keys:
                    c.HEURISTIC = eval("c.Heuristic." + control["Heuristic"])
                    self.cfg_dict["Heuristic"] = c.HEURISTIC.name
                if "CoreQuantum" in keys:
                    self.cfg_dict["CoreQuantum"] = c.CORE_QUANTUM = control["CoreQuantum"]
                if "CoreMin" in keys:
                    self.cfg_dict["CoreMin"] = c.CORE_MIN = control["CoreMin"]
                if "CpuPeriod" in keys:
                    self.cfg_dict["CpuPeriod"] = c.CPU_PERIOD = control["CpuPeriod"]
                #DEADLINE = control["Deadline"] if "Deadline" in keys else DEADLINE
                #MAX_EXECUTOR = control["MaxExecutor"] if "MaxExecutor" in keys else MAX_EXECUTOR
                #CORE_VM = control["CoreVM"] if "CoreVM" in keys else CORE_VM
                #STAGE_ALLOCATION = control["StageAllocation"] if "StageAllocation" in keys else STAGE_ALLOCATION
                #CORE_ALLOCATION = control["CoreAllocation"] if "CoreAllocation" in keys else CORE_ALLOCATION
                #DEADLINE_ALLOCATION = control["DeadlineAllocation"] if "DeadlineAllocation" in keys else DEADLINE_ALLOCATION
                print("Configuration from " + filepath + " done")
            else: 
                print("Control file: " + filepath + " is empty: using defaults")
        else: 
            print("Control file: " + filepath + "  not found: using defaults")
    
    @staticmethod
    def update_config_parms(self):
        if len(c.BENCHMARK_PERF) + len(c.BENCHMARK_BENCH) > 1 or len(c.BENCHMARK_PERF) + len(
                c.BENCHMARK_BENCH) == 0:
            print("ERROR BENCHMARK SELECTION")
            exit(1)
        
        if len(c.BENCHMARK_PERF) > 0:
            self.cfg_dict["ScaleFactor"] = c.SCALE_FACTOR = c.BENCH_CONF[c.BENCHMARK_PERF[0]]["ScaleFactor"]
            self.cfg_dict["InputRecord"] = c.INPUT_RECORD = 200 * 1000 * 1000 * c.SCALE_FACTOR
            self.cfg_dict["NumTask"] = c.NUM_TASK = c.SCALE_FACTOR
        else:
            self.cfg_dict["ScaleFactor"] = c.SCALE_FACTOR = c.BENCH_CONF[c.BENCHMARK_BENCH[0]]["NUM_OF_PARTITIONS"][1]
            self.cfg_dict["NumTask"] = c.NUM_TASK = c.SCALE_FACTOR
            try:
                self.cfg_dict["InputRecord"] = c.INPUT_RECORD = c.BENCH_CONF[c.BENCHMARK_BENCH[0]]["NUM_OF_EXAMPLES"][1]
            except KeyError:
                try:
                    self.cfg_dict["InputRecord"] = c.INPUT_RECORD = c.BENCH_CONF[c.BENCHMARK_BENCH[0]]["NUM_OF_POINTS"][1]
                except KeyError:
                    self.cfg_dict["InputRecord"] = c.INPUT_RECORD = c.BENCH_CONF[c.BENCHMARK_BENCH[0]]["numV"][1]
        self.cfg_dict["BenchConf"][self.cfg_dict["BenchmarkPerf"] if len(self.cfg_dict["BenchmarkPerf"]) > 0 else self.cfg_dict["BenchmarkBench"][0]]["NumTrials"] = \
            c.BENCH_CONF[c.BENCHMARK_PERF[0] if len(c.BENCHMARK_PERF) > 0 else c.BENCHMARK_BENCH[0]]["NumTrials"] = c.BENCH_NUM_TRIALS
    
        self.cfg_dict["Hdfs"] = c.HDFS = 1 if c.HDFS_MASTER == "" else 0
        #DELETE_HDFS = 1 if SCALE_FACTOR != PREV_SCALE_FACTOR else 0
        
        self.cfg_dict["PrivateKeyPath"] = c.PRIVATE_KEY_PATH = c.KEY_PAIR_PATH if c.PROVIDER == "AWS_SPOT" \
            else c.AZ_PRV_KEY_PATH if c.PROVIDER == "AZURE" \
            else None
        self.cfg_dict["PrivateKeyName"] = c.PRIVATE_KEY_NAME = c.KEY_PAIR_PATH.split("/")[-1] if c.PROVIDER == "AWS_SPOT" \
            else c.AZ_PRV_KEY_PATH.split("/")[-1] if c.PROVIDER == "AZURE" \
            else None
        self.cfg_dict["TemporaryStorgae"] = c.TEMPORARY_STORAGE = "/dev/xvdb" if c.PROVIDER == "AWS_SPOT" \
            else "/dev/sdb1" if c.PROVIDER == "AZURE" \
            else None
        
        self.cfg_dict["ConfigDict"] = c.CONFIG_DICT = {
                                        "Provider": c.PROVIDER,
                                        "Benchmark": {
                                            "Name": c.BENCHMARK_PERF[0] if len(c.BENCHMARK_PERF) > 0 else c.BENCHMARK_BENCH[0],
                                            "Config": c.BENCH_CONF[c.BENCHMARK_PERF[0] if len(c.BENCHMARK_PERF) > 0 else c.BENCHMARK_BENCH[0]]
                                        },
                                        "Deadline": c.DEADLINE,
                                        "Control": {
                                            "Alpha": c.ALPHA,
                                            "Beta": c.BETA,
                                            "OverScale": c.OVER_SCALE,
                                            "MaxExecutor": c.MAX_EXECUTOR,
                                            "CoreVM": c.CORE_VM,
                                            "K": c.K,
                                            "Ti": c.TI,
                                            "TSample": c.T_SAMPLE,
                                            "CoreQuantum": c.CORE_QUANTUM,
                                            "Heuristic": c.HEURISTIC.name,
                                            "CoreAllocation": c.CORE_ALLOCATION,
                                            "DeadlineAllocation": c.DEADLINE_ALLOCATION,
                                            "StageAllocation": c.STAGE_ALLOCATION
                                        },
                                        "Aws": {
                                            "InstanceType": c.INSTANCE_TYPE,
                                            "HyperThreading": not c.DISABLE_HT,
                                            "Price": c.PRICE,
                                            "AMI": c.DATA_AMI[c.REGION]["ami"],
                                            "Region": c.REGION,
                                            "AZ": c.DATA_AMI[c.REGION]["az"],
                                            "SecurityGroup": c.SECURITY_GROUP,
                                            "KeyPair": c.DATA_AMI[c.REGION]["keypair"],
                                            "EbsOptimized": c.EBS_OPTIMIZED,
                                            "SnapshotId": c.DATA_AMI[c.REGION]["snapid"]
                                        },
                                        "Azure": {
                                            "NodeSize": c.AZ_SIZE,
                                            "NodeImage": c.AZ_VHD_IMAGE,
                                            "Location": c.AZ_LOCATION,
                                            "PubKeyPath": c.AZ_PUB_KEY_PATH,
                                            "ClusterId": c.CLUSTER_ID,
                                            "ResourceGroup": c.AZ_RESOURCE_GROUP,
                                            "StorageAccount": {"Name": c.AZ_STORAGE_ACCOUNT,
                                                               "Sku": c.AZ_SA_SKU,
                                                               "Kind": c.AZ_SA_KIND},
                                            "Network": c.AZ_NETWORK,
                                            "Subnet": c.AZ_SUBNET,
                                            "SecurityGroup": c.AZ_SECURITY_GROUP
                                        },
                                        "Spark": {
                                            "ExecutorCore": c.CORE_VM,
                                            "ExecutorMemory": c.RAM_EXEC,
                                            "ExternalShuffle": c.ENABLE_EXTERNAL_SHUFFLE,
                                            "LocalityWait": c.LOCALITY_WAIT,
                                            "LocalityWaitProcess": c.LOCALITY_WAIT_PROCESS,
                                            "LocalityWaitNode": c.LOCALITY_WAIT_NODE,
                                            "LocalityWaitRack": c.LOCALITY_WAIT_RACK,
                                            "CPUTask": c.CPU_TASK,
                                            "SparkHome": c.SPARK_HOME
                                        },
                                        "HDFS": bool(c.HDFS),
                                    }

    @staticmethod
    def exp_par_map(parm):
        map = {
            "ScaleFactor": "ScaleFactor",
            "NumPartitions": "num-partitions",
            "UniqueKeys": "unique-keys",
            "ReduceTasks": "reduce-tasks",
            "NumOfPartitions": "NUM_OF_PARTITIONS",
            "NumV": "numV",
            "Mu": "mu",
            "Sigma": "sigma",
            "MaxIterations": "MAX_ITERATION",
            "NumTrials": "NumTrials",
            "NumOfPoints": "NUM_OF_POINTS",
            "NumOfClusters": "NUM_OF_CLUSTERS",
            "Dimensions": "DIMENSIONS",
            "Scaling": "SCALING",
            "NumOfExamples": "NUM_OF_EXAMPLES",
            "NumOfFeatures": "NUM_OF_FEATURES",
            "NumOfClassC": "NUM_OF_CLASS_C",
        }
        return map.get(parm, None)
    
    @staticmethod
    def exp_inverse_par_map(parm):
        map = {
            "ScaleFactor": "ScaleFactor",
            "num-partitions": "NumPartitions",
            "unique-keys": "UniqueKeys",
            "reduce-tasks": "ReduceTasks",
            "NUM_OF_PARTITIONS": "NumOfPartitions",
            "numV": "NumV",
            "mu": "Mu",
            "sigma": "Sigma",
            "MAX_ITERATION": "MaxIterations",
            "NumTrials": "NumTrials",
            "NUM_OF_POINTS": "NumOfPoints",
            "NUM_OF_CLUSTERS": "NumOfClusters",
            "DIMENSIONS": "Dimensions",
            "SCALING": "Scaling",
            "NUM_OF_EXAMPLES": "NumOfExamples",
            "NUM_OF_FEATURES": "NumOfFeatures",
            "NUM_OF_CLASS_C": "NumOfClassC",
        }
        return map.get(parm, None)
    
    @staticmethod
    def exp_lowercase_to_camelcase_par_map(parm):
        map = {
            "scalefactor": "ScaleFactor",
            "numpartitions": "NumPartitions",
            "uniquekeys": "UniqueKeys",
            "reducetasks": "ReduceTasks",
            "numofpartitions": "NumOfPartitions",
            "numv": "NumV",
            "mu": "Mu",
            "sigma": "Sigma",
            "maxIterations": "MaxIterations",
            "numtrials": "NumTrials",
            "numofpoints": "NumOfPoints",
            "numofclusters": "NumOfClusters",
            "dimensions": "Dimensions",
            "scaling": "Scaling",
            "numofexamples": "NumOfExamples",
            "numoffeatures": "NumOfFeatures",
            "numofclassc": "NumOfClassC",
        }
        return map.get(parm, None)
    
config_instance = Config()

#print("PROCESS_ON_SERVER: "+ str(c.PROCESS_ON_SERVER))
#print("instantiated config_instance: " + str(config_instance.cfg_dict))