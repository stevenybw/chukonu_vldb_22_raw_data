import os

CMR_PREFIX = "/home/ybw/OpenSourceCode/CMR"
BUILD_PREFIX = f"{CMR_PREFIX}/build"
BUILD_PREFIX_EXP1 = f"{CMR_PREFIX}/build_exp1"
BUILD_PREFIX_EXP2 = f"{CMR_PREFIX}/build_exp2"
BUILD_PREFIX_EXP3 = f"{CMR_PREFIX}/build_exp3"
BUILD_PREFIX_EXP4 = f"{CMR_PREFIX}/build_exp4"
BUILD_PREFIX_EXP5 = f"{CMR_PREFIX}/build_exp5"
RESULT_DIR = "/home/ybw/Research/PDD/result/new"
TEMP_DATASET_PATH = "/tmp/chukonu_dataset.txt"
CMR_ADAPTER_PATH = f"{CMR_PREFIX}/spark/cmr-spark-adapter_2.12-0.1.jar"
CMR_ADAPTER_PACKAGES = {"net.java.dev.jna:jna:5.6.0"}
SPARK_BENCHMARK_PATH = f"{CMR_PREFIX}/spark/sparkbenchmark_2.12-0.1.jar"
SPARK_BENCHMARK_PACKAGES = {"org.apache.hadoop:hadoop-mapreduce-examples:2.7.4", "graphframes:graphframes:0.8.1-spark3.0-s_2.12"}
SPARK_BENCHMARK_REPOSITORIES = {"https://repos.spark-packages.org/"}

def getPackagesCommand(packages):
    return f"--packages {','.join(packages)}"

def getRepositoriesCommand(repos):
    return f"--repositories {','.join(repos)}"

#UCX_LIBRARY_PATH = "/home/ybw/Software/ucx-1.8.0-java/lib"
#UCX_JAVA_BINDING_PATH = f"{UCX_LIBRARY_PATH}/jucx-1.8.0.jar"

UCX_LIBRARY_PATH = "/home/ybw/Software/ucx-1.9.0-java-dbg/lib"
UCX_JAVA_BINDING_PATH = f"{UCX_LIBRARY_PATH}/jucx-1.9.0.jar"

# UCX_LIBRARY_PATH = "/home/ybw/Software/ucx-1.10.0-java-mt/lib"
# UCX_JAVA_BINDING_PATH = f"{UCX_LIBRARY_PATH}/jucx-1.10.0.jar"

# UCX_LIBRARY_PATH = "/home/ybw/Software/ucx-1.10.1-rc1-java/lib"
# UCX_JAVA_BINDING_PATH = f"{UCX_LIBRARY_PATH}/jucx-1.10.1.jar"

SPARK_UCX_PATH = "/home/ybw/OpenSourceCode/sparkucx/target/spark-ucx-1.0-for-spark-3.0.jar"
EXECUTOR_CORES = 7
DRIVER_MEMORY_GB = 22
EXECUTOR_MEMORY_GB = 25
EVENT_LOG_DIR = "hdfs://bic07.lab.pacman-thu.org:8020/user/spark/applicationHistory"
ENABLE_DYNAMIC_ALLOCATION = False
ENABLE_SHUFFLE_SERVICE = True # TODO
ENABLE_SPECULATIVE_EXECUTION = False
ENABLE_SPARK_UCX = False
ENABLE_FAULT_INJECTION = False
ENABLE_ASAN = False
DEPLOY_MODE = "client"
USE_STRAWMAN_IMPLEMENTATION = False
USE_STRAWMAN_B = False
STORAGE_DEVICE_TYPE_HDD = "hdd"
STORAGE_DEVICE_TYPE_SSD = "ssd"
STORAGE_DEVICE_TYPE = "ssd"

if USE_STRAWMAN_IMPLEMENTATION or USE_STRAWMAN_B or ENABLE_ASAN:
    print("WARNING: Check your parameters")

if USE_STRAWMAN_IMPLEMENTATION:
    print("USE_STRAWMAN_IMPLEMENTATION")
    BUILD_PREFIX = BUILD_PREFIX_EXP1

assert(DEPLOY_MODE in {"cluster", "client"})
#SPARK_HOME = os.environ['SPARK_HOME']
#JAVA_HOME = os.environ['JAVA_HOME']
# JAVA_HOME = "/usr/lib/jvm/java-8-oracle"
JAVA_HOME = "/home/ybw/Software/jdk-11.0.8"
SPARK_HOME = "/home/ybw/Software/spark-3.0.1-bin-hadoop2.7"

BASE_ENVIRONMENT_VARIABLE = {
    "JAVA_HOME": JAVA_HOME,
}

ASAN_ENVIRONMENT_VARIABLE = {
    "ASAN_OPTIONS": "detect_leaks=0",
    "LD_PRELOAD": "/spack-bic/opt/spack/linux-ubuntu16.04-broadwell/gcc-9.2.0/gcc-10.1.0-lbfxfidfshnlcwat66gryycsrhbtegzk/lib64/libasan.so.6",
}

if ENABLE_ASAN:
    BASE_ENVIRONMENT_VARIABLE = {**BASE_ENVIRONMENT_VARIABLE, **ASAN_ENVIRONMENT_VARIABLE}

def genEnvRelatedConf(envs):
    ans = dict()
    for k in envs:
        ans[f"spark.executorEnv.{k}"] = envs[k] 
        ans[f"spark.yarn.appMasterEnv.{k}"] = envs[k]
    return ans

def genEnvCommadline(envs):
    return " ".join([f"{k}={envs[k]}" for k in envs])


DYNAMIC_ALLOCATION_CONFIG = {"spark.shuffle.service.enabled": "true",
                             "spark.dynamicAllocation.enabled": "true", "spark.dynamicAllocation.minExecutors": "0"}

SPARK_UCX_CONFIG = {
    "spark.driver.extraClassPath": f"{UCX_LIBRARY_PATH}:{UCX_JAVA_BINDING_PATH}:{SPARK_UCX_PATH}",
    "spark.executor.extraClassPath": f"{UCX_LIBRARY_PATH}:{UCX_JAVA_BINDING_PATH}:{SPARK_UCX_PATH}",
    "spark.shuffle.manager": "org.apache.spark.shuffle.UcxShuffleManager",
    "spark.shuffle.sort.io.plugin.class": "org.apache.spark.shuffle.compat.spark_3_0.UcxLocalDiskShuffleDataIO",
}

SPARK_CONFIG_BASE = {"spark.eventLog.enabled": "true",
                     "spark.eventLog.dir": EVENT_LOG_DIR,
                     "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    #  "spark.executorEnv.JAVA_HOME": JAVA_HOME,
                    #  "spark.yarn.appMasterEnv.JAVA_HOME": JAVA_HOME,
                     "spark.yarn.submit.waitAppCompletion": "true",
                     "spark.scheduler.minRegisteredResourcesRatio": "1.0",
                     "spark.driver.extraJavaOptions": f"-verbose:gc -XX:+PrintGCDetails -ea -Xss4m -XX:ErrorFile=/tmp/hs_err_pid%p.log",
                     "spark.executor.extraJavaOptions": f"-verbose:gc -XX:+PrintGCDetails -ea -Xss4m -XX:ErrorFile=/tmp/hs_err_pid%p.log",
                     "spark.sparklet.numRepeats": "3",
                     "spark.metrics.conf": f"{SPARK_HOME}/conf/metrics.properties",
                     "spark.executor.processTreeMetrics.enabled": "true",
                     }


SPARK_CONFIG_BASE = {**SPARK_CONFIG_BASE, **genEnvRelatedConf(BASE_ENVIRONMENT_VARIABLE)}

if (USE_STRAWMAN_B):
    SPARK_CONFIG_BASE["spark.sparklet.strawmanB"] = "true"

if ENABLE_SHUFFLE_SERVICE:
    SPARK_CONFIG_BASE["spark.shuffle.service.enabled"] = "true"

# Enlarge the memory configuration to avoid data spilling
SPARK_CONFIG_BASE["spark.memory.fraction"] = "0.9"
SPARK_CONFIG_BASE["spark.memory.storageFraction"] = "0.8"

if ENABLE_SPECULATIVE_EXECUTION:
    SPARK_CONFIG_BASE["spark.speculation"] = "true"
    # SPARK_CONFIG_BASE["spark.speculation.multiplier"] = "3.0"

if ENABLE_FAULT_INJECTION:
    SPARK_CONFIG_BASE["spark.sparklet.injectFault"] = "true"

if ENABLE_DYNAMIC_ALLOCATION:
    SPARK_CONFIG_BASE = {**SPARK_CONFIG_BASE, **DYNAMIC_ALLOCATION_CONFIG}

if ENABLE_SPARK_UCX:
    SPARK_CONFIG_BASE = {**SPARK_CONFIG_BASE, **SPARK_UCX_CONFIG}

# Config for Sparklet
SPARK_CONFIG_ADAPTER = {
    # The following is for enable custom shuffle manager
    #"spark.driver.extraClassPath": f"{CMR_ADAPTER_PATH}",
    #"spark.executor.extraClassPath": f"{CMR_ADAPTER_PATH}",
    #"spark.shuffle.manager": "org.apache.spark.shuffle.DisposableShuffleManager",
    #"spark.shuffle.disposable.manager": "org.apache.spark.shuffle.sort.SortShuffleManager"
}
