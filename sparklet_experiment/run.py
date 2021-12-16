from .spark_yarn import SparkCommand
from . import config

app_spark_rdd_main_class = {
    "wc": "org.pacman.run.MeasureWordCountRDD",
    "pr": "org.pacman.run.MeasurePageRankRDD",
    "km": "org.pacman.run.MeasureKMeansRDD",
    "lr": "org.pacman.run.MeasureLRRDD",
}

app_name = {
    "wc": "WordCount",
    "pr": "PageRank",
    "km": "KMeans",
    "lr": "LR",
    "cc": "WCC",
    "ts": "TeraSort",
}

app_sparklet_module_name = {
    "wc": "word_count",
    "pr": "page_rank_rdd",
    "km": "kmeans_new",
    "lr": "lr_new",
    "cc": "wcc_rdd",
    "ts": "terasort",
}


def to_hdfs_path(path):
    return "hdfs://bic07:8020" + path

app_dataset_hdd = {
    "wc": to_hdfs_path("/user/ybw/hdd/Dataset/deepmind-gutenberg.text"),
    "pr": to_hdfs_path("/user/ybw/hdd/Dataset/twitter-2010.text"),
    "km": to_hdfs_path("/user/ybw/hdd/Dataset/data-Kmeans-80000000-64.text"),
    "lr": to_hdfs_path("/user/ybw/hdd/Dataset/MNIST01-12665-10k-784.text"),
    "cc": to_hdfs_path("/user/ybw/hdd/Dataset/twitter-2010.text"),
    "ts-in": to_hdfs_path("/user/ybw/hdd/Dataset/terasort-250G"),
    "ts-out": to_hdfs_path("/user/ybw/hdd/output/terasort-250G-sorted"),
}

app_dataset_ssd = {
    "wc": to_hdfs_path("/user/ybw/ssd/Dataset/deepmind-gutenberg.text"),
    "pr": to_hdfs_path("/user/ybw/ssd/Dataset/twitter-2010.text"),
    "km": to_hdfs_path("/user/ybw/ssd/Dataset/data-Kmeans-80000000-64.text"),
    "lr": to_hdfs_path("/user/ybw/ssd/Dataset/MNIST01-12665-10k-784.text"),
    "cc": to_hdfs_path("/user/ybw/ssd/Dataset/twitter-2010.text"),
    "ts-in": to_hdfs_path("/user/ybw/ssd/Dataset/terasort-250G"),
    "ts-out": to_hdfs_path("/user/ybw/ssd/output/terasort-250G-sorted"),
}

SPARK_BENCHMARK_CHECKPOINT_DIR = to_hdfs_path("/user/ybw/ssd/checkpoint")

if config.STORAGE_DEVICE_TYPE == config.STORAGE_DEVICE_TYPE_HDD:
    app_dataset = app_dataset_hdd
elif config.STORAGE_DEVICE_TYPE == config.STORAGE_DEVICE_TYPE_SSD:
    app_dataset = app_dataset_ssd

num_iters = 10

def get_sparklet_wc_args(total_num_cores):
    return f"""{app_dataset["wc"]} {total_num_cores} 32"""


def get_sparklet_pr_args(total_num_cores):
    return f"""{app_dataset["pr"]} {total_num_cores} 10"""


def get_sparklet_km_args(total_num_cores):
    return f"""{app_dataset["km"]} 500 {num_iters} {total_num_cores}"""


def get_sparklet_lr_args(total_num_cores):
    return f"""{app_dataset["lr"]} {num_iters} {total_num_cores * 4}""" # LR dataset is too large


def get_sparklet_cc_args(total_num_cores):
    return f"""{app_dataset["cc"]} {total_num_cores}"""


def get_sparklet_ts_args(total_num_cores):
    return f"""{app_dataset["ts-in"]} {app_dataset["ts-out"]} 2048"""


app_args = {
    "wc": get_sparklet_wc_args,
    "pr": get_sparklet_pr_args,
    "km": get_sparklet_km_args,
    "lr": get_sparklet_lr_args,
    "cc": get_sparklet_cc_args,
    "ts": get_sparklet_ts_args,
}

# Fault injection point in (job_id, stage_id)
app_fault_injection_point = {
    "wc": (0, 1),
    "ts": (1, 1),
    "pr": (2, 5),
    "cc": (13, 0),
    "km": (7, 0),
    "lr": (7, 0),
}

def set_fault_injection_point(app):
    job_id, stage_id = app_fault_injection_point[app]
    config.SPARK_CONFIG_BASE["spark.sparklet.injectionJobId"] = job_id
    config.SPARK_CONFIG_BASE["spark.sparklet.injectionStageId"] = stage_id

if __name__ == "__main__":
    sc = SparkCommand()
    from .benchmark_params import *
    num_executors = 64
    executor_memory_gb = 22
    num_cores_per_executor = [7]

    def getResultDir(is_spark=False, is_strawman_a=False, is_strawman_b=False):
        curr = config.RESULT_DIR
        is_hdd = config.STORAGE_DEVICE_TYPE_HDD == config.STORAGE_DEVICE_TYPE
        is_ucx = config.ENABLE_SPARK_UCX
        if config.ENABLE_FAULT_INJECTION:
            curr += "/fault"
        if config.ENABLE_SPECULATIVE_EXECUTION:
            curr += "/speculative"
        if is_hdd:
            curr += "/hdd"
        if is_spark:
            curr += "/spark"
        if is_ucx:
            curr += "/ucx"
        if is_strawman_a:
            curr += "/strawmanA"
        if is_strawman_b:
            curr += "/strawmanB"
        return curr

    def get_sparklet_commandline(app, nc):
        if (config.ENABLE_FAULT_INJECTION):
            set_fault_injection_point(app)
        total_num_cores = num_executors * nc
        module = app_sparklet_module_name[app]
        args = app_args[app](total_num_cores)
        this_app_name = app_name[app]
        result_path = f"{getResultDir(is_spark=False)}/{app}_{num_executors}"
        if config.USE_STRAWMAN_IMPLEMENTATION:
            result_path = f"{getResultDir(is_spark=False, is_strawman_a=True)}/{app}_{num_executors}"
        if config.USE_STRAWMAN_B:
            result_path = f"{getResultDir(is_spark=False, is_strawman_b=True)}/{app}_{num_executors}"
        cmdline = sc.submit_sparklet(f"{this_app_name}", f"{module}_so", args, num_executors=num_executors,
                                    executor_memory_gb=executor_memory_gb, executor_cores=nc)
        return f"{cmdline} 1>{result_path}.log.stdout 2>{result_path}.log.stderr"

    def get_spark_commandline(app, baseline, nc):
        if (config.ENABLE_FAULT_INJECTION):
            set_fault_injection_point(app)
        total_num_cores = num_executors * nc
        args = app_args[app](total_num_cores)
        this_app_name = f"Spark_{app_name[app]}_{baseline}"
        result_path = f"{getResultDir(is_spark=True)}/{app}_{baseline}_{num_executors}"
        cmdline = sc.submit_spark(f"{this_app_name}", config.SPARK_BENCHMARK_PATH, "org.pacman.Launch", f"{app} {baseline} {SPARK_BENCHMARK_CHECKPOINT_DIR} {args}", num_executors=num_executors,
                                    executor_memory_gb=executor_memory_gb, executor_cores=nc)
        return f"{cmdline} 1>{result_path}.log.stdout 2>{result_path}.log.stderr"

    sparklet = True
    spark_all = True
    spark_rdd = False
    spark_blas = True

    if True:
        # Fault tolerance
        sparklet = True
        spark_all = False
        spark_rdd = True
        spark_blas = False

    if sparklet:
        # Sparklet
        for app in app_list:
            for nc in num_cores_per_executor:
                print(get_sparklet_commandline(app, nc))

    if spark_all:
        # Spark
        for app in app_list:
            for baseline in baseline_list:
                for nc in num_cores_per_executor:
                    print(get_spark_commandline(app, baseline, nc))

    if spark_rdd:
        # Spark (RDD Only)
        for app in app_list:
            baseline = "rdd"
            for nc in num_cores_per_executor:
                print(get_spark_commandline(app, baseline, nc))

    if spark_blas:
        # Spark BLAS
        for nc in num_cores_per_executor:
            for app in ["km", "lr"]:
                print(get_spark_commandline(app, "rddblas", nc))
    
    if False:
        for (baseline, app) in [('sql', 'pr'), ('sql', 'cc')]:
            assert(baseline in {*baseline_list})
            assert(app in {*app_list})
            nc = 7
            total_num_cores = num_executors * nc
            args = app_args[app](total_num_cores)
            this_app_name = f"Spark_{app_name[app]}_{baseline}"
            if config.ENABLE_SPARK_UCX:
                result_path = f"/home/ybw/Research/PDD/result/new/ucx/spark/{app}_{baseline}_{num_executors}"
            else:
                result_path = f"/home/ybw/Research/PDD/result/new/spark/{app}_{baseline}_{num_executors}"
            cmdline = sc.submit_spark(f"{this_app_name}", config.SPARK_BENCHMARK_PATH, "org.pacman.Launch", f"{app} {baseline} {SPARK_BENCHMARK_CHECKPOINT_DIR} {args}", num_executors=num_executors,
                                        executor_memory_gb=executor_memory_gb, executor_cores=nc)
            print(f"{cmdline} 1>{result_path}.log.stdout 2>{result_path}.log.stderr")


# Show spark-shell
# print(sc.spark_shell(f"Explore", num_executors=num_executors, executor_memory_gb=executor_memory_gb, executor_cores=nc))
