from .config import *


class SparkCommand(object):
    def __init__(self):
        self.spark_home = SPARK_HOME
        self.adapter_path = CMR_ADAPTER_PATH
        self.event_log_dir = EVENT_LOG_DIR

    def _config_argument(self, conf):
        return " ".join([f"--conf {k}='{conf[k]}'" for k in conf])

    def _normal_config(self):
        return self._config_argument(SPARK_CONFIG_BASE)

    def _native_config(self):
        return self._config_argument({**SPARK_CONFIG_BASE, **SPARK_CONFIG_ADAPTER})

    def _get_env_var_prefix(self, env_var):
        return " ".join([f"{x}={env_var[x]}" for x in env_var])

    def prepare(self, build_prefix=BUILD_PREFIX):
        return f"""
            cd {build_prefix}
            cmake .
            make -j32
            clush -w bic0[1-9] rm -rf /dev/shm/*
            clush -w bic0[1-9] "~/drop_cache"
            """

    def submit_sparklet(self, name, target, args, module_prefix=f"{BUILD_PREFIX}/applications", num_executors=2, driver_memory_gb=DRIVER_MEMORY_GB, executor_cores=EXECUTOR_CORES, executor_memory_gb=EXECUTOR_MEMORY_GB, env_vars=dict()):
        packages = getPackagesCommand(CMR_ADAPTER_PACKAGES)
        return f"""SPARK_HOME={self.spark_home} {genEnvCommadline(BASE_ENVIRONMENT_VARIABLE)} {self._get_env_var_prefix(env_vars)} {self.spark_home}/bin/spark-submit --master yarn --name {name} --deploy-mode {DEPLOY_MODE} --num-executors {num_executors} --conf spark.dynamicAllocation.maxExecutors={num_executors} --executor-cores {executor_cores} --executor-memory {executor_memory_gb}g --driver-memory {driver_memory_gb}g {self._native_config()} --class org.pacman.cmr.applications.Launch --conf spark.module.name={target} --conf spark.module.path={module_prefix} {packages} {self.adapter_path} {args}"""

    def submit_spark(self, name, jar_path, main_class, args, num_executors=2, driver_memory_gb=DRIVER_MEMORY_GB, executor_cores=EXECUTOR_CORES, executor_memory_gb=EXECUTOR_MEMORY_GB, env_vars=dict()):
        packages = getPackagesCommand(SPARK_BENCHMARK_PACKAGES)
        repositories = getRepositoriesCommand(SPARK_BENCHMARK_REPOSITORIES)
        return f"""SPARK_HOME={self.spark_home} {genEnvCommadline(BASE_ENVIRONMENT_VARIABLE)} {self._get_env_var_prefix(env_vars)} {self.spark_home}/bin/spark-submit --master yarn --name {name} --deploy-mode {DEPLOY_MODE} --num-executors {num_executors} --conf spark.dynamicAllocation.maxExecutors={num_executors} --executor-cores {executor_cores} --executor-memory {executor_memory_gb}g --driver-memory {driver_memory_gb}g {self._normal_config()} {packages} {repositories} --class {main_class} {jar_path} {args}"""

    def spark_shell(self, name, num_executors=32, driver_memory_gb=DRIVER_MEMORY_GB, executor_cores=EXECUTOR_CORES, executor_memory_gb=EXECUTOR_MEMORY_GB, env_vars=dict()):
        packages = getPackagesCommand(SPARK_BENCHMARK_PACKAGES)
        repositories = getRepositoriesCommand(SPARK_BENCHMARK_REPOSITORIES)
        return f"""SPARK_HOME={self.spark_home} {genEnvCommadline(BASE_ENVIRONMENT_VARIABLE)} {self._get_env_var_prefix(env_vars)} {self.spark_home}/bin/spark-shell --master yarn --name {name} --deploy-mode {DEPLOY_MODE} --num-executors {num_executors} --conf spark.dynamicAllocation.maxExecutors={num_executors} --executor-cores {executor_cores} --executor-memory {executor_memory_gb}g --driver-memory {driver_memory_gb}g {self._normal_config()} {packages} {repositories}"""
