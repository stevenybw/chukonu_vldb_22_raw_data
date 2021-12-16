from sparklet_experiment import data_thrill_husky
from sparklet_experiment import utilities
import pandas as pd
import pandasql as ps
from IPython.display import display

results_dir = "results"
METRICS_DIR = "/home/ybw/Research/PDD/result/new/spark/metrics"

results_1 = utilities.load(f"{results_dir}/primary.pickle")
results_2 = data_thrill_husky.get()
results = pd.concat([results_1, results_2])

def readFromMetricCsv(path):
    with open(path, "r") as f:
        next(f)
        for l in f:
            sp = l.split(",")
            ts = int(sp[0])
            val = int(sp[1])
            yield (ts, val)


def readExecutorsTraces(appId, name="ExecutorMetrics.ProcessTreeJVMRSSMemory", numExecutors=64):
    def do_read():
        for executorId in range(1, numExecutors+1):
            path = f"{METRICS_DIR}/{appId}.{executorId}.{name}.csv"
            ts, vs = zip(*[(ts, val) for ts, val in readFromMetricCsv(path)])
            yield executorId, ts, vs
    executorTraces = list(do_read())
    mints = min([min(ts) for eid, ts, vs in executorTraces])
    return [(eid, [i - mints for i in ts], vs) for eid, ts, vs in executorTraces]


def readOverallRSSMetrics(appId, executorId):
    executorTraces = readExecutorsTraces(
        appId, "ExecutorMetrics.ProcessTreeJVMRSSMemory")
    maxts = max([max(ts) for eid, ts, vs in executorTraces])
    agg = [0.0 for i in range(maxts+1)]
    for eid, ts, vs in executorTraces:
        dedup = [None for i in range(maxts+1)]  # severe data duplication
        for t, v in zip(ts, vs):
            dedup[t] = v
        for i, v in enumerate(dedup):
            if v is not None:
                agg[i] += v
    vs = [1e-9*i for i in agg]
    return list(range(len(vs))), vs, "RSS (GB)"

# utilization / bandwidth from time / bytes

def readDeltaMetrics(appId, executorId, name):
    executorTraces = readExecutorsTraces(appId, name)
    agg = dict()

    def getDelta(tsAndVal):
        ts = list(sorted(tsAndVal.keys()))
        x, y = zip(*[(ts[i], (tsAndVal[ts[i+1]]-tsAndVal[ts[i]]) /
                   (ts[i+1]-ts[i])) for i in range(len(ts)-1)])
        return x, y
    for eid, ts, vs in executorTraces:
        dedup = dict()
        for t, v in zip(ts, vs):
            if t not in dedup:
                dedup[t] = 0.0
            dedup[t] = max(dedup[t], v)
        x, y = getDelta(dedup)
        for i, v in zip(x, y):
            if i not in agg:
                agg[i] = 0.0
            agg[i] += v
    x = sorted(agg.keys())
    y = [agg[i] for i in x]
    return x, y


def readOverallCPUMetrics(appId, executorId):
    x, y = readDeltaMetrics(appId, executorId, "JVMCPU.jvmCpuTime")
    return x, [1e-9*i for i in y], "CPU Cores"


def readOverallShuffleReadMetrics(appId, executorId):
    x, y = readDeltaMetrics(
        appId, executorId, "executor.shuffleTotalBytesRead")
    return x, [1e-9*i for i in y], "GB/s"


def readRSSMetricOfExecutor(appId, executorId):
    path = f"{METRICS_DIR}/{appId}.{executorId}.ExecutorMetrics.ProcessTreeJVMRSSMemory.csv"
    ts, vs = zip(*[(ts, val) for ts, val in readFromMetricCsv(path)])
    mints = min(ts)
    x = [i - mints for i in ts]
    y = [y*1e-9 for y in vs]
    return x, y, "RSS (GB)"


def readGCTimeOfExecutor(appId, executorId):
    path = f"{METRICS_DIR}/{appId}.{executorId}.executor.shuffleFetchWaitTime.csv"
    ts, vs = zip(*[(ts, val) for ts, val in readFromMetricCsv(path)])
    mints = min(ts)
    x = [i - mints for i in ts]
    y = [y*1e-3 for y in vs]  # Milliseconds to sec
    # y = [(y*1e-3)/(t * 1e-9) for t, y in zip(ts, vs)] # Milliseconds to sec
    return x, y, "GC (%)"


def readCPUUtilizationMetricOfExecutor(appId, executorId):
    # path = f"{METRICS_DIR}/{appId}.{executorId}.executor.cpuTime.csv"
    path = f"{METRICS_DIR}/{appId}.{executorId}.JVMCPU.jvmCpuTime.csv"
    ts, vs = zip(*[(ts, val) for ts, val in readFromMetricCsv(path)])
    mints = min(ts)
    x = [i - mints for i in ts]
    y = [y*1e-9 for y in vs]
    x, y = zip(*[(x[i], (y[i+1]-y[i])/(x[i+1]-x[i]))
               for i in range(len(x)-1) if x[i+1] > x[i]])
    return x, y, "Cores Utilized"


def readShuffleReadBwMetricOfExecutor(appId, executorId):
    path = f"{METRICS_DIR}/{appId}.{executorId}.executor.shuffleTotalBytesRead.csv"
    ts, vs = zip(*[(ts, val) for ts, val in readFromMetricCsv(path)])
    mints = min(ts)
    x = [i - mints for i in ts]
    y = [y*1e-6 for y in vs]
    x, y = zip(*[(x[i], (y[i+1]-y[i])/(x[i+1]-x[i]))
               for i in range(len(x)-1) if x[i+1] > x[i]])
    return x, y, "Shuffle Read (MB/s)"


getTraceFnList = [readOverallRSSMetrics, readOverallCPUMetrics, readOverallShuffleReadMetrics, readOverallCPUMetrics]

if __name__ == "__main__":
    ans = dict()
    for getTraceFn in getTraceFnList:
        for executorId in [0, 10]:
            for app in ["wc", "ts", "pr", "cc", "km", "lr"]:
                appIds = ps.sqldf(
                    f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet', 'rdd', 'rddblas', 'sfw', 'sql');").set_index("baseline").to_dict()["appid"]
                for baseline, appId in appIds.items():
                    print(f"Read from {getTraceFn.__name__} {app} {baseline} {appId} {executorId}")
                    ans[(getTraceFn.__name__, appId, executorId)] = getTraceFn(appId, executorId)
    from . import utilities
    utilities.dump(ans, f"{results_dir}/traces.pickle")