import re
import sys
import pandas as pd
from itertools import chain
from IPython.display import display

DATASET_DIR = "/home/ybw/Research/PDD/result/new"
APP_LIST = ["wc", "pr", "cc", "ts", "km", "lr"]

def grepForMultiColumn(path, keyword, columns):
    file = open(path, "r")
    ans = dict()
    for i in columns:
        ans[i] = list()
    for line in file:
        if re.search(keyword, line) != None:
            sp = line.split()
            for i in columns:
                ans[i].append(sp[i])
    return ans

def grepForColumn(path, keyword, column):
    return grepForMultiColumn(path, keyword, [column])[column]

def processTime(in_time):
    time = [float(i) for i in in_time]
    if(len(time) == 3):
        return min(time[1], time[2])
    elif(len(time) == 2):
        print("WARNING: not complete run")
        return time[1]
    elif(len(time) == 1):
        print("WARNING: even more not complete run")
        return time[0]
    else:
        return None

def processSingleElement(elem):
    if len(elem) == 1:
        return elem[0]
    else:
        return None

EXECUTOR_PER_CORE = 7
RECORD_COLUMN_NC = "nc"
RECORD_COLUMN_BASELINE = "baseline"
RECORD_COLUMN_APP = "app"
RECORD_COLUMN_E2E = "e2e"
RECORD_COLUMN_COMPUTE = "compute"
RECORD_COLUMN_APPID = "appid"
RECORD_COLUMN_NUM_REPEATS = "numrepeats"
RECORD_COLUMNS = [RECORD_COLUMN_BASELINE, RECORD_COLUMN_NC, RECORD_COLUMN_APP, RECORD_COLUMN_E2E, RECORD_COLUMN_COMPUTE, RECORD_COLUMN_APPID, RECORD_COLUMN_NUM_REPEATS]

def transformRecord(scale, baseline, app, end_to_end_time, compute_time, appid, num_repeasts):
    return (baseline, scale * EXECUTOR_PER_CORE, app, end_to_end_time, compute_time, appid, num_repeasts)

def parseSparklet(dataset_dir = DATASET_DIR):
    for scale in [64]:
        for app in APP_LIST:
            try:
                path = f"{dataset_dir}/{app}_{scale}.log.stderr"
                sparklet_endtoend = grepForColumn(path, "Overall duration of main", 8)
                sparklet_compute = grepForColumn(path, "Compute duration:", 7)
                sparklet_appid = grepForColumn(path, "Submitting application", 6)
                yield transformRecord(scale, "sparklet", app, processTime(sparklet_endtoend), processTime(sparklet_compute), processSingleElement(sparklet_appid), len(sparklet_endtoend))
            except:
                print(f"WARNING: Unable to open {path}")

def parseSpark(dataset_dir = DATASET_DIR, baselines = ["rdd", "sfw", "sql", "rddblas"]):
    for baseline in baselines:
        for scale in [64]:
            for app in APP_LIST:
                try:
                    path = f"{dataset_dir}/spark/{app}_{baseline}_{scale}.log.stderr"
                    spark_endtoend = grepForColumn(path, "Overall duration of main", 8)
                    spark_compute = grepForColumn(path, "Compute duration:", 2)
                    spark_appid = grepForColumn(path, "Submitting application", 6)
                    yield transformRecord(scale, baseline, app, processTime(spark_endtoend), processTime(spark_compute), processSingleElement(spark_appid), len(spark_endtoend))
                except:
                    print(f"WARNING: Unable to open {path}")

def generateTable():
    return pd.DataFrame(chain(parseSparklet(DATASET_DIR), parseSpark(DATASET_DIR)), columns = RECORD_COLUMNS)

def generateStrawmanTable():
    dfN = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}")), columns = RECORD_COLUMNS)
    dfN["strawman"] = "N"
    dfA = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/strawmanA")), columns = RECORD_COLUMNS)
    dfA["strawman"] = "A"
    dfB = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/strawmanB")), columns = RECORD_COLUMNS)
    dfB["strawman"] = "B"
    return pd.concat([dfN, dfA, dfB])

def generateHDDvsSSDTable():
    dfN = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}")), columns = RECORD_COLUMNS)
    dfN["dev"] = "ssd"
    dfNS = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}", ['rdd'])), columns = RECORD_COLUMNS)
    dfNS["dev"] = "ssd"
    dfA = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/hdd")), columns = RECORD_COLUMNS)
    dfA["dev"] = "hdd"
    dfAS = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}/hdd", ["rdd"])), columns = RECORD_COLUMNS)
    dfAS["dev"] = "hdd"
    return pd.concat([dfN, dfA, dfNS, dfAS])

def generateFaultTolerance():
    d1 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}")), columns = RECORD_COLUMNS)
    d1['fault'] = "no"
    d1S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}", ['rdd'])), columns = RECORD_COLUMNS)
    d1S['fault'] = "no"
    d2 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/fault")), columns = RECORD_COLUMNS)
    d2['fault'] = 'yes'
    d2S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}/fault", ['rdd'])), columns = RECORD_COLUMNS)
    d2S['fault'] = "yes"
    return pd.concat([d1, d1S, d2, d2S])

def generateStragglerMitigation():
    d1 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}")), columns = RECORD_COLUMNS)
    d1['fault'] = "no"
    d1S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}", ['rdd'])), columns = RECORD_COLUMNS)
    d1S['fault'] = "no"
    d2 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/speculative")), columns = RECORD_COLUMNS)
    d2['fault'] = 'yes'
    d2S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}/speculative", ['rdd'])), columns = RECORD_COLUMNS)
    d2S['fault'] = "yes"
    return pd.concat([d1, d1S, d2, d2S])
    # return pd.concat([d1, d2])

def generateStragglerMitigationStaged():
    d1 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}")), columns = RECORD_COLUMNS)
    d1['fault'] = "no"
    d1S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}", ['rdd'])), columns = RECORD_COLUMNS)
    d1S['fault'] = "no"
    d2 = pd.DataFrame(chain(parseSparklet(f"{DATASET_DIR}/straggler")), columns = RECORD_COLUMNS)
    d2['fault'] = 'yes'
    d2S = pd.DataFrame(chain(parseSpark(f"{DATASET_DIR}/straggler", ['rdd'])), columns = RECORD_COLUMNS)
    d2S['fault'] = "yes"
    return pd.concat([d1, d1S, d2, d2S])
    # return pd.concat([d1, d2])

def displayAndSaveTable(table, path):
    display(table)
    from . import utilities
    utilities.dump(table, path)

if __name__ == "__main__":
    output_dir = "results"
    displayAndSaveTable(generateHDDvsSSDTable(), f"{output_dir}/hdd_vs_ssd.pickle")
    displayAndSaveTable(generateTable(), f"{output_dir}/primary.pickle")
    displayAndSaveTable(generateFaultTolerance(), f"{output_dir}/fault_tolerance.pickle")
    displayAndSaveTable(generateStragglerMitigationStaged(), f"{output_dir}/straggler_mitigation_staged.pickle")
    displayAndSaveTable(generateStragglerMitigation(), f"{output_dir}/straggler_mitigation.pickle")
    displayAndSaveTable(generateStrawmanTable(), f"{output_dir}/strawman_table.pickle")
