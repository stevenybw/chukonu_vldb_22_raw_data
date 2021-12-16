# %%
from numpy.core.shape_base import stack
from numpy.lib.function_base import disp
from scipy import stats
from sqlalchemy.engine import base
from sparklet_experiment import utilities
from matplotlib.ticker import ScalarFormatter, AutoMinorLocator
import pandas as pd
import pandasql as ps
from sparklet_experiment import data_thrill_husky
from IPython.display import display
import matplotlib.pyplot as plt
import os
from sparklet_experiment.benchmark_params import *
import matplotlib.ticker as ticker

results_dir = "results"
dirbase = 'fig/'
style_path = f"{os.path.dirname(os.path.realpath(__file__))}/plot_style.txt"
plt.style.use(style_path)

color_def = [
    '#f4b183',
    '#ffd966',
    '#c5e0b4',
    '#bdd7ee',
    "#8dd3c7",
    "#bebada",
    "#fb8072",
    "#80b1d3",
    "#fdb462",
    "#cccccc",
    "#fccde5",
    "#b3de69",
    "#ffd92f",
]

def extractRGB(txt):
    r, g, b = (int(txt[1:3], base=16), int(
        txt[3:5], base=16), int(txt[5:7], base=16))
    return r / 255.0, g / 255.0, b / 255.0


def recoverRGB(r, g, b):
    return f"#{int(r*255):x}{int(g*255):x}{int(b*255):x}"


hatch_def = [
    '',
    '//',
    '\\\\',
    'xx',
    '**',
    'oo',
    '..',
]

marker_def = [
    'o',
    'x',
    'D',
    '*',
    '+',
]

# line_color_def = ['#329932',
#                   '#ff6961',
#                   'b',
#                   '#6a3d9a',
#                   '#fb9a99',
#                   '#e31a1c',
#                   '#fdbf6f',
#                   '#ff7f00',
#                   '#cab2d6',
#                   '#6a3d9a',
#                   '#ffff99',
#                   '#b15928',
#                   '#67001f',
#                   '#b2182b',
#                   '#d6604d',
#                   '#f4a582',
#                   '#fddbc7',
#                   '#f7f7f7',
#                   '#d1e5f0',
#                   '#92c5de',
#                   '#4393c3',
#                   '#2166ac',
#                   '#053061']

line_color_def = ['b',
                  '#329932',
                  '#ff6961',
                  '#6a3d9a',
                  '#053061']

line_dash_style = [[1000, 0], [3, 1], [
    2, 1, 10, 1], [4, 1, 1, 1, 1, 1], [2, 1]]

# correct results
results_1 = utilities.load(f"{results_dir}/primary.pickle")
results_2 = data_thrill_husky.get()
results = pd.concat([results_1, results_2])
strawman_results = utilities.load(f"{results_dir}/strawman_table.pickle")
oursys = "Chukonu"

metric_traces = utilities.load(f"{results_dir}/traces.pickle")

def readMetrics(metricName):
    def doRead(appId, executorId):
        return metric_traces[(metricName, appId, executorId)]
    return doRead

all_baselines = {"sparklet": oursys, "rdd": "Spark", "rddblas": "Spark+BLAS",
                 "sfw": "sfw", "sql": "sql", "thrill": "Thrill", "husky": "Husky"}
inverseBaseline = dict()
for (i, app) in enumerate(["sparklet", "rdd", "rddblas", "sfw", "sql", "thrill", "husky"]):
    inverseBaseline[app] = i

all_applications = {
    "wc": "Word Count",
    "pr": "Page Rank",
    "cc": "Connected Components",
    "ts": "TeraSort",
    "lr": "Logistic Regression",
    "km": "K-Means",
}

app_order = ['WC', 'TS', 'PR', 'CC', 'KM', 'LR']

sfw_mapping = {
    "pr": "GraphX",
    "cc": "GraphX",
    "km": "MLlib",
    "lr": "MLlib",
}
sql_mapping = {
    "wc": "SparkSQL",
    "pr": "G.Frame",
    "cc": "G.Frame",
    "ts": "SparkSQL",
}


def translateBaseline(bl, app):
    if bl == 'sql':
        return sql_mapping[app]
    elif bl == 'sfw':
        return sfw_mapping[app]
    else:
        return all_baselines[bl]


def boxWithDataLabel(ax, x, y, colors, hatches, label, title, width=0.5):
    bars = ax.bar(x, y, width=width, color=colors, edgecolor='black')
    rect = bars[-1]
    for bar, ht in zip(bars, hatches):
        bar.set_hatch(ht)
    for bar, l in zip(bars, label):
        bar_height = bar.get_height()
        if bar.get_height() > ax.get_ylim()[1]:
            bar_height = ax.get_ylim()[1]
            posx = bar.get_x() + bar.get_width()/2
            d = 0.1*width
            ax.plot((posx-3*d, posx+3*d), (1-d, 1+d), color='k',
                    clip_on=False, transform=ax.get_xaxis_transform())
        ax.text(bar.get_x() + rect.get_width() /
                2.0, bar_height, l, ha='center', va='bottom', color='r', fontsize=16)
    ax.set_title(title)


def box(ax, x, y, colors, hatches, title):
    bars = ax.bar(x, y, color=colors, edgecolor='black')
    rect = bars[-1]
    for bar, ht in zip(bars, hatches):
        bar.set_hatch(ht)
    ax.set_title(title)


def changeColor(c, deltah=0.0, deltas=0.0, deltav=0.0):
    import colorsys
    r, g, b = extractRGB(c)
    h, s, v = colorsys.rgb_to_hsv(r, g, b)
    h += deltah
    s += deltas
    v += deltav
    if (h > 1.0):
        h = 1.0
    if (h < 0.0):
        h = 0.0
    if (s > 1.0):
        s = 1.0
    if (s < 0.0):
        s = 0.0
    if (v > 1.0):
        v = 1.0
    if (v < 0.0):
        v = 0.0
    nr, ng, nb = colorsys.hsv_to_rgb(h, s, v)
    return recoverRGB(nr, ng, nb)


def drawOneEndtoend(ax, app):
    import math
    print(f"Result of {app}")
    T = ps.sqldf(
        f"SELECT baseline, e2e, compute FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL;")
    x = list(T.baseline)
    print(x)
    xshow = [translateBaseline(i, app) for i in x]
    y = list(T.e2e)
    # if app == 'ts':
    #     x.append('husky')
    #     xshow.append('Husky')
    #     y.append(0)
    # elif app == 'cc':
    #     x.insert(len(x)-1, 'thrill')
    #     xshow.insert(len(xshow)-1, 'Thrill')
    #     y.insert(len(y)-1, 0)
    xcolor = [color_def[inverseBaseline[b]] for b in x]
    xhatch = [hatch_def[inverseBaseline[b]] for b in x]
    assert(x[0] == 'sparklet')
    xshow.append(xshow[0])
    y.append(y[0])
    xshow.pop(0)
    y.pop(0)
    normalized_y = [v / y[-1] for v in y]
    if app == 'wc':
        ax.set_ylim(0, 60)
    if app == 'km':
        ax.set_ylim(0, 200)
    if app == 'cc':
        ax.set_ylim(0, 400)
    if app == 'lr':
        ax.set_ylim(0, 300)
    boxWithDataLabel(ax, xshow, y, xcolor, [None for x in xshow], [
                     ('%.2f×' % ny) if ny > 0 else 'N/A' for ny in normalized_y], all_applications[app], width=0.13*len(xshow))
    # if app in {"pr", "cc", "km", "lr"}:
    #     box(ax, xshow, yc, xcolor, xhatch, all_applications[app])


def drawOneCompute(ax, app):
    print(f"Result of {app}")
    T = ps.sqldf(
        f"SELECT baseline, compute FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet', 'rdd', 'sfw', 'sql');")
    x = T.baseline
    xshow = [translateBaseline(i, app) for i in x]
    xcolor = [color_def[inverseBaseline[b]] for b in x]
    xhatch = [hatch_def[inverseBaseline[b]] for b in x]
    y = T.compute
    assert(x[0] == 'sparklet')
    normalized_y = [v / y[0] for v in y]
    boxWithDataLabel(ax, xshow, y, xcolor, xhatch, [
                     '%.2fX' % ny for ny in normalized_y], all_applications[app])

# Draw End-to-end

def drawCompute():
    def createAxes(row, col):
        h = 1.0 / row
        w = 1.0 / col
        hratio = 0.7
        wratio = 0.9
        return [[fig.add_axes([j*w, i*h, w * wratio, h * hratio]) for j in range(col)] for i in range(row)]
    fig = plt.figure(figsize=(12, 4))
    ax = fig.add_axes([0, 0, 0.9, 0.9])
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top=False,
                   bottom=False, left=False, right=False)
    ax.set_ylabel('Exec. time (s)')
    axes = createAxes(2, 2)
    for (i, app) in enumerate(["pr", "cc", "km", "lr"]):
        ax = axes[i // 2][i % 2]
        drawOneCompute(ax, app)
    plt.show()

def drawTracesFromAppids(ax, app, nameToAppid, executorId, getTraceFn):
    maxx = 0
    maxy = 0
    for i, baseline in enumerate(nameToAppid):
        if baseline == 'sparklet':
            alphaval = 1.0
            lw = 2.0
        else:
            alphaval = 0.6
            lw = 1.8
        appId = nameToAppid[baseline]
        x, y, unit = getTraceFn(appId, executorId)
        ax.plot(x, y, linewidth=lw,
                color=line_color_def[inverseBaseline[baseline]], linestyle='-', dashes=line_dash_style[inverseBaseline[baseline]], label=translateBaseline(baseline, app), alpha=alphaval)
        maxx = max(maxx, max(x))
        maxy = max(maxy, max(y))
        print(f"App {app} Baseline {baseline} MaxY = {max(y)}")
    ax.set_xlim((0, maxx))
    ax.set_ylim((0, maxy*1.2))
    ax.set_title(all_applications[app])
    ax.legend(frameon=False, loc='upper left', ncol=5, handlelength=4)


def drawTraces(ax, app, getTraceFn):
    appIds = ps.sqldf(
        f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet', 'rdd', 'rddblas', 'sfw', 'sql');").set_index("baseline").to_dict()["appid"]
    display(appIds)
    drawTracesFromAppids(ax, app, appIds, executorId=10, getTraceFn=getTraceFn)


def summarizeMaxRSS():
    def do():
        for app in all_applications:
            appIds = ps.sqldf(f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet', 'rdd', 'rddblas', 'sfw', 'sql');").set_index(
                "baseline").to_dict()["appid"]
            for i, baseline in enumerate(appIds):
                appId = appIds[baseline]
                ts, vs, unit = readMetrics("readOverallRSSMetrics")(appId, 0)
                yield(app, baseline, max(vs))
    return pd.DataFrame(do(), columns=["app", "baseline", "maxrss"])


def summarizeCPUUtil():
    def do():
        for app in all_applications:
            appIds = ps.sqldf(f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet', 'rdd', 'rddblas', 'sfw', 'sql');").set_index(
                "baseline").to_dict()["appid"]
            for i, baseline in enumerate(appIds):
                appId = appIds[baseline]
                ts, vs, unit = readMetrics("readOverallCPUMetrics")(appId, 0)
                yield(app, baseline, sum(vs) / len(vs))
    return pd.DataFrame(do(), columns=["app", "baseline", "cores"])


def drawEndtoend():
    def createAxes(row, col):
        h = 1.0 / row
        w = 1.0 / col
        hratio = 0.7
        wratio = 0.9
        return [[fig.add_axes([j*w, i*h, w * wratio, h * hratio]) for j in range(col)] for i in range(row)]
    fig = plt.figure(figsize=(16, 4))
    ax = fig.add_axes([0, 0, 0.9, 0.9])
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top=False,
                   bottom=False, left=False, right=False)
    ax.set_ylabel('Exec. time (s)', fontsize=16)
    axes = createAxes(2, 3)
    for (i, app) in enumerate(["ts", "cc", "lr", "wc", "pr", "km"]):
        ax = axes[i // 3][i % 3]
        drawOneEndtoend(ax, app)
    fig.savefig(dirbase+'endtoend.pdf', bbox_inches='tight')
    plt.show()

def drawMemoryUsages():
    def createAxes(row, col):
        h = 1.0 / row
        w = 1.0 / col
        hratio = 0.7
        wratio = 0.9
        return [[fig.add_axes([j*w, i*h, w * wratio, h * hratio]) for j in range(col)] for i in range(row)]
    fig = plt.figure(figsize=(16, 4))
    ax = fig.add_axes([0, 0, 0.9, 0.9])
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top=False,
                   bottom=False, left=False, right=False)
    ax.set_xlabel('Timestamp (sec)')
    ax.set_ylabel('Memory Usage (GB)')
    axes = createAxes(2, 3)
    for (i, app) in enumerate(["ts", "cc", "lr", "wc", "pr", "km"]):
        ax = axes[i // 3][i % 3]
        drawTraces(ax, app, readMetrics("readOverallRSSMetrics"))
    fig.savefig(dirbase+'memory_usages.pdf', bbox_inches='tight')
    plt.show()


def drawCPUUsages():
    def createAxes(row, col):
        h = 1.0 / row
        w = 1.0 / col
        hratio = 0.7
        wratio = 0.9
        return [[fig.add_axes([j*w, i*h, w * wratio, h * hratio]) for j in range(col)] for i in range(row)]
    fig = plt.figure(figsize=(16, 4))
    ax = fig.add_axes([0, 0, 0.9, 0.9])
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top=False,
                   bottom=False, left=False, right=False)
    ax.set_xlabel('Timestamp (sec)')
    ax.set_ylabel('Memory Usage (GB)')
    axes = createAxes(2, 3)
    for (i, app) in enumerate(["ts", "cc", "lr", "wc", "pr", "km"]):
        ax = axes[i // 3][i % 3]
        appIds = ps.sqldf(
            f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet');").set_index("baseline").to_dict()["appid"]
        display(appIds)
        drawTracesFromAppids(ax, app, appIds, executorId=10,
                             getTraceFn=readMetrics("readOverallCPUMetrics"))
    # fig.savefig(dirbase+'memory_usages.pdf', bbox_inches='tight')
    plt.show()


def drawBottleneckAnalysis():
    def drawTrace(ax, app, color, label, loc, dashes, appId, executorId, getTraceFn, alphaval=1.0, lw=2.0, maxylim=None):
        maxx = 0
        maxy = 0
        baseline = 'sparklet'
        x, y, unit = getTraceFn(appId, executorId)
        ax.plot(x, y, linewidth=lw,
                color=color, linestyle='-', dashes=dashes, label=label, alpha=alphaval)
        maxx = max(maxx, max(x))
        maxy = max(maxy, max(y))
        ax.set_xlim((0, maxx))
        if maxylim:
            maxy = maxylim
        else:
            maxy = maxy*1.2
        ax.set_ylim((0, maxy))
        ax.set_title(all_applications[app])
        ax.legend(frameon=False, loc=loc, ncol=5, handlelength=4)

    def createAxes(row, col):
        h = 1.0 / row
        w = 1.0 / col * 0.93
        hratio = 0.7
        wratio = 0.8
        return [[fig.add_axes([j*w, i*h, w * wratio, h * hratio]) for j in range(col)] for i in range(row)]
    fig = plt.figure(figsize=(16, 4))
    # Left axis
    ax = fig.add_axes([0, 0, 0.9, 0.9])

    def hideSpines(ax):
        ax.spines['top'].set_color('none')
        ax.spines['bottom'].set_color('none')
        ax.spines['left'].set_color('none')
        ax.spines['right'].set_color('none')
        ax.tick_params(labelcolor='w', top=False,
                       bottom=False, left=False, right=False)
    hideSpines(ax)
    ax.set_xlabel('Timestamp (sec)')
    ax.set_ylabel('CPU Utilization (Cores)')
    # Right axis
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    hideSpines(ax)
    ax.set_ylabel("Shuffle Read Bandwidth (GB/s)")
    ax.yaxis.set_label_position("right")
    axes = createAxes(2, 3)
    for (i, app) in enumerate(["ts", "cc", "lr", "wc", "pr", "km"]):
        ax = axes[i // 3][i % 3]
        appIds = ps.sqldf(
            f"SELECT baseline, appid FROM results WHERE app = '{app}' AND nc = 448 AND e2e IS NOT NULL AND baseline IN ('sparklet');").set_index("baseline").to_dict()["appid"]
        display(appIds)
        ax1 = ax.twinx()
        drawTrace(ax, app, "#329932", "CPU Util. (Cores)", 'upper left', line_dash_style[0],
                  appIds["sparklet"], 0, getTraceFn=readMetrics("readOverallCPUMetrics"), alphaval=0.8, lw=2.0)
        drawTrace(ax1, app, "#ff6961", "Shuffle Read (GB/s)", 'upper right', line_dash_style[1],
                  appIds["sparklet"], 0, getTraceFn=readMetrics("readOverallShuffleReadMetrics"), alphaval=0.8, lw=2.0, maxylim=15)
        ax1.spines['right'].set_visible(True)
    fig.savefig(dirbase+'bottleneck_analysis.pdf', bbox_inches='tight')
    plt.show()


def drawStrawmanCompare():
    strawmans = ps.sqldf("SELECT UPPER(app) as app, strawman, e2e FROM strawman_results").set_index(
        ["app", "strawman"]).unstack()["e2e"]
    fig, ax = plt.subplots(figsize=(6, 2))
    strawmans[oursys] = 1.0
    strawmans['Strawman A'] = strawmans['A'] / strawmans['N']
    strawmans['Strawman B'] = strawmans['B'] / strawmans['N']
    strawmans.columns.name = None
    width = 0.8
    strawmans.reset_index()[['app', oursys, "Strawman A", "Strawman B"]].set_index("app").reindex(app_order).plot.bar(
        ax=ax, colormap="Paired", width=width, rot=0)
    ax.set_xlabel(None)
    ax.set_ylabel(f"Slowdown to {oursys}")
    ax.set_ylim(0, 15)
    bars = ax.patches
    rect = bars[-1]
    print("A:", stats.gmean(strawmans['A'].dropna()))
    print("B:", stats.gmean(strawmans['B'].dropna()))
    # for bar, ht in zip(bars, hatch_def):
    #     bar.set_hatch(ht)
    for bar in bars:
        bar_height = bar.get_height()
        if bar.get_height() > ax.get_ylim()[1]:
            bar_height = ax.get_ylim()[1]
            posx = bar.get_x() + bar.get_width()/2
            d = 0.03*width
            ax.plot((posx-3*d, posx+3*d), (1-d, 1+d), color='k',
                    clip_on=False, transform=ax.get_xaxis_transform())
        if bar.get_height() != 0.0:
            ax.text(bar.get_x() + rect.get_width() /
                    2.0, bar_height, '%.1fx' % bar.get_height(), ha='center', va='bottom', color='r')
        else:
            ax.text(bar.get_x() + rect.get_width() /
                    2.0, bar_height, 'OOM', ha='center', va='bottom', color='r', fontsize=6)
    fig.savefig(dirbase+'strawman_compare.pdf', bbox_inches='tight')
    plt.show()


def reportMaxRSSTable():
    maxrss = summarizeMaxRSS()
    return ps.sqldf("SELECT * FROM maxrss WHERE baseline IN ('sparklet', 'rdd')").set_index(["app", "baseline"]).unstack()


def compareHDDSSD(path):
    fig, ax = plt.subplots(figsize=(6, 2))
    result_hdd_vs_hdd = utilities.load(path)
    T = ps.sqldf("SELECT baseline, app, dev, e2e FROM result_hdd_vs_hdd").set_index(
        ["app", "baseline", "dev"]).unstack()["e2e"]
    T["Speedup from SSD"] = T.hdd / T.ssd
    T = T["Speedup from SSD"].unstack()
    T.index = T.index.str.upper()
    T = T.reindex(app_order)
    T.columns.name = None
    T.plot.bar(ax=ax, rot=0).legend(["Spark", oursys])
    ax.set_xlabel(None)
    ax.set_ylabel("Slow-down from HDD")
    ax.hlines(1.0, -2.0, 7.0, color=color_def[0], linestyles='dotted')
    fig.savefig(dirbase+'speedup_from_ssd.pdf', bbox_inches='tight')
    plt.show()


def drawFaultTolerance(path):
    ftresult = utilities.load(path)
    fig, ax = plt.subplots(figsize=(6, 2))
    T1 = ps.sqldf("SELECT app, fault, e2e FROM ftresult WHERE baseline='sparklet'").set_index(
        ["app", "fault"]).unstack()["e2e"]
    T2 = ps.sqldf("SELECT app, fault, e2e FROM ftresult WHERE baseline='rdd'").set_index(
        ["app", "fault"]).unstack()["e2e"]
    T1[oursys] = (T1["yes"]/T1["no"])
    T1["Spark"] = (T2["yes"]/T2["no"])
    print(f"""Chukonu overhead: {stats.gmean(T1["yes"]/T1["no"])}""")
    print(f"""Spark overhead: {stats.gmean(T2["yes"]/T2["no"])}""")
    T = T1[["Spark", oursys]]
    T.index = T.index.str.upper()
    T = T.reindex(app_order)
    T.columns.name = None
    T.plot.bar(ax=ax, rot=0)
    ax.hlines(1.0, -2.0, 7.0, color=color_def[0], linestyles='dotted')
    ax.set_xlabel(None)
    ax.set_ylabel("Fault / Non-fault Ratio")
    fig.savefig(dirbase+'fault_tolerance_overhead.pdf', bbox_inches='tight')
    plt.show()


def drawStragglerMitigation(path):
    ftresult = utilities.load(path)
    fig, ax = plt.subplots(figsize=(6, 2))
    T1 = ps.sqldf("SELECT app, fault, e2e FROM ftresult WHERE baseline='sparklet'").set_index(
        ["app", "fault"]).unstack()["e2e"]
    T2 = ps.sqldf("SELECT app, fault, e2e FROM ftresult WHERE baseline='rdd'").set_index(
        ["app", "fault"]).unstack()["e2e"]
    T1[oursys] = (T1["yes"]/T1["no"])
    T1["Spark"] = (T2["yes"]/T2["no"])
    print(f"""Chukonu overhead: {stats.gmean(T1["yes"]/T1["no"])}""")
    print(f"""Spark overhead: {stats.gmean(T2["yes"]/T2["no"])}""")
    T = T1[["Spark", oursys]]
    T.index = T.index.str.upper()
    T = T.reindex(app_order)
    T.columns.name = None
    T.plot.bar(ax=ax, rot=0)
    ax.hlines(1.0, -2.0, 7.0, color=color_def[0], linestyles='dotted')
    ax.set_xlabel(None)
    ax.set_ylabel("Misconfig / Normal")
    ax.legend(loc="best")
    fig.savefig(dirbase+'straggler_mitigation_overhead.pdf',
                bbox_inches='tight')
    plt.show()

def lighten_color(color, amount=0.3):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])

def drawCompileTime():
    e2e_time = ps.sqldf(f"SELECT UPPER(app) App, baseline, e2e FROM results WHERE baseline in ('rdd', 'sparklet', 'thrill', 'husky') AND nc = 448 AND e2e IS NOT NULL;").sort_values(["App", "baseline"]).set_index(["App", "baseline"]).unstack()["e2e"]
    compile_time = data_thrill_husky.getCompileTime()
    T = compile_time.set_index("App")
    T.index = T.index.str.upper()
    T = T.reindex(app_order)
    T = e2e_time.join(T).reindex(app_order)
    fig, ax = plt.subplots(figsize=(6, 1.5))
    WIDTH = 0.1
    W = 1
    T.plot.bar(y = ["Spark", "rdd"], stacked=True, ax=ax, rot=0, label=["Spark-C", "Spark-E"], width=WIDTH, position=2*W, color=["#f4b183", lighten_color("#f4b183")])
    T.plot.bar(y = ["Chukonu", "sparklet"], stacked=True, ax=ax, rot=0, label=["Chukonu-C", "Chukonu-E"], width=WIDTH, position=W, color=["#c5e0b4", lighten_color("#c5e0b4")])
    T.plot.bar(y = ["Thrill", "thrill"], stacked=True, ax=ax, rot=0, label=["Thrill-C", "Thrill-E"], width=WIDTH, position=0, color=["#ffd966", lighten_color("#ffd966")])
    T.plot.bar(y = ["Husky", "husky"], stacked=True, ax=ax, rot=0, label=["Husky-C", "Husky-E"], width=WIDTH, position=-W, color=["#bdd7ee", lighten_color("#bdd7ee")])
    T["increased compilation time"] = T["Chukonu"] - T['Spark']
    T["decreased execution time"] = T["rdd"] - T['sparklet']
    T["C-Lat"] = T["Chukonu"] + T['sparklet']
    T["S-Lat"] = T["Spark"] + T['rdd']
    T["LatSpeedup"] = T["S-Lat"] / T["C-Lat"]
    display(T)
    print(f"Geometric mean of speedup in Lat = {stats.gmean(T['LatSpeedup'])}")
    print(f"Average compilation time Chukonu (s) = {T['Chukonu'].mean()}")
    ax.set_ylim(0, 200)
    ax.set_xlim(-0.5, 5.5)
    ax.set_xlabel(None)
    ax.set_ylabel("Time (s)")
    for bars in ax.containers:
        for bar in bars:
            max_height = ax.get_ylim()[1]
            if bar.get_height() > max_height:
                posx = bar.get_x() + bar.get_width() / 2
                d = bar.get_width() / 3
                ax.plot((posx - d, posx + d), (max_height - 4, max_height + 4), color='k', clip_on=False)
    ax.legend(ncol=4, loc=(0, 1.05), framealpha=0.5)
    fig.savefig(dirbase+'compile_time.pdf', bbox_inches='tight')
    plt.show()


def drawLinesOfCode():
    lines_of_code = data_thrill_husky.getLinesOfCode()
    fig, ax = plt.subplots(figsize=(6, 2))
    T = lines_of_code.set_index("App")
    T.index = T.index.str.upper()
    T = T.reindex(app_order)
    T.plot.bar(ax=ax, rot=0)
    ax.set_xlabel(None)
    ax.set_ylabel("Lines of Code")
    ax.set_ylim(0, 250)
    ax.legend(ncol=2)
    fig.savefig(dirbase+'lines_of_code.pdf', bbox_inches='tight')
    plt.show()

# def drawStraggler():
#     import io
#     TABLE_STRAGGLERS = f"""Baseline	Original	Straggler	+Speculation
# {oursys}	8.354	16.737	11.489926
# Spark	26.588	65.909	44.749
# Husky	31.7	63.579	
# Thrill	25.8	29.374	"""
#     T = pd.read_csv(io.StringIO(TABLE_STRAGGLERS), sep='\t').set_index("Baseline")
#     fig, ax = plt.subplots(figsize=(6, 2))
#     T.plot.bar(ax=ax, rot=0)
#     ax.set_xlabel(None)
#     ax.set_ylabel("End-to-end time (s)")
#     fig.savefig(dirbase+'straggler_mitigation_overhead.pdf', bbox_inches='tight')
#     plt.show()

def reportResults():
    def reportSeries(name, series):
        s1 = series.min()
        s2 = stats.gmean(series)
        s3 = series.max()
        display(series)
        print(f"{name} min = {s1}  gmean = {s2}  max = {s3}")

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'rdd')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_spark", T['rdd'] / T['sparklet'])

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('rdd', 'thrill') AND app in ('wc', 'pr', 'km', 'ts')").set_index(["app", "baseline"]).unstack()["e2e"]
    T['speedup'] = T['rdd'] / T['thrill']
    print("Thrill's speedup to Apache Spark")
    display(T)
    print(f"Thrill's speedup to Apache Spark geomean = {stats.gmean(T.speedup)}")

    T1 = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'thrill')").set_index(["app", "baseline"]).unstack()["e2e"]
    T2 = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'husky')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_cmr_to_thrill", (T1['thrill'] / T1['sparklet']).dropna())
    reportSeries("overall_speedup_cmr_to_husky", (T2['husky'] / T2['sparklet']).dropna())

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'sql') AND app IN ('wc', 'ts')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_sql_unstuctured", T['sql'] / T['sparklet'])

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'sql') AND app IN ('pr', 'cc')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_graphframe_unstuctured", T['sql'] / T['sparklet'])

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'sfw')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_sfw", (T['sfw'] / T['sparklet']).dropna())

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'rddblas')").set_index(["app", "baseline"]).unstack()["e2e"]
    reportSeries("overall_speedup_rddblas", (T['rddblas'] / T['sparklet']).dropna())

    T = ps.sqldf("SELECT app, baseline, e2e FROM results WHERE baseline IN ('sparklet', 'sfw', 'sql', 'rddblas')").set_index(["app", "baseline"]).unstack()["e2e"]
    T1 = (T['rddblas'] / T['sparklet']).dropna()
    T2 = (T['sfw'] / T['sparklet']).dropna()
    T3 = (T['sql'] / T['sparklet']).dropna()
    reportSeries("overall_speedup_all_domain_specific_frameworks", pd.concat([T1, T2, T3]))

def readTpcdsTime():
    ds = pd.read_csv(open(f"{results_dir}/tpcds_execution_time.txt", "r"), sep=' ')
    ds = ds[(ds.ITER == 1) | (ds.ITER == 2)] # Select 2nd and 3rd results
    time = ds[["QID", "SF", "ITER", "SYS", "TIME"]].sort_values(["QID", "SF", "ITER", "SYS"]).set_index(["QID", "SF", "ITER", "SYS"]).unstack()["TIME"]
    time["cmr_ans_display_time"] = time["cmr_ans_display_time"].fillna(0.0)
    time["cmr_time"] = time["cmr_time"] - time["cmr_ans_display_time"]
    del time["cmr_ans_display_time"]
    time = time.groupby(["QID", "SF"]).agg("mean") # Get the average of 2nd and 3rd results
    time["speedup"] = time["spark_time"] / time["cmr_time"]
    time_1000 = time.reset_index()
    time_1000 = time_1000[time_1000.SF == 1000]
    return time_1000

def drawTpcdsSpeedup():
    time_1000 = readTpcdsTime()
    print(f"Valid results = {len(time_1000)}")
    print(f"Failed results = {set(range(0, 103)) - set((x for x in time_1000['QID']))}")
    sum_cmr = sum(time_1000.cmr_time)
    sum_spark = sum(time_1000.spark_time)
    print(f"Sum CMR = {sum_cmr}")
    print(f"Sum Spark = {sum_spark}")
    print(f"Overall speedup = {sum_spark / sum_cmr}")
    slower_results = time_1000[time_1000.speedup < 1.0]
    print(f"Slower results = {len(slower_results)}")
    print(f"Slower geomean slowdown = {1.0 / stats.gmean(slower_results.speedup)}")
    print(f"Short duration (Spark<=1s) = {len(time_1000[time_1000.spark_time <= 1])}")
    print(f"Short duration (Spark<=10s) = {len(time_1000[time_1000.spark_time <= 10])}")
    print(f"Short duration (Spark<=33s) = {len(time_1000[time_1000.spark_time <= 33])}")
    display(slower_results.sort_values("speedup"))
    spark_top_5_time = sorted([x for x in time_1000.spark_time])[-5:]
    print(f"Spark Top 5 = {sum(spark_top_5_time)} sec ({100.0 * sum(spark_top_5_time) / sum_spark} %)")
    fig, ax = plt.subplots(figsize=(6, 2))
    time_1000["QIDp1"] = time_1000["QID"] + 1
    time_1000[["QIDp1", "speedup"]].plot.bar(x="QIDp1", y="speedup", legend=False, color="#b15928", edgecolor = "none", ax = ax)
    ax.set_ylim(0, 6)
    ax.set_xlabel("TPC-DS Query ID")
    ax.set_ylabel("Chukonu Speedup")
    ax.axhline(1.0, -1, 1, linewidth = 1, color = "b", linestyle = "dotted")
    ax.xaxis.set_major_locator(ticker.MultipleLocator(5.0))
    fig.savefig(dirbase+'tpcds_speedup.pdf', bbox_inches='tight')
    plt.show()


# %%
drawTpcdsSpeedup()

# %%
reportResults()

# %%
drawEndtoend()

# %%
compareHDDSSD(f"{results_dir}/hdd_vs_ssd.pickle")

# %%
drawMemoryUsages()

# %%
drawStrawmanCompare()

# %%
drawBottleneckAnalysis()

# %%
drawFaultTolerance(f"{results_dir}/fault_tolerance.pickle")

# %%
drawCompileTime()

# %%
drawLinesOfCode()

# %%
# drawCPUUsages()

# %%
# # reportMaxRSSTable()

# %%
drawStragglerMitigation(f"{results_dir}/straggler_mitigation.pickle")
