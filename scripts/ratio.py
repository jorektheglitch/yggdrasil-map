#!/usr/bin/env python2.7

import matplotlib.pyplot as plt
import json


with open("current", "r") as f:
    data = json.loads(f.read())

kinds = ["peers", "dht", "ratio"]
results = dict()
for kind in kinds:
    results[kind] = dict()

for k, v in data["yggnodes"].iteritems():
    for kind in kinds:
        if kind not in v:
            continue
        num = len(v[kind])
        if num not in results[kind]:
            results[kind][num] = 0
        results[kind][num] += 1
    # Added ratio part
    if "dht" in v and "peers" in v:
        ratio = float(len(v["dht"]))/len(v["peers"])
        if ratio not in results["ratio"]:
            results["ratio"][ratio] = 0
        results["ratio"][ratio] += 1

fig, axs = plt.subplots(1, len(kinds), sharey=True, tight_layout=True)
for kdx in xrange(len(kinds)):
    kind = kinds[kdx]
    bins = []
    for num, count in results[kind].iteritems():
        bins += [num]*count
    nbins = max(results[kind].keys())+1
    axs[kdx].set_title(kind)
    if kind == "ratio":
        nbins = "auto"
        axs[kdx].set_title("ratio (dht/peers)")
    axs[kdx].hist(bins, bins=nbins)
plt.savefig("fig.svg")
plt.yscale('log')
plt.savefig("fig-logy.svg")

print results
