# This script generates real-time streaming plot for word/tag count
# It needs support from lightning-viz (install: pip install lightning-python)
# To plot, start the streaming and lightning server and put the file in the intermediate result folder.

from lightning import Lightning
import numpy as np
import time
import os
import sys

def main():
    targets = sys.argv[1:]
    prefix = "./wordCountSplit/"
    folders = filter(lambda x: x[0]!= ".", os.listdir(prefix))
    count = []

    for folder in folders:
        count.append([0]*(len(sys.argv)-1))
        with open(prefix+folder+"/part-00000", "r") as f:
            for line in f.readlines():
                terms = line.split(" ")
                if terms[0] in targets:
                    i = targets.index(terms[0])
                    count[-1][i] += int(terms[-1])

    print(count)

    lgn = Lightning()

    series = np.array(count[:10]).reshape((len(sys.argv)-1, 10))
    print(series)
    
    viz = lgn.linestreaming(series, max_width=15, xaxis="Window No. (each window is 60 sec with 5 sec update interval", yaxis="Word Frequency")
    
    time.sleep(10)

    for c in count[10:]:
        viz.append(np.array(c).reshape((len(sys.argv)-1, 1)))
        time.sleep(0.3)

if __name__ == "__main__":
    main()