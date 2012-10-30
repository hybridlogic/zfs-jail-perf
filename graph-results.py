import sys, pickle

import matplotlib.pyplot as plt
from matplotlib import legend

def main(filename):
    with open(filename) as fObj:
        measurements = pickle.load(fObj)

    artist = []
    label = []
    for dataset in ['read', 'write', 'jail_read', 'jail_write', 'loaded_read', 'loaded_write', 'loaded_jail_read', 'loaded_jail_write']:
        [line] = plt.plot(measurements[dataset + '_measurements'])
        line.label = dataset
        artist.append(line)
        label.append(dataset)
    legend(artist, label)
    plt.ylabel('seconds')
    plt.show()


if __name__ == '__main__':
    main(sys.argv[1])
