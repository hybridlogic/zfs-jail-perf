import sys, pickle, itertools

import matplotlib.pyplot as plt

def main(filename):
    with open(filename) as fObj:
        measurements = pickle.load(fObj)

    artist = []
    label = []
    styles = itertools.cycle(['-r', '-g', '2b', '2m', '+r', '+g', 'Hb', 'Hm', 'xr', 'xg'])
    for dataset in ['read', 'write', 'jail_read', 'jail_write', 'loaded_read', 'loaded_write', 'loaded_jail_read', 'loaded_jail_write']:
        [line] = plt.plot(measurements[dataset + '_measurements'], next(styles))
        line.label = dataset
        artist.append(line)
        label.append(dataset)
    plt.legend(artist, label)
    plt.ylabel('seconds')
    plt.show()


if __name__ == '__main__':
    main(sys.argv[1])
