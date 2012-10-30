import sys, pickle

import matplotlib.pyplot as plt

def main(filename):
    with open(filename) as fObj:
        measurements = pickle.load(fObj)

    for dataset in ['read', 'write', 'jail_read', 'jail_write', 'loaded_read', 'loaded_write', 'loaded_jail_read', 'loaded_jail_write']:
        line = plt.plot(measurements[dataset + '_measurements'])
        line.label = dataset
    plt.ylabel('seconds')
    plt.show()
    

if __name__ == '__main__':
    main(sys.argv[1])
