from mrjob.job import MRJob
import random

# this should produce a random sample of 100 colleges
# there is the possibility that the random number generator will miss a number entirely and the sample may be too small
# but with 1302 colleges, it's highly unlikely that will happen


class MRRandomSample(MRJob):

    def mapper(self, _, line):
        # need y = electricity price
        # need x = area
        line = line.split(',')
        if line[0] != 'College Name':  # to ignore header
            if line[2] == "1":
                yield random.randint(1, 50), "public: " + line[0]
            if line[2] == "2":
                yield random.randint(51, 100), "private: " + line[0]

    def reducer(self, key, values):
        for v in values:
            yield key, v  # sorter randomizes mapper output order so first value is random
            break


if __name__ == '__main__':
    MRRandomSample.run()

