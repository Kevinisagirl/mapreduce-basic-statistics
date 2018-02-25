from mrjob.job import MRJob

class MRStateArea(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        state_pop = line[4]
        if (state_pop != '') & (line[0] != 'United States'):
            yield "", (state_pop, 1)

    def reducer(self, key, values):
        largest = 0
        smallest = 10000000000
        total = 0
        count = 0
        for v in values:
            population = int(v[0])
            if population > largest:
                largest = population
            if population < smallest:
                smallest = population
            total += population
            count += 1
        mean = total/count
        yield "largest population", largest
        yield "smallest population", smallest
        yield "mean population", mean

if __name__ == '__main__':
    MRStateArea.run()

