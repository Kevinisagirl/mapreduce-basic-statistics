from mrjob.job import MRJob

class MRStateArea(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        state_area = line[3]
        if (state_area != '') & (line[0] != 'United States'):
            yield "", (state_area, 1)

    def reducer(self, key, values):
        largest = 0
        smallest = 10000000000
        total = 0
        count = 0
        for v in values:
            area = int(v[0])
            if area > largest:
                largest = area
            if area < smallest:
                smallest = area
            total += area
            count += 1
        mean = total/count
        yield "largest", largest
        yield "smallest", smallest
        yield "mean", mean

if __name__ == '__main__':
    MRStateArea.run()

