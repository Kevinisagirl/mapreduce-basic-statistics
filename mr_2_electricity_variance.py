from mrjob.job import MRJob

class MRElectricityVariance(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        state_electricity = float(line[1])
        yield "", state_electricity

    def reducer(self, key, values):
        # will be calculating variance using: (sum(x^2)/N)-mean^2
        total = 0
        count = 0
        sum_squared_x = 0
        for v in values:
            total += v
            count += 1
            sum_squared_x += v**2
        mean = total/count
        variance = (sum_squared_x/count)-mean**2
        yield "variance", variance

if __name__ == '__main__':
    MRElectricityVariance.run()

