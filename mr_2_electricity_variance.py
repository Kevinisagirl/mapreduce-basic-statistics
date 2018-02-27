from mrjob.job import MRJob

# Expected to run with: python3 mr_2_electricity_variance.py Electricity.csv


class MRElectricityVariance(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        state_electricity = float(line[1])
        yield "", (state_electricity, state_electricity**2, 1)

    def combiner(self, key, values):
        total = 0
        count = 0
        sum_squared_x = 0
        for v in values:
            total += v[0]
            count += v[2]
            sum_squared_x += v[1]
        yield "", (total, sum_squared_x, count)

    def reducer(self, key, values):
        # will be calculating variance using: (sum(x^2)/N)-mean^2
        total = 0
        count = 0
        sum_squared_x = 0
        for v in values:
            total += v[0]
            count += v[2]
            sum_squared_x += v[1]
        mean = total/count
        variance = (sum_squared_x/count)-mean**2
        yield "variance", variance


if __name__ == '__main__':
    MRElectricityVariance.run()

