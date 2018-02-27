from mrjob.job import MRJob

# solving for alpha and beta
# population = area*alpha + beta

class MRStateLinearRegression(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        if (line[1] != '') & (line[0] != 'United States'): #to ignore lines at end of file
            y_population = int(line[4])
            x_area = int(line[3])
            x_squared = x_area**2
            x_times_y = x_area*y_population
            yield "", (x_area, y_population, x_squared, x_times_y, 1)

    def combiner(self, key, values):
        n = 0
        sum_x = 0
        sum_y = 0
        sum__x_squared = 0
        sum_x_times_y = 0

        for v in values:
            sum_x += v[0]
            sum_y += v[1]
            sum__x_squared += v[2]
            sum_x_times_y += v[3]
            n += v[4]

        yield "", (sum_x, sum_y, sum__x_squared, sum_x_times_y, n)


    def reducer(self, key, values):
        # slope: (  (n*sum(xi*yi)-(sum(yi)*sum(xi)))  /  (n*sum(xi^2))-(sum(xi)^2)  )
        # intercept: (  sum(yi)-slope*sum(xi)  /  n  )
        n = 0
        sum_x = 0
        sum_y = 0
        sum__x_squared = 0
        sum_x_times_y = 0

        for v in values:
            sum_x += v[0]
            sum_y += v[1]
            sum__x_squared += v[2]
            sum_x_times_y += v[3]
            n += v[4]

        slope = (n*sum_x_times_y - sum_y*sum_x)/(n*sum__x_squared - sum_x**2)
        intercept = (sum_y-slope*sum_x)/n

        yield "alpha", slope
        yield "beta", intercept


if __name__ == '__main__':
    MRStateLinearRegression.run()

