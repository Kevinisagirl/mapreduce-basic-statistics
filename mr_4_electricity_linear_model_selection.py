from mrjob.job import MRJob

class MRStateLinearRegression(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        if (line[1] != '') & (line[0] != 'United States'): #to ignore lines at end of file
            y_population = int(line[4])
            x_area = int(line[3])
            yield "", (x_area, y_population)

    def reducer(self, key, values):
        # slope: (  (n*sum(xi*yi)-(sum(yi)*sum(xi)))  /  (n*sum(xi^2))-(sum(xi)^2)  )
        # intercept: (  sum(yi)-slope*sum(xi)  /  n  )
        n = 0
        sum_x = 0
        sum_y = 0
        sum_x_times_y = 0
        sum__x_squared = 0
        for v in values:
            x = v[0]
            y = v[1]
            n += 1
            sum_x += x
            sum_y += y
            sum_x_times_y += (x*y)
            sum__x_squared += x**2
        slope = (n*sum_x_times_y - sum_y*sum_x)/(n*sum__x_squared - sum_x**2)
        intercept = (sum_y-slope*sum_x)/n
        yield "alpha", slope
        yield "beta", intercept

if __name__ == '__main__':
    MRStateLinearRegression.run()

