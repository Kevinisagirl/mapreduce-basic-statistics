from mrjob.job import MRJob

# Electricity Price = Area * <alpha> + <beta>  Or
# Electricity Price = Population * <alpha> + <beta>

# This will find the alpha and beta using area as a predictor
# first run "4_combine_csv.py"
# second use "combined_states_electricity.csv" as input to the population and area linear model.py <----
# finally use the output alpha's, beta's and mean in this file


class MRElectricityAreaLinearRegression(MRJob):

    def mapper(self, _, line):
        # need y = electricity price
        # need x = area
        line = line.split(',')
        if line[0] != 'state':  # to ignore header
            y_electricity = float(line[1])
            x_area = float(line[4])
            yield "", (x_area, y_electricity)

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
        yield "alpha using area as a predictor", slope
        yield "beta using area as a predictor", intercept
        yield "mean of Y", sum_y/n

if __name__ == '__main__':
    MRElectricityAreaLinearRegression.run()

