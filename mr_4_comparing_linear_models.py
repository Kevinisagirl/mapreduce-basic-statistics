from mrjob.job import MRJob

# Electricity Price = Area * <alpha> + <beta>  Or
# Electricity Price = Population * <alpha> + <beta>

# This will find R^2 to compare the models
# first run "4_combine_csv.py"
# second use "combined_states_electricity.csv" as input to the population and area linear model.py
# finally use the output alpha's, beta's and mean in this file <---

area_alpha = 1.2114275255325954e-06
area_beta = 7.965724336268823
pop_alpha = 9.560444953758013e-08
pop_beta = 7.5894723517121445
mean_y = 8.055686274509803

# need to calculate y_hats for both models and send with yi


class MRElectricityAreaLinearRegression(MRJob):

    def mapper(self, _, line):
        # need y = electricity price
        # need x = area
        line = line.split(',')
        if line[0] != 'state':  # to ignore header
            y_electricity = float(line[1])
            x_area = float(line[4])
            x_population = float(line[5])
            y_hat_area = area_alpha*x_area + area_beta
            y_hat_population = pop_alpha*x_population + pop_beta
            yield "", (y_electricity, y_hat_area, y_hat_population)

    def reducer(self, key, values):
        # using R^2 to compare models
        # R^2 = 1 - (SSres/SStot)
        # ss_residual = sum((yi-yhat)^2)
        # ss_total = sum((yi-ybar)^2)
        area_ss_residual = 0
        pop_ss_residual = 0
        ss_total = 0
        for v in values:
            yi = v[0]
            y_hat_area = v[1]
            y_hat_pop = v[2]
            area_ss_residual += (yi-y_hat_area)**2
            pop_ss_residual += (yi-y_hat_pop)**2
            ss_total += (yi-mean_y)**2

        r_sq_area = 1 - (area_ss_residual/ss_total)
        r_sq_pop = 1 - (pop_ss_residual/ss_total)

        if r_sq_area > r_sq_pop:
            yield "R^2 using area as a predictor is larger:", r_sq_area
        else:
            yield "R^2 using population as a predictor is larger:", r_sq_pop


if __name__ == '__main__':
    MRElectricityAreaLinearRegression.run()

