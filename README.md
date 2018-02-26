# mapreduce-basic-statistics
The goal of this assignment is to perform some more complicated tasks using the MapReduce framework. A smaller (toy) data set will be used so to easily check the results by loading all the data into memory (e.g., using R).

Exercises
Write Map-Reduce applications that allow you to answer the following questions.

1. Calculate the largest, smallest, and average (mean) population for a state. Calculate the largest, smallest, and average (mean) area for a state.  
  
mr_1_state_area_stats.py & mr_1_state_population_stats.py  
  
2. Calculate the variance in electricity prices among the states.  
  
mr_2_electricity_variance.py  
  
3. Use linear regression to fit the following simple model  
  
Population = Area * <alpha> + <beta>  
  
That is, find <alpha> and <beta> that minimize the squared residuals when the state data is represented using this model.  
  
mr_3_state_linear_regression.py
  
4. Which of the following linear models is a better fit for the electricity data  
  
Electricity Price = Area * <alpha> + <beta>  Or   
Electricity Price = Population * <alpha> + <beta>  
  
5. Obtain a random sample of approximately 100 colleges in which each college is equally likely to appear in the sample.  
  
6. Obtain a random sample of approximately 100 colleges in which:  
  
a. Each public college is equally likely to be sampled and each private college is equally likely to be sampled.  
  
b. The sample is weighted so that in expectation there are the same number of public and private colleges in the sample.  

