# ran before mapreduce jobs for  problem 4. to combine the files

# first run "4_combine_csv.py" <---
# second use "combined_states_electricity.csv" as input to the population and area linear model.py <----
# finally use the output alpha's, beta's and mean in this file

import pandas as pd

electricity = pd.read_csv("Electricity.csv", names=["state", "electricity"])
states = pd.read_csv("states.csv", names=["state", "abbrev", "2name", "area", "population"])
merged = electricity.merge(states, on='state')
merged.to_csv("combined_states_electricity.csv", index=False)
