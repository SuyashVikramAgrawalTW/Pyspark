import great_expectations as ge
import numpy as np
import pandas as pd

# Retriee your Data Context
context = ge.get_context()
print(type(context).__name__)

# Define the Data Source name
data_source_name = "GEPractice"