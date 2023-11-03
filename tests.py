
'''
author: Roberto Scalas 
date:   2023-10-17 09:37:39.648273
'''
import unittest 
import pandas as pd


data = pd.read_excel('data_canaryw_test_feed.xlsx')
# keep only half of the data
data = data.iloc[:int(len(data)/2),:]
# save the data

data.to_excel('data_canaryw_test_feed.xlsx', index=False)

