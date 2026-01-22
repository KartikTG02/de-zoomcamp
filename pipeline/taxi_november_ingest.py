#!/usr/bin/env python
# coding: utf-8

# In[35]:


import pandas as pd
from sqlalchemy import create_engine
import click
from tqdm.auto import tqdm
import requests
import json
import pyarrow.parquet as pq


# In[23]:


json_output = taxiDf.dtypes.apply(lambda x: x.name).to_json(indent=4)
print(json_output)


# In[25]:


engine = create_engine(f'postgresql://root:root@localhost:5432/ny_taxi')


# In[36]:


taxiDataUrl = 'green_tripdata_2025-11.parquet'
taxiDf = pq.ParquetFile(taxiDataUrl, dtypes=json_output)


# In[37]:


first = True

for batch in taxiDf.iter_batches(batch_size=100000):
    df_chunk = batch.to_pandas()
    if first:
        df_chunk.head(1).to_sql(
            name = 'yellow_taxi_nov_25',
            con=engine,
            if_exists='replace'
        )
        first = False

    df_chunk.to_sql(
        name = 'yellow_taxi_nov_25',
        con=engine,
        if_exists=
    )


# In[ ]:




