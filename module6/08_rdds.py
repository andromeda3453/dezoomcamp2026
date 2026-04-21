#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# In[2]:


df_green = spark.read.parquet('data/pq/green/*/*')

# ```
# SELECT 
#     date_trunc('hour', lpep_pickup_datetime) AS hour, 
#     PULocationID AS zone,
# 
#     SUM(total_amount) AS amount,
#     COUNT(1) AS number_records
# FROM
#     green
# WHERE
#     lpep_pickup_datetime >= '2020-01-01 00:00:00'
# GROUP BY
#     1, 2
# ```

# In[8]:


rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd


# In[13]:


from datetime import datetime


# In[19]:


start = datetime(year=2020, month=1, day=1)

def filter_outliers(row):
    return row.lpep_pickup_datetime >= start


# In[21]:


rows = rdd.take(10)
row = rows[0]


# In[29]:


row


# In[31]:


def prepare_for_grouping(row): 
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)

    amount = row.total_amount
    count = 1
    value = (amount, count)

    return (key, value)


# In[34]:


def calculate_revenue(left_value, right_value):
    left_amount, left_count = left_value
    right_amount, right_count = right_value

    output_amount = left_amount + right_amount
    output_count = left_count + right_count

    return (output_amount, output_count)


# In[39]:


from collections import namedtuple


# In[40]:


RevenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])


# In[41]:


def unwrap(row):
    return RevenueRow(
        hour=row[0][0], 
        zone=row[0][1],
        revenue=row[1][0],
        count=row[1][1]
    )


# In[45]:


from pyspark.sql import types


# In[46]:


result_schema = types.StructType([
    types.StructField('hour', types.TimestampType(), True),
    types.StructField('zone', types.IntegerType(), True),
    types.StructField('revenue', types.DoubleType(), True),
    types.StructField('count', types.IntegerType(), True)
])


# In[47]:


df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .map(unwrap) \
    .toDF(result_schema) 


# In[50]:


df_result.write.parquet('tmp/green-revenue')


# In[55]:


columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
    .select(columns) \
    .rdd


# In[67]:


import pandas as pd


# In[68]:


rows = duration_rdd.take(10)


# In[81]:


df = pd.DataFrame(rows, columns=columns)


# In[74]:


columns


# In[76]:


#model = ...

def model_predict(df):
#     y_pred = model.predict(df)
    y_pred = df.trip_distance * 5
    return y_pred


# In[98]:


def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_predict(df)
    df['predicted_duration'] = predictions

    for row in df.itertuples():
        yield row


# In[102]:


df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch)\
    .toDF() \
    .drop('Index')


# In[104]:


df_predicts.select('predicted_duration').show()


# In[ ]:




