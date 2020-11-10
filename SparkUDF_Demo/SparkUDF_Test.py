#! /usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import csv
import pandas as pd
import string
from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType, StructType, StructField


# return one value
def square_UDFtest1(x):
    return x**2


# return list value
def square_UDFtest2(x):
    return [float(val)**2 for val in x]


# return struct
def convert_ascii_test(number):
    return [number, string.ascii_letters[number]]


def transform_arguments(map_dic, seq_row):
    new_row = []
    for col_value in seq_row:
        new_row.append(int(map_dic[col_value]))
    return new_row


def transform_arguments_udf(map_dic):
    return udf(lambda y: transform_arguments(map_dic, y), returnType=ArrayType(IntegerType()))


if __name__ == '__main__':
    print("Hello PySpark.")
    test_pdDF = pd.DataFrame(
        data={
            'integers': [1, 2, 3],
            'floats': [-1.0, 0.5, 2.7],
            'integer_arrays': [[1, 2], [3, 4, 5], [7, 8, 9]]
        }
    )

    spark = SparkSession.builder.appName("Spark UDF Test App").getOrCreate()
    test_sparkDF = spark.createDataFrame(test_pdDF)
    test_sparkDF.show()
    # UDF register
    square_udf_int = udf(lambda z: square_UDFtest1(z), returnType=IntegerType())
    # UDF usage
    sparkUDF_df = test_sparkDF.select('integers', 'floats', square_udf_int('integers').alias('int_squared'),
                                      square_udf_int('floats').alias('floats_squared'))
    sparkUDF_df.show()

    # test return list
    square_udf_list = udf(lambda y: square_UDFtest2(y), returnType=ArrayType(FloatType()))
    sparkUDF_df1 = test_sparkDF.select('integer_arrays', square_udf_list('integer_arrays'))
    sparkUDF_df1.show()

    # test return struct
    array_schema = StructType([
        StructField('number', IntegerType(), nullable=False),
        StructField('letters', StringType(), nullable=False)
    ])
    convert_ascii_udf = udf(lambda x: convert_ascii_test(x), returnType=array_schema)
    ascii_df = test_sparkDF.select('integers', convert_ascii_udf('integers').alias('ascii_map'))
    ascii_df.show()

    # test arguments udf
    map_dic = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60, 7: 70, 8: 80, 9: 90}
    test_mapDF = test_sparkDF.withColumn("map_value", transform_arguments_udf(map_dic)(test_sparkDF["integer_arrays"]))
    test_mapDF.show()

