# Spark UDF Demo

Spark UDF测试Demo。

来源于：[How to Turn Python Functions into PySpark Functions (UDF)](https://changhsinlee.com/pyspark-udf/)

## 0x00 Introduction

Spark UDF使用需要注意函数的返回值类型，测试代码中分别测试了三种类型的返回值。

三种类型返回值分别是：

* 单个值返回（测试整型）；
* `List`类型值返回；
* `Struct`类型值返回；

注意：当返回值类型错误时，将会返回`null`。Spark不能像Python一样支持隐式类型转换。



在上述应用场景之外，还有一种场景：该UDF需要接收外部传入的参数，根据参数中的数据信息，对DataFrame数据进行处理。因此需要考虑如何进一步携带参数的UDF。

以参考[4]中的方案为主：即，使用UDF封装一个lamda表达式，而lamda表达式再封装一个函数。


## 0x01 Simple Test

Output:

```bash
$ python3 SparkUDF_Test.py

20/06/20 10:46:44 WARN Utils: Your hostname, yingMBP.local resolves to a loopback address: 127.0.0.1; using xxxx instead (on interface en0)
20/06/20 10:46:44 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
20/06/20 10:46:45 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+--------+------+--------------+
|integers|floats|integer_arrays|
+--------+------+--------------+
|       1|  -1.0|        [1, 2]|
|       2|   0.5|     [3, 4, 5]|
|       3|   2.7|     [7, 8, 9]|
+--------+------+--------------+

+--------+------+-----------+--------------+
|integers|floats|int_squared|floats_squared|
+--------+------+-----------+--------------+
|       1|  -1.0|          1|          null|
|       2|   0.5|          4|          null|
|       3|   2.7|          9|          null|
+--------+------+-----------+--------------+

+--------------+------------------------+
|integer_arrays|<lambda>(integer_arrays)|
+--------------+------------------------+
|        [1, 2]|              [1.0, 4.0]|
|     [3, 4, 5]|       [9.0, 16.0, 25.0]|
|     [7, 8, 9]|      [49.0, 64.0, 81.0]|
+--------------+------------------------+

+--------+---------+
|integers|ascii_map|
+--------+---------+
|       1|   [1, b]|
|       2|   [2, c]|
|       3|   [3, d]|
+--------+---------+

+--------+------+--------------+------------+
|integers|floats|integer_arrays|   map_value|
+--------+------+--------------+------------+
|       1|  -1.0|        [1, 2]|    [10, 20]|
|       2|   0.5|     [3, 4, 5]|[30, 40, 50]|
|       3|   2.7|     [7, 8, 9]|[70, 80, 90]|
+--------+------+--------------+------------+
```

## Reference

1. [pyspark.sql.functions.udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)
2. [How to Convert Python Functions into PySpark UDFs](https://walkenho.github.io/how-to-convert-python-functions-into-pyspark-UDFs/)
3. [How to Turn Python Functions into PySpark Functions (UDF)](https://changhsinlee.com/pyspark-udf/)
4. [Passing a data frame column and external list to udf under withColumn](https://stackoverflow.com/questions/37409857/passing-a-data-frame-column-and-external-list-to-udf-under-withcolumn)