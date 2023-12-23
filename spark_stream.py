import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType

time_to_reset = 10 # Повторного просмотра папки на новые файлы

koef_anomaly_for_mes_freq = 10 #множитель определяющий аномалию частоты

spark = SparkSession \
    .builder \
    .appName("structured") \
    .getOrCreate()

userSchema = StructType().add("text", "string").add("channel", "string").add("date", "string").add("Is Media Exist", "string").add("time", "string")

df = spark \
    .readStream \
    .schema(userSchema)\
    .format("parquet") \
    .load("D:/python/Spark_session/*.parquet")

#data = df
query1 = df.writeStream.queryName("structured").format("memory").outputMode("append").start()


new_df = pd.DataFrame
last_time = 0 #Время последнего полученного сообщения
default_mes_freq = 0 #стандартная частота, со временем будет меняться
all_mes_count = 0
all_time = 0
while True:
    if new_df.empty:
        new_df = spark.sql("select * from structured where cast(time as INT) > " + str(last_time)).toPandas()
        if not new_df.empty:
            # Обработчик частоты сообщений
            count_new_mes = len(new_df)
            all_mes_count += count_new_mes
            new_freq = count_new_mes/(float(new_df.iloc[-1].tolist()[-1])-last_time)
            if last_time == 0:
                new_time = (float(new_df.iloc[-1].tolist()[-1]) - float(new_df.iloc[0].tolist()[-1]))
                all_time += new_time
            elif default_mes_freq*koef_anomaly_for_mes_freq < new_freq:
                print("Обнаружена аномалия: резко возросла частота сообщений")
            all_time += (float(new_df.iloc[-1].tolist()[-1])-last_time)
            default_mes_freq = all_mes_count/all_time
            last_time = float(new_df.iloc[-1].tolist()[-1])
    else:
        new_df = spark.sql("select * from structured where cast(time as INT) > " + str(last_time)).toPandas()
        if not new_df.empty:
            #Обработчик частоты сообщений
            count_new_mes = len(new_df)
            all_mes_count += count_new_mes
            new_freq = count_new_mes/(float(new_df.iloc[-1].tolist()[-1])-last_time)
            if default_mes_freq*koef_anomaly_for_mes_freq < new_freq:
                print("Обнаружена аномалия: резко возросла частота сообщений")
            all_time += (float(new_df.iloc[-1].tolist()[-1])-last_time)
            default_mes_freq = all_mes_count/all_time
            last_time = float(new_df.iloc[-1].tolist()[-1])
    time.sleep(time_to_reset)
    print(new_df)

query1.stop()


# Ждем завершения потокового приложения