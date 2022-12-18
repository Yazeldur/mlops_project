from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark

spark_application_name = "Spark_Application_Name"
spark = (SparkSession.builder.appName(spark_application_name).getOrCreate())

atm_path = "/home/yassinb/ING3/mlops/Yass/atm_csvs"

absolute_path = "/home/yassinb/ING3/mlops/Yass/data_analysis"

def read_disp_info(file, delimiter = ',', header = True):
    '''
    Read csv
    '''
    devColumns = [StructField("index",IntegerType()), StructField("date",TimestampType()), StructField("id",IntegerType()), StructField("argent_retire",IntegerType()) ]
    devSchema = StructType(devColumns)
    df = spark.read.options(header=header, delimiter=delimiter).csv(file, schema= devSchema)
    df.printSchema()
    print(df.head(40))
    print(df.tail(40))
    print("Transactions :" + str(df.count()))
    return df

def most_least_transactions_per_hour(df):
    '''
    At what time are there more and the least transactions ? 
    '''
    df = df.select(F.hour(F.col("date")).alias("hour"))
    df = df.groupBy("hour").count().orderBy(F.col("count"))
    df.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question1")
    print(f'The hour with the most amount of transactions across every ATM is {df.tail(1)}')
    print(f'The hour with the least amount of transactions across every ATM is {df.head(1)}')

def most_least_argent_retire(df):
    '''
    What is the biggest and lowest argent_retire from an ATM ? 
    '''
    df = df.select(F.to_date(F.col("date")).alias("day"), F.col("id"), F.col("argent_retire"))
    w_max = Window.partitionBy("id").orderBy(F.col("argent_retire").desc()) 
    df_max = df.withColumn("row", F.row_number().over(w_max)).filter(F.col("row") == 1).drop("row")
    w_min = Window.partitionBy("id").orderBy(F.col("argent_retire").asc()) 
    df_min = df.withColumn("row", F.row_number().over(w_min)).filter(F.col("row") == 1).drop("row")
    df_min.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question2a")
    df_max.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question2b")
    print(f'The most amount of argent retire is {df_min.head(1)}')
    print(f'The least amount of argent retire is {df_max.head(1)}')

def highest_lowest_argent_retire_per_hour_all_ATMs(df):
    '''
    At what time is the argent retire the highest and lowest for all ATMs ?
    '''
    df = df.select(F.hour(F.col("date")).alias("hour"), F.col("argent_retire"))
    df = df.groupBy("hour").sum("argent_retire").orderBy(F.col("sum(argent_retire)"))
    df.show(24)
    df.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question3")
    print(f'The hour with the most amount of argent retire across every ATM is {df.head(1)}')
    print(f'The hour with the least amount of argent retire across every ATM is {df.tail(1)}')

def highest_lowest_argent_retire_per_hour_per_ATM(df):
    '''
    At what time is the argent retire the highest and lowest for each ATM ?
    '''
    df = df.select(F.hour(F.col("date")).alias("hour"), F.col("id"), F.col("argent_retire"))
    df = df.groupBy("hour", "id").sum("argent_retire").orderBy(F.col("id"), F.col("sum(argent_retire)"))
    w_max = Window.partitionBy("id").orderBy(F.col("sum(argent_retire)").desc()) 
    df_max = df.withColumn("row", F.row_number().over(w_max)).filter(F.col("row") == 1).drop("row")
    w_min = Window.partitionBy("id").orderBy(F.col("sum(argent_retire)").asc())
    df_min = df.withColumn("row", F.row_number().over(w_min)).filter(F.col("row") == 1).drop("row")
    df_min.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question4a")
    df_max.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question4b")

def highest_lowest_argent_retire_per_day_all_ATMs(df):
    '''
    What day is the argent retire the highest and lowest for all ATMs ?
    '''
    df = df.select(F.to_date(F.col("date")).alias("day"), F.col("argent_retire"))
    df = df.groupBy("day").sum("argent_retire").orderBy(F.col("sum(argent_retire)"))
    df.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question5")
    print(f'The day with the most amount of argent retire across every ATM is {df.tail(1)}')
    print(f'The day with the least amount of argent retire across every ATM is {df.head(1)}')

def highest_lowest_argent_retire_per_day_each_ATM(df):
    '''
    What day is the argent retire the highest and lowest for each ATM ?
    '''
    df = df.select(F.to_date(F.col("date")).alias("day"), F.col("id"), F.col("argent_retire"))
    df = df.groupBy("day", "id").sum("argent_retire").orderBy(F.col("id"),F.col("sum(argent_retire)"))
    w_max = Window.partitionBy("id").orderBy(F.col("sum(argent_retire)").desc()) 
    df_max = df.withColumn("row", F.row_number().over(w_max)).filter(F.col("row") == 1).drop("row")
    w_min = Window.partitionBy("id").orderBy(F.col("sum(argent_retire)").asc())
    df_min = df.withColumn("row", F.row_number().over(w_min)).filter(F.col("row") == 1).drop("row")
    df_min.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question6a")
    df_max.write.mode("overwrite").option("header",True).csv(f"{absolute_path}/df_csvs/question6b")

def main():
    print("READING CSV")
    df = read_disp_info(atm_path, delimiter = ',', header = True)
    print("----------------------------------------------------------------------")
    print("À quelle heure y a-t-il le plus et le moins de transactions ? ")
    most_least_transactions_per_hour(df)
    print("----------------------------------------------------------------------")
    print("Quel est le montant le plus élevé et le plus bas de l'argent_retire à partir d'un distributeur automatique de billets (DAB)?")
    most_least_argent_retire(df)
    print("----------------------------------------------------------------------")
    print("À quelle heure l'argent_retire est-elle la plus élevée et la plus basse pour tous les DAB ?")
    highest_lowest_argent_retire_per_hour_all_ATMs(df)
    print("----------------------------------------------------------------------")
    print("À quelle heure l'argent_retire est-il le plus élevé et le plus bas pour chaque DAB ?")
    highest_lowest_argent_retire_per_hour_per_ATM(df)
    print("----------------------------------------------------------------------")
    print("Quel jour le retrait d'argent est-il le plus élevé et le plus bas pour tous les DAB ?")
    highest_lowest_argent_retire_per_day_all_ATMs(df)
    print("----------------------------------------------------------------------")
    print("Quel jour le retrait d'argent est-il le plus élevé et le plus bas pour chaque DAB ?")
    highest_lowest_argent_retire_per_day_each_ATM(df)

main()