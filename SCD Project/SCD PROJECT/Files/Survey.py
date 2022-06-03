from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Survey").getOrCreate()





    Suervey_Df=spark.read.csv(r"C:\Brainworks\3.SPARK\survey data",header=True)

    Suervey_Df.show(truncate=False)
    print(Suervey_Df.count())

    Suervey_Df.write.mode("overwrite").csv(r"C:\Users\kagar\PycharmProjects\SCD\Target File",header=True)


    Target_df= spark.read.csv(r"C:\Users\kagar\PycharmProjects\SCD\Target File",header=True,inferSchema=True)

    Target_df.show()
    print(Target_df.count())

    Target_df.withColumn("date",lit(current_date())).show()


    New_data=Suervey_Df.unionAll(Target_df).withColumn("Row_num",row_number().over(Window.partitionBy("year")\
                                                                                   .orderBy("date")))


    New_data.write.mode("overwrite").csv(r"C:\Users\kagar\PycharmProjects\SCD\Mediater File",header=True)

    Med_file=spark.read.csv(r"C:\Users\kagar\PycharmProjects\SCD\Mediater",header=True,inferSchema=True)


    Med_file.write.mode("overwrite").csv(r"C:\Users\kagar\PycharmProjects\SCD\Target File",header=True)



