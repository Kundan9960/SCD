from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("sql queations").getOrCreate()




    Empdf=spark.read.csv(r"C:\Users\kagar\PycharmProjects\pythonProject\Files\hr.employees with New Header.csv",header=True,inferSchema=True)
    Empdf.show(100)
    Empdf.printSchema()
    Empdf.cache()

    Empdf.createOrReplaceTempView("Employee")


    ''' 1. Display all the details from Emp table.'''

    # spark.sql("select * from Employee").show()

    # Empdf.select("*").show()




    '''2. Select the employees in department 100.'''

    # spark.sql("select * from Employee where dept=100").show()

    # Empdf.select("*").filter(col("dept")==100).show()


    '''3. List the names, numbers and departments of all clerks.'''

    # spark.sql("select empid, First_name,Last_name,dept from Employee where job like  '%C%'").show()

    # Empdf.select(["empid", "First_name","Last_name","dept","job"]).filter(col("job").like("%C%")).show()



    '''4. Find the department numbers and names of employees of all departments with deptno greater than 20.'''

    # spark.sql("select * from Employee where dept>20").show()

    # Empdf.select("*").filter(col("dept")>20).show()



    '''5. Find employees whose commission is greater than their salaries.'''

    # spark.sql("select * from Employee where (Comm)*100 > salary ").show()

    # Empdf.select("*").where((col("Comm")*100)>"salary").show()





    '''6. Find employees whose commission is greater than 60 % of their salaries.'''

    # spark.sql("select * from Employee where Comm > (salary*100)/60").show()

    # Empdf.select("*").where(col("Comm")>((col("salary"))*(60/100))).show()



    '''7. List name, job and salary of all employees in department 20 who earn more than 2000/-.'''

    # spark.sql("select * from Employee where dept=20 and salary>2000").show()
    # spark.sql("select * from Employee where dept=20 ").show()


    # Empdf.select(["First_name","job","salary"]).filter((col("dept")==20) & (col("salary")>2000)).show()



    '''8. Find all salesmen in department 30 whose salary is greater than 1500/-.'''

    # spark.sql("select * from Employee where dept=30 and job like '%SA%' and salary >1500").show()

    # Empdf.select("*").filter((col("dept")==30) & (col("job").like("%SA%")) & (col("salary")>1500)).show()


    '''9. Find all employees whose designation is either manager or president.'''

    # spark.sql("select * from Employee where EMPLOYEE_ID in (select distinct (MANAGER_ID) from Employee) ").show()

    # Empdf.select("*").where(col("EMPLOYEE_ID")).distinct(col("MANAGER_ID")).show() .... NS



    '''10. Find all managers who are not in department 30.'''

    spark.sql("").show()










