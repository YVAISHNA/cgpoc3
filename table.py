import data
import prop as prop

from pyspark.sql import *

spark = SparkSession.builder.appName('table').config("spark.jars","C:\Installers\Drivers\postgresql-42.6.0.jar").getOrCreate()


properties = {
"url" : "jdbc:postgresql://localhost:5433/postgres",

    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "2626"
}

url = "jdbc:postgresql://localhost:5433/postgres"


# reading data files

customers = spark.read.jdbc(url=url, table='customers', properties=properties)
items = spark.read.jdbc(url=url, table='items', properties=properties)
orders = spark.read.jdbc(url=url, table='orders', properties=properties)
order_details = spark.read.jdbc(url=url, table='order_details', properties=properties)
salesperson = spark.read.jdbc(url=url, table='salesperson', properties=properties)
ship_to = spark.read.jdbc(url=url, table='ship_to', properties=properties)




#customers.show()
customers.write.format("parquet").mode("append").save("C:/parquets/customers/")
items.write.format("parquet").mode("append").save("C:/parquets/items/")
orders.write.format("parquet").mode("append").save("C:/parquets/orders/")
order_details.write.format("parquet").mode("append").save("C:/parquets/order_details/")
salesperson.write.format("parquet").mode("append").save("C:/parquets/salesperson/")
ship_to.write.format("parquet").mode("append").save("C:/parquets/ship_to/")
tables = [customers,items,orders,order_details,salesperson,ship_to]

for table in tables:
  print(table.show())


