# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "12af3cd4-0641-4f41-9598-0f2378bde2da",
# META       "default_lakehouse_name": "demo1",
# META       "default_lakehouse_workspace_id": "015b04ef-4fb5-4029-a408-10be4f0f90c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "12af3cd4-0641-4f41-9598-0f2378bde2da"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC # Welcome to your new notebook
# MAGIC # Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create TABLE customers AS
# MAGIC (
# MAGIC WITH cte 
# MAGIC as 
# MAGIC (
# MAGIC SELECT  DISTINCT CustomerName as cus_name,EmailAddress
# MAGIC  from dbo.sales
# MAGIC )
# MAGIC select cus_name,EmailAddress , ROW_NUMBER() OVER(order by cus_name) cus_id
# MAGIC from cte
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create TABLE products AS
# MAGIC (
# MAGIC WITH cteProduct 
# MAGIC as 
# MAGIC (
# MAGIC     select DISTINCT Item,UnitPrice,TaxAmount from dbo.sales
# MAGIC     )
# MAGIC select Item,UnitPrice,TaxAmount , ROW_NUMBER() OVER(order by Item) as Item_id
# MAGIC from cteProduct
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table new_sales as 
# MAGIC (
# MAGIC select SalesOrderNumber,SalesOrderLineNumber,OrderDate,Quantity ,cs.cus_id,pr.item_id
# MAGIC from demo1.dbo.sales sa
# MAGIC JOIN demo1.dbo.customers cs on sa.CustomerName = cs.cus_name
# MAGIC join demo1.dbo.products pr on sa.item = pr.item
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
