# Databricks notebook source
# MAGIC %md
# MAGIC # Criando um Data Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criando tabelas Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Configuração

# COMMAND ----------

username = 'thais'

northwind = f'/letscode/{username}/northwind/'

# Primeiramente, usaremos este banco de dados apenas para converter as tabelas para Delta.
spark.sql(f'USE letscode_{username}_trusted')

# Depois, criaremos outro banco de dados para criar as tabelas fato, agregadas e dimensões.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Convertendo tabelas em Parquet para Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA categories;
# MAGIC CONVERT TO DELTA customers;
# MAGIC CONVERT TO DELTA employee_territories;
# MAGIC CONVERT TO DELTA employees;
# MAGIC CONVERT TO DELTA order_details;
# MAGIC CONVERT TO DELTA orders;
# MAGIC CONVERT TO DELTA products;
# MAGIC CONVERT TO DELTA region;
# MAGIC CONVERT TO DELTA shippers;
# MAGIC CONVERT TO DELTA suppliers;
# MAGIC CONVERT TO DELTA territories;
# MAGIC CONVERT TO DELTA us_states

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Registrando as tabelas no Metastore

# COMMAND ----------

path_trusted = northwind + 'trusted/'

for folder in dbutils.fs.ls(path_trusted):
    folder_name = f'{folder.name}'.rsplit('/', 1)[0]
    spark.sql(
        f"""
        DROP TABLE IF EXISTS {folder_name}
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {folder_name}
        USING DELTA
        LOCATION "{northwind}trusted/{folder.name}"
        """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Exibindo os atributos da tabela "categories"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED categories;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Manipulando as tabelas para criar um Data Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.1 Lendo as tabelas em DFs

# COMMAND ----------

df_orders = spark.read.table('orders')
df_order_details = spark.read.table('order_details')
df_customers = spark.read.table('customers')
df_employees = spark.read.table('employees')
df_products = spark.read.table('products')
df_shippers = spark.read.table('shippers')
df_us_states = spark.read.table('us_states')
df_employee_territories = spark.read.table('employee_territories')
df_territories = spark.read.table('territories')
df_region = spark.read.table('region')
df_categories = spark.read.table('categories')
df_suppliers = spark.read.table('suppliers')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.2 Criando "surrogate keys" nas tabelas que serão definidas como dimensão.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id,row_number

# Criando "surrogate keys"
dm_customers = df_customers.withColumn("sk_customers",row_number().over(Window.orderBy(monotonically_increasing_id())))
dm_employees = df_employees.withColumn("sk_employees",row_number().over(Window.orderBy(monotonically_increasing_id())))
dm_products = df_products.withColumn("sk_products",row_number().over(Window.orderBy(monotonically_increasing_id())))
dm_shippers = df_shippers.withColumn("sk_shippers",row_number().over(Window.orderBy(monotonically_increasing_id())))
dm_us_states = df_us_states.withColumn("sk_us_states",row_number().over(Window.orderBy(monotonically_increasing_id())))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.3 Alterando o nome das colunas

# COMMAND ----------

dm_customers = dm_customers.withColumnRenamed('customer_id','nk_customers')
dm_employees = dm_employees.withColumnRenamed('employee_id','nk_employees')
dm_products = dm_products.withColumnRenamed('product_id','nk_products')
dm_shippers = dm_shippers.withColumnRenamed('shipper_id','nk_shippers')
dm_us_states = dm_us_states.withColumnRenamed('state_id','nk_us_states')
df_employee_territories = df_employee_territories.withColumnRenamed('employee_id','nk_employees')

df_suppliers = df_suppliers.toDF('supplier_id','sup_company_name', 'sup_contact_name', 'sup_contact_title', 'sup_address', 'sup_city', 'sup_region', 'sup_postal_code', 'sup_country', 'sup_phone', 'sup_fax', 'sup_homepage')

# COMMAND ----------

# Verificando um dos DFs alterados
display(dm_customers.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.4 Unindo "orders" e "order_details"

# COMMAND ----------

# Unindo os DFs
df_orders_2 = df_orders.join(df_order_details,['order_id'],how='inner')

# Alterando o nome das colunas
df_orders_2 = df_orders_2.withColumnRenamed('order_id','ft_orders_id').withColumnRenamed('customer_id','nk_customers').withColumnRenamed('employee_id','nk_employees').withColumnRenamed('product_id','nk_products')

display(df_orders_2.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.5 Unindo "orders_2", "customers", "employees" e "products"
# MAGIC Essas tabelas serão unidas apenas para adicionar as "surrogate keys" e remover as "natural keys" na tabela fato.

# COMMAND ----------

# Unindo os DFs
df_temp_1 = df_orders_2.join(dm_customers,['nk_customers'],how='inner')
df_temp_2 = df_temp_1.join(dm_employees,['nk_employees'],how='inner')
df_temp_3 = df_temp_2.join(dm_products,['nk_products','unit_price'],how='inner')

# Selecionando as colunas da tabela fato
ft_orders = df_temp_3.select(
  'ft_orders_id',
  'sk_customers',
  'sk_employees',
  'sk_products',
  'order_date',
  'order_year',
  'order_month',
  'required_date',
  'shipped_date',
  'ship_via',
  'freight',
  'ship_name',
  'ship_address',
  'ship_city',
  'ship_region',
  'ship_postal_code',
  'ship_country',
  'unit_price',
  'quantity',
  'discount'
)

display(ft_orders.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.6 Unindo "employees", "employee_territories", "territories" e "region"

# COMMAND ----------

# Unindo os DFs
df_temp_1 = dm_employees.join(df_employee_territories,['nk_employees'],how='inner')
df_temp_2 = df_temp_1.join(df_territories,['territory_id'],how='inner')
df_temp_3 = df_temp_2.join(df_region,['region_id'],how='inner')

# Selecionando as colunas da tabela
dm_employees = df_temp_3.select(
  'sk_employees',
  'nk_employees',
  'last_name',
  'first_name',
  'title',
  'title_of_courtesy',
  'birth_date',
  'hire_date',
  'address',
  'city',
  'territory_description',
  'region',
  'region_description',
  'postal_code',
  'country',
  'home_phone',
  'extension',
  'photo',
  'notes',
  'reports_to',
  'photo_path'
)

display(dm_employees.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5.7 Unindo "products", "categories" e "suppliers"

# COMMAND ----------

# Unindo os DFs
df_temp_1 = dm_products.join(df_categories,['category_id'],how='inner')
df_temp_2 = df_temp_1.join(df_suppliers,['supplier_id'],how='inner')

# Selecionando as colunas da tabela
dm_products = df_temp_2.select(
  'sk_products',
  'nk_products',
  'product_name',
  'category_name',
  'description',
  'picture',
  'quantity_per_unit',
  'unit_price',
  'units_in_stock',
  'units_on_order',
  'reorder_level',
  'discontinued',
  'sup_company_name',
  'sup_contact_name',
  'sup_contact_title',
  'sup_address',
  'sup_city',
  'sup_region',
  'sup_postal_code',
  'sup_country',
  'sup_phone',
  'sup_fax',
  'sup_homepage'
)

display(dm_products.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Limpeza
# MAGIC Removendo diretórios, caso este notebook seja executado mais de uma vez.

# COMMAND ----------

dbutils.fs.rm(northwind + "refined/ft_orders", recurse=True)

dbutils.fs.rm(northwind + "refined/ft_year_orders", recurse=True)
dbutils.fs.rm(northwind + "refined/ft_month_orders", recurse=True)

dbutils.fs.rm(northwind + "refined/dm_employees", recurse=True)
dbutils.fs.rm(northwind + "refined/dm_products", recurse=True)
dbutils.fs.rm(northwind + "refined/dm_customers", recurse=True)
dbutils.fs.rm(northwind + "refined/dm_shippers", recurse=True)
dbutils.fs.rm(northwind + "refined/dm_us_states", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 Gravando os arquivos Delta

# COMMAND ----------

(ft_orders.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/ft_orders'))

(dm_employees.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/dm_employees'))

(dm_products.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/dm_products'))

(dm_customers.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/dm_customers'))

(dm_shippers.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/dm_shippers'))

(dm_us_states.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/dm_us_states'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 Criando um banco de dados para armazenar as tabelas do DW

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS letscode_{username}_refined')
spark.sql(f'USE letscode_{username}_refined')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.9 Registrando as tabela Delta no Metastore

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS ft_orders')
spark.sql(f'DROP TABLE IF EXISTS dm_customers')
spark.sql(f'DROP TABLE IF EXISTS dm_employees')
spark.sql(f'DROP TABLE IF EXISTS dm_products')
spark.sql(f'DROP TABLE IF EXISTS dm_shippers')
spark.sql(f'DROP TABLE IF EXISTS dm_us_states')


spark.sql(
    f"""
    CREATE TABLE ft_orders
    USING DELTA
    LOCATION "{northwind}refined/ft_orders"
    """
)
spark.sql(
    f"""
    CREATE TABLE dm_customers
    USING DELTA
    LOCATION "{northwind}refined/dm_customers"
    """
)
spark.sql(
    f"""
    CREATE TABLE dm_employees
    USING DELTA
    LOCATION "{northwind}refined/dm_employees"
    """
)
spark.sql(
    f"""
    CREATE TABLE dm_products
    USING DELTA
    LOCATION "{northwind}refined/dm_products"
    """
)
spark.sql(
    f"""
    CREATE TABLE dm_shippers
    USING DELTA
    LOCATION "{northwind}refined/dm_shippers"
    """
)
spark.sql(
    f"""
    CREATE TABLE dm_us_states
    USING DELTA
    LOCATION "{northwind}refined/dm_us_states"
    """
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.10 Criando dois DataFrames agregados

# COMMAND ----------

from pyspark.sql.functions import sum, col, avg

# Agregando o valor total de vendas por ano, sem levar o frete em consideração
ft_year_orders = (
  ft_orders
  .groupby('order_year')
  .agg((sum(col('unit_price') * col('quantity') - col('discount'))).alias('total_sales'))
  .sort(('order_year'))
)

# Agregando o valor total de vendas por mês, sem levar o frete em consideração
ft_month_orders = (
  ft_orders
  .groupby('order_year','order_month')
  .agg((sum(col('unit_price') * col('quantity') - col('discount'))).alias('total_sales'))
  .sort((['order_year','order_month']))
)

# COMMAND ----------

display(ft_year_orders)

# COMMAND ----------

display(ft_month_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.10.1 Gravando os arquivos Delta

# COMMAND ----------

(ft_year_orders.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/ft_year_orders'))

(ft_month_orders.write
 .format("delta")
 .mode("overwrite")
 .save(northwind + 'refined/ft_month_orders'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.10.2 Registrando as tabela Delta no Metastore

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS ft_year_orders')
spark.sql(f'DROP TABLE IF EXISTS ft_month_orders')


spark.sql(
    f"""
    CREATE TABLE ft_year_orders
    USING DELTA
    LOCATION "{northwind}refined/ft_year_orders"
    """
)
spark.sql(
    f"""
    CREATE TABLE ft_month_orders
    USING DELTA
    LOCATION "{northwind}refined/ft_month_orders"
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ft_year_orders
# MAGIC WHERE order_year = 1997;