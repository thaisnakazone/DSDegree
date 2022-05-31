# Databricks notebook source
# MAGIC %md
# MAGIC # Criando uma tabela Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC 
# MAGIC Antes de executar essa célula, adicione seu nome ao arquivo:
# MAGIC <a href="$./includes/configuration" target="_blank">
# MAGIC includes/configuration</a>
# MAGIC 
# MAGIC ```username = "seu_nome"```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Recarregar os dados do  DataFrame

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
health_tracker_data_2020_1_df = spark.read.format("json").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Criar uma tabela Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 1: Limpeza
# MAGIC Primeiro removeremos os arquivos do diretório `healthtracker/trusted`.
# MAGIC 
# MAGIC Então excluiremos a tabela.
# MAGIC 
# MAGIC Este passo tornará o notebook idempotente. Em outras palavras, ele poderá ser executado mais de uma vez sem erros e sem adicionar arquivos extras.
# MAGIC 
# MAGIC 🚨 **NOTE** Ao longo desta lição, gravaremos arquivos no root do Databricks File System (DBFS). Em geral, a prática recomendada é gravar arquivos no armazenamento de objetos em nuvem. Usamos a raiz DBFS aqui para fins de demonstração.

# COMMAND ----------

dbutils.fs.rm(health_tracker + "trusted", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_trusted
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transformar os Dados
# MAGIC Realizaremos transformações selecionando colunas das seguintes formas:
# MAGIC - Usar `from_unixtime` para trasnformar `"time"`, converter para `date`, e por alias `dte`
# MAGIC - Usar `from_unixtime` para trasnformar `"time"`, converter para `timestamp`, e por alias `time`
# MAGIC - `heartrate` selecionada como é
# MAGIC - `name` selecionado como é
# MAGIC - converter `"device_id"` para inteiro e por o alias `p_device_id`

# COMMAND ----------

# Transformação
from pyspark.sql.functions import col, from_unixtime

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .select(
        from_unixtime("time").cast("date").alias("dte"),
        from_unixtime("time").cast("timestamp").alias("time"),
        "heartrate",
        "name",
        col("device_id").cast("integer").alias("p_device_id")
    )
  )

processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 3: Grave os Arquivos no diretório trusted
# MAGIC 
# MAGIC Note that we are partitioning the data by device id.
# MAGIC Note que estamos particionando os dados por device_id
# MAGIC 
# MAGIC 1. Usar `.format("parquet")`
# MAGIC 1. Particionar por `"p_device_id"`

# COMMAND ----------

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "trusted"))

# COMMAND ----------

display(processedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 4: Registre a Tabela no Metastore
# MAGIC Usar Spark SQL para registrar a tabela no metastore.
# MAGIC Na criação especificamos o formato como parquet e que deve ser usado o local onde os arquivos parquet foram gravados.

# COMMAND ----------

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_trusted
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_trusted
USING PARQUET
LOCATION "{health_tracker}/trusted"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo realizando a mesma transformação utilizando SQL
# MAGIC CREATE TABLE IF NOT EXISTS health_tracker_processed3
# MAGIC USING parquet
# MAGIC AS SELECT cast(from_unixtime(time) as date) as dte
# MAGIC         ,cast(from_unixtime(time) as timestamp) as time
# MAGIC         ,"heartrate"
# MAGIC         ,"name"
# MAGIC         ,cast(device_id as integer) as p_device_id
# MAGIC FROM json.`caminho_arquivo_json`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 5: Verifique a tabela-parquet do Data Lake
# MAGIC Conte os registros na tabela `health_tracker_trusted`

# COMMAND ----------

# Contagem de registros
health_tracker_trusted = spark.read.table("health_tracker_trusted")
health_tracker_trusted.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Note que a contagem não retorna valores

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Registre as partições
# MAGIC 
# MAGIC Por boas práticas nós criamos uma tabela particionada. Entretanto, se criar uma tabela particionada a partir de dados existentes
# MAGIC o Spark SQL não identifica as partições automáticamente para registrá-las no metastore.
# MAGIC 
# MAGIC `MSCK REPAIR TABLE` irá registrar as partições no Hive Metastore. Leia mais sobre este comando em: <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-repair-table.html" target="_blank">
# MAGIC documentação</a>.

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE health_tracker_trusted")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 7: Conte os registros na tabela `health_tracker_trusted`
# MAGIC 
# MAGIC Conte os registros na tabela `health_tracker_trusted`.
# MAGIC 
# MAGIC Com a tabela reparada e as pastições registradas, temos os resultados.
# MAGIC São esperados 3720 registros: cinco medições do dispositivo, 24 horas por dia durante 31 dias.

# COMMAND ----------

## Contagem de registros
health_tracker_trusted.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM health_tracker_trusted;