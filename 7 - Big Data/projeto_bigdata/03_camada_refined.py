# Databricks notebook source
# MAGIC %md
# MAGIC #Criar tabelas delta
# MAGIC  
# MAGIC Objetivo: Converter uma tabela baseada em Parquet em uma tabela Delta.
# MAGIC 
# MAGIC Lembre-se de que uma tabela Delta consiste em três coisas:
# MAGIC - os arquivos de dados mantidos no armazenamento de objetos (ou seja, AWS S3, Azure Data Lake Storage)
# MAGIC - o log de transações delta salvo com os arquivos de dados no armazenamento de objetos
# MAGIC - uma tabela cadastrada no Metastore. Esta etapa é opcional, mas geralmente recomendada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC 
# MAGIC Antes de executar essa célula, adicione seu nome ao arquivo:
# MAGIC <a href="$./includes/configuration" target="_blank">
# MAGIC includes/configuration</a>
# MAGIC 
# MAGIC ```username = "seu_nome"```
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando uma tabela
# MAGIC Com o Delta Lake, você cria tabelas:
# MAGIC * Ao ingerir novos arquivos em uma Tabela Delta pela primeira vez
# MAGIC * Transformando uma tabela de data lake baseada em Parquet existente em uma tabela Delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **OBSERVAÇÃO:** 
# MAGIC Ao longo desta aula gravaremos arquivos no diretório raiz do Databricks File System (DBFS).
# MAGIC Em geral, a prática recomendada é gravar arquivos no armazenamento de objetos em nuvem. Usamos o DBFS apenas para demonstração.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Step 1: Describe the `health_tracker_processed` Table
# MAGIC Antes de converter a tabela `health_tracker_trusted`, vamos usar o comando Spark SQL `DESCRIBE`, com o parâmetro opcional `EXTENDED`, para exibir os atributos da tabela.
# MAGIC Observe que a tabela tem o "provider" listado como `PARQUET`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Você terá que rolar até `#Detailed Table Information` para encontrar o provedor.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED health_tracker_trusted;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converter uma Tabela Parquet existente em uma Tabela Delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC #### Passo 1: Converter os Arquivos em Arquivos Delta
# MAGIC 
# MAGIC Primeiro, converteremos os arquivos no local para arquivos Delta. A conversão cria um log de transações do Delta Lake que rastreia os arquivos associados.

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA health_tracker_trusted
# MAGIC PARTITIONED BY (p_device_id integer)

# COMMAND ----------

from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}trusted`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 2: Registrar a Tabela Delta
# MAGIC Neste ponto, os arquivos contendo nossos registros foram convertidos em arquivos Delta.
# MAGIC O Metastore, no entanto, não foi atualizado para refletir a alteração.
# MAGIC Para alterar isso, registramos novamente a tabela no Metastore.
# MAGIC O comando Spark SQL inferirá automaticamente o esquema de dados lendo os rodapés dos arquivos Delta.

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_trusted
""")

spark.sql(f"""
CREATE TABLE health_tracker_trusted
USING DELTA
LOCATION "{health_tracker}/trusted" 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 3: Adicionar comentários de coluna 
# MAGIC 
# MAGIC Os comentários podem facilitar a leitura e a manutenção de suas tabelas. Usamos um comando `ALTER TABLE` para adicionar novos comentários de coluna à tabela Delta existente.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   health_tracker_trusted
# MAGIC REPLACE COLUMNS
# MAGIC   (dte DATE COMMENT "Formato: YYYY/mm/dd", 
# MAGIC   time TIMESTAMP, 
# MAGIC   heartrate DOUBLE,
# MAGIC   name STRING COMMENT "Formato: First Last",
# MAGIC   p_device_id INT COMMENT "range 0 - 4");

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 4: descreva a tabela `health_tracker_trusted`
# MAGIC Podemos verificar se os comentários foram adicionados à tabela usando o comando `DESCRIBE` Spark SQL seguido do parâmetro opcional `EXTENDED`. Você pode ver os comentários da coluna que adicionamos, bem como algumas informações adicionais. Role para baixo para confirmar que a nova tabela tinha Delta listado como "provider". 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED health_tracker_trusted;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 5: Contar os Registros na Tabela `health_tracker_trusted`
# MAGIC Contamos os registros em `health_tracker_trusted` com Apache Spark.
# MAGIC Com o Delta Lake, a mesa Delta não requer reparos e está imediatamente pronta para uso.

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_trusted")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crie uma nova tabela delta
# MAGIC Em seguida, criaremos uma nova tabela Delta. Faremos isso criando uma tabela agregada
# MAGIC dos dados na tabela Delta health_track_trusted que acabamos de criar.
# MAGIC Dentro do contexto do nosso EDSS (Enterprise Decision Support System), esta é uma tabela agregada downstream ou data mart.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 1: Remover Arquivos no Diretório `health_tracker_user_analytics`
# MAGIC Esta etapa tornará o notebook idempotente. Em outras palavras, ele pode ser executado mais de uma vez sem gerar erros ou introduzir arquivos extras.

# COMMAND ----------

dbutils.fs.rm(health_tracker + "refined/health_tracker_user_analytics",
              recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 2: criar um DataFrame Agregado
# MAGIC A subconsulta usada para definir a tabela é uma consulta agregada sobre a tabela Delta `health_tracker_trsuted` usando estatísticas resumidas para cada dispositivo.

# COMMAND ----------

# Criar DF agregado
from pyspark.sql.functions import col, avg, max, stddev

health_tracker_refined_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Passo 3: Gravar os Arquivos Delta

# COMMAND ----------

(health_tracker_refined_user_analytics.write
 .format("delta")
 .mode("overwrite")
 .save(health_tracker + "refined/health_tracker_refined_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passo 4: Registrar a Tabela Delta no Metastore
# MAGIC Por fim, registre esta tabela no Metastore.

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_refined_user_analytics
""")

spark.sql(f"""
CREATE TABLE health_tracker_refined_user_analytics
USING DELTA
LOCATION "{health_tracker}/refined/health_tracker_refined_user_analytics"
""")


# COMMAND ----------

display(health_tracker_refined_user_analytics)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC FROM health_tracker_refined_user_analytics
# MAGIC WHERE p_device_id = 4;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Visualização de dados com Power BI
# MAGIC 
# MAGIC https://powerbi.microsoft.com/en-us/blog/announcing-power-bi-integration-with-databricks-partner-connect/