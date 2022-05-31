# Databricks notebook source
# MAGIC %md
# MAGIC # Revisar e visualizar dados
# MAGIC #### Health tracker data
# MAGIC Um caso de uso comum para trabalhar com Data Lakes é coletar e processar dados de Internet das Coisas (IoT).
# MAGIC Aqui, fornecemos um conjunto de dados de sensor de IoT simulado para fins de demonstração.
# MAGIC Os dados simulam dados de frequência cardíaca medidos por um dispositivo rastreador de saúde.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC 
# MAGIC Antes de executar esta célula adicione seu nome ao arquivo:
# MAGIC <a href="$./includes/configuration" target="_blank">
# MAGIC includes/configuration</a>
# MAGIC 
# MAGIC ```
# MAGIC username = "seu_nome"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health tracker Exemplo de Dados
# MAGIC 
# MAGIC ```
# MAGIC {"device_id":0,"heartrate":52.8139067501,"name":"Deborah Powell","time":1.5778368E9}
# MAGIC {"device_id":0,"heartrate":53.9078900098,"name":"Deborah Powell","time":1.5778404E9}
# MAGIC {"device_id":0,"heartrate":52.7129593616,"name":"Deborah Powell","time":1.577844E9}
# MAGIC {"device_id":0,"heartrate":52.2880422685,"name":"Deborah Powell","time":1.5778476E9}
# MAGIC {"device_id":0,"heartrate":52.5156095386,"name":"Deborah Powell","time":1.5778512E9}
# MAGIC {"device_id":0,"heartrate":53.6280743846,"name":"Deborah Powell","time":1.5778548E9}
# MAGIC ```
# MAGIC Este é um exemplo dos dados do health tracker que usaremos. Note que cada linha é um JSON valido.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health Tracker Schema
# MAGIC Os dados tem o seguinte schema:
# MAGIC 
# MAGIC 
# MAGIC | Coluna    | Tipo      |
# MAGIC |-----------|-----------|
# MAGIC | name      | string    |
# MAGIC | heartrate | double    |
# MAGIC | device_id | int       |
# MAGIC | time      | long      |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Carregando os Dados
# MAGIC Carregando os dados como um DataFrame do Spark a partir do diretório raw.
# MAGIC Isso é feito usando a opção `.format("json")`
# MAGIC assim como um caminho para o método `.load()`

# COMMAND ----------

# TODO
file_path = health_tracker + "raw/health_tracker_data_2020_1.json"

health_tracker_data_2020_1_df = (
  spark.read.json(file_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualizar Dados
# MAGIC ### Mostrar os Dados
# MAGIC Honestamente, isso não faz parte do processo ETL, mas exibi-los nos dá uma visão dos dados com os quais estamos trabalhando.

# COMMAND ----------

display(health_tracker_data_2020_1_df)