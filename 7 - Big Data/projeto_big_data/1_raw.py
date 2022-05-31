# Databricks notebook source
# MAGIC %md
# MAGIC # Criando um Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingestão de dados na camada "raw"
# MAGIC As tabelas em CSV do banco de dados "northwind" deverão ser carregadas no diretório "raw", que será a primeira camada do Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Configuração

# COMMAND ----------

username = 'thais'

northwind = f'/letscode/{username}/northwind/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Limpeza
# MAGIC Removendo o diretório "northwind" no DBFS, caso este notebook seja executado mais de uma vez.

# COMMAND ----------

dbutils.fs.rm(northwind, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Criando a camada "raw" no DBFS
# MAGIC O upload das tabelas da Northwind foi feito em FileStore > tables.   
# MAGIC Essas tabelas serão copiadas para suas pastas correspondentes na camada "raw".   
# MAGIC Decidi copiar as tabelas ao invés de movê-las, devido à necessidade de executar testes neste notebook inúmeras vezes.

# COMMAND ----------

def data_to_raw(data_path):
    
    '''
    Função para criar a camada "raw" e realizar a ingestão dos dados.
    
    Arg:
        data_path: caminho original dos dados.
    
    Return:
        raw_content: mostra o conteúdo da camada "raw".
    '''
    
    for file in dbutils.fs.ls(data_path):
        origin = data_path + file.name
        file_without_extension = f'{file.name}'.rsplit('.', 1)[0]
        dbutils.fs.mkdirs(northwind + f'raw/{file_without_extension}')
        dbfs = northwind + f'raw/{file_without_extension}/' + file.name
        dbutils.fs.cp(origin, dbfs)
    raw_content = display(dbutils.fs.ls(northwind + 'raw/'))
    
    return raw_content

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Mostrando o conteúdo da camada "raw"

# COMMAND ----------

# Chamando a função que mostra o conteúdo da "raw"
data_to_raw('/FileStore/tables/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Validando a ingestão dos dados
# MAGIC Verificando se todas as tabelas foram copiadas para a camada "raw".

# COMMAND ----------

def validate_data_ingestion(data_path):
    
    '''
    Função para validar a ingestão dos dados na camada "raw".
    
    Arg:
        data_path: caminho original dos dados.
    
    Return:
        imprime a validação de cada arquivo na camada "raw".
    '''
    
    for file in dbutils.fs.ls(data_path):
        file_without_extension = f'{file.name}'.rsplit('.', 1)[0]
        assert file.name in [file.name for file in dbutils.fs.ls(northwind + f'raw/{file_without_extension}')], f'{file.name} not present in Raw Path.'
        print(f'{file.name}: assertion passed.')

# Chamando a função que valida a ingestão dos dados
validate_data_ingestion('/FileStore/tables/')