# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure storage folders using access key, SAS key and Service principal

# COMMAND ----------

# DBTITLE 1,List scopes

# use this to create Secrets  https://adb-6089266189947178.18.azuredatabricks.net/?o=6089266189947178#secrets/createScope

dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,List secrets
dbutils.secrets.list(scope='databricks101-kv-scope')

# COMMAND ----------

# DBTITLE 1,Get Access key
databricks101_dl_access_key = dbutils.secrets.get(scope='databricks101-kv-scope', key='databricks101-dl-access-key')

# COMMAND ----------

# DBTITLE 1,List files using access keys
demo_container="abfss://demo@databricks101dl.dfs.core.windows.net"
# Format of the key is important
spark.conf.set("fs.azure.account.key.databricks101dl.dfs.core.windows.net",
               databricks101_dl_access_key)

display(dbutils.fs.ls(demo_container))

# COMMAND ----------

# DBTITLE 1,List file contents
# Read circuits.csv file from demo_container
circuits_df = spark.read.csv(f"{demo_container}/circuits.csv", header=True, inferSchema=True)
display(circuits_df)

# COMMAND ----------

# DBTITLE 1,Get SAS Keys
## SAS Key expires on May 19 2024

databricks101_dl_demo_container_sas_key = dbutils.secrets.get(scope='databricks101-kv-scope', key='databricks101-dl-demo-container-sas-key')

spark.conf.set("fs.azure.account.auth.type.databricks101dl.dfs.core.windows.net",
               "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricks101dl.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricks101dl.dfs.core.windows.net",
               databricks101_dl_demo_container_sas_key)

# COMMAND ----------

# DBTITLE 1,List file contents using SAS key
# Read circuits.csv file from demo_container
circuits_df = spark.read.csv(f"{demo_container}/circuits.csv", header=True, inferSchema=True)
display(circuits_df)

# COMMAND ----------

# DBTITLE 1,Get Service Principal
tenant_id= dbutils.secrets.get(scope='databricks101-kv-scope', key='tenant-id')
client_id= dbutils.secrets.get(scope='databricks101-kv-scope', key='formula1-app-client-id')
client_secret= dbutils.secrets.get(scope='databricks101-kv-scope', key='formula1-app-client-secret')

spark.conf.set("fs.azure.account.auth.type.databricks101-dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricks101-dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricks101-dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricks101-dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricks101-dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

# DBTITLE 1,List file contents using Service Principal
# Read circuits.csv file from demo_container
circuits_df = spark.read.csv(f"{demo_container}/circuits.csv", header=True, inferSchema=True)
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting Folders

# COMMAND ----------

# DBTITLE 1,Create config using SAS token

tenant_id= dbutils.secrets.get(scope='databricks101-kv-scope', key='tenant-id')
client_id= dbutils.secrets.get(scope='databricks101-kv-scope', key='formula1-app-client-id')
client_secret= dbutils.secrets.get(scope='databricks101-kv-scope', key='formula1-app-client-secret')


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# DBTITLE 1,Mount folder
m_point = "/mnt/circuitsData"
container_name = "demo"
storage_account_name = "databricks101dl"

dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"{m_point}",
    extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,List folder contents
# MAGIC %fs
# MAGIC ls /mnt/circuitsData

# COMMAND ----------

# DBTITLE 1,Show file output
circuits_df = spark.read.csv(f"{m_point}/circuits.csv", header=True, inferSchema=True)
display(circuits_df)
