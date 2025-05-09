# Spark Streaming Homework

### Balázs Mikes

#### github link:
https://github.com/csirkepaprikas/M13_SparkStreaming_PYTHON_AZURE.git

The Spark Streaming Homework was designed to provide me with a deeper understanding of streaming processes and how to work with them within the Databricks platform.
Through this assignment, I gained more experience with streaming solutions and become more familiar with the key concepts of Spark Streaming applications.
By the end of the homework, I have acquired knowledge of execution, monitoring, and managing streaming applications, which will be valuable for developing robust and scalable streaming solutions.

## Preparations

First I created a new Resource Group in the Azure GUI:

![new_rg](https://github.com/user-attachments/assets/7601ded3-94a8-4a13-b0fd-212e55474b67)

Then I created the storage account:

![new_cont](https://github.com/user-attachments/assets/64a286a5-1659-47bd-a103-779abf8c9fe7)

![created_stor_cont](https://github.com/user-attachments/assets/c9896417-7d48-4374-b81f-8cbfb338bdfd)

Also created the container for the terraform:

![container](https://github.com/user-attachments/assets/a95c3fcb-f4a5-499d-9456-c8006a4e43e7)


Then I modified the main.tf to being able to create the infrastructure on top of Azure.
```python
c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>terraform init
Initializing the backend...

Successfully configured the backend "azurerm"! Terraform will automatically
use this backend unless the backend configuration changes.
Initializing provider plugins...
- Finding hashicorp/azurerm versions matching "~> 4.3.0"...
- Finding latest version of hashicorp/random...
- Installing hashicorp/azurerm v4.3.0...
- Installed hashicorp/azurerm v4.3.0 (signed by HashiCorp)
- Installing hashicorp/random v3.7.2...
- Installed hashicorp/random v3.7.2 (signed by HashiCorp)
Terraform has created a lock file .terraform.lock.hcl to record the provider
selections it made above. Include this file in your version control repository
so that Terraform can guarantee to make the same selections by default when
you run "terraform init" in the future.

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>terraform plan -out terraform.plan
Acquiring state lock. This may take a few moments...
data.azurerm_client_config.current: Reading...
data.azurerm_client_config.current: Read complete after 0s [id=Y2NGY4ZjcwNWYxZTI=]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # azurerm_databricks_workspace.bdcc will be created
  + resource "azurerm_databricks_workspace" "bdcc" {
      + customer_managed_key_enabled      = false
      + disk_encryption_set_id            = (known after apply)
      + id                                = (known after apply)
      + infrastructure_encryption_enabled = false
      + location                          = "westeurope"
      + managed_disk_identity             = (known after apply)
      + managed_resource_group_id         = (known after apply)
      + managed_resource_group_name       = (known after apply)
      + name                              = (known after apply)
      + public_network_access_enabled     = true
      + resource_group_name               = (known after apply)
      + sku                               = "standard"
      + storage_account_identity          = (known after apply)
      + tags                              = {
          + "env"    = "dev"
          + "region" = "global"
        }
      + workspace_id                      = (known after apply)
      + workspace_url                     = (known after apply)

      + custom_parameters (known after apply)
    }

  # azurerm_resource_group.bdcc will be created
  + resource "azurerm_resource_group" "bdcc" {
      + id       = (known after apply)
      + location = "westeurope"
      + name     = (known after apply)
      + tags     = {
          + "env"    = "dev"
          + "region" = "global"
        }
    }

  # azurerm_storage_account.bdcc will be created
  + resource "azurerm_storage_account" "bdcc" {
      + access_tier                        = (known after apply)
      + account_kind                       = "StorageV2"
      + account_replication_type           = "LRS"
      + account_tier                       = "Standard"
      + allow_nested_items_to_be_public    = true
      + cross_tenant_replication_enabled   = false
      + default_to_oauth_authentication    = false
      + dns_endpoint_type                  = "Standard"
      + https_traffic_only_enabled         = true
      + id                                 = (known after apply)
      + infrastructure_encryption_enabled  = false
      + is_hns_enabled                     = true
      + large_file_share_enabled           = (known after apply)
      + local_user_enabled                 = true
      + location                           = "westeurope"
      + min_tls_version                    = "TLS1_2"
      + name                               = (known after apply)
      + nfsv3_enabled                      = false
      + primary_access_key                 = (sensitive value)
      + primary_blob_connection_string     = (sensitive value)
      + primary_blob_endpoint              = (known after apply)
      + primary_blob_host                  = (known after apply)
      + primary_blob_internet_endpoint     = (known after apply)
      + primary_blob_internet_host         = (known after apply)
      + primary_blob_microsoft_endpoint    = (known after apply)
      + primary_blob_microsoft_host        = (known after apply)
      + primary_connection_string          = (sensitive value)
      + primary_dfs_endpoint               = (known after apply)
      + primary_dfs_host                   = (known after apply)
      + primary_dfs_internet_endpoint      = (known after apply)
      + primary_dfs_internet_host          = (known after apply)
      + primary_dfs_microsoft_endpoint     = (known after apply)
      + primary_dfs_microsoft_host         = (known after apply)
      + primary_file_endpoint              = (known after apply)
      + primary_file_host                  = (known after apply)
      + primary_file_internet_endpoint     = (known after apply)
      + primary_file_internet_host         = (known after apply)
      + primary_file_microsoft_endpoint    = (known after apply)
      + primary_file_microsoft_host        = (known after apply)
      + primary_location                   = (known after apply)
      + primary_queue_endpoint             = (known after apply)
      + primary_queue_host                 = (known after apply)
      + primary_queue_microsoft_endpoint   = (known after apply)
      + primary_queue_microsoft_host       = (known after apply)
      + primary_table_endpoint             = (known after apply)
      + primary_table_host                 = (known after apply)
      + primary_table_microsoft_endpoint   = (known after apply)
      + primary_table_microsoft_host       = (known after apply)
      + primary_web_endpoint               = (known after apply)
      + primary_web_host                   = (known after apply)
      + primary_web_internet_endpoint      = (known after apply)
      + primary_web_internet_host          = (known after apply)
      + primary_web_microsoft_endpoint     = (known after apply)
      + primary_web_microsoft_host         = (known after apply)
      + public_network_access_enabled      = true
      + queue_encryption_key_type          = "Service"
      + resource_group_name                = (known after apply)
      + secondary_access_key               = (sensitive value)
      + secondary_blob_connection_string   = (sensitive value)
      + secondary_blob_endpoint            = (known after apply)
      + secondary_blob_host                = (known after apply)
      + secondary_blob_internet_endpoint   = (known after apply)
      + secondary_blob_internet_host       = (known after apply)
      + secondary_blob_microsoft_endpoint  = (known after apply)
      + secondary_blob_microsoft_host      = (known after apply)
      + secondary_connection_string        = (sensitive value)
      + secondary_dfs_endpoint             = (known after apply)
      + secondary_dfs_host                 = (known after apply)
      + secondary_dfs_internet_endpoint    = (known after apply)
      + secondary_dfs_internet_host        = (known after apply)
      + secondary_dfs_microsoft_endpoint   = (known after apply)
      + secondary_dfs_microsoft_host       = (known after apply)
      + secondary_file_endpoint            = (known after apply)
      + secondary_file_host                = (known after apply)
      + secondary_file_internet_endpoint   = (known after apply)
      + secondary_file_internet_host       = (known after apply)
      + secondary_file_microsoft_endpoint  = (known after apply)
      + secondary_file_microsoft_host      = (known after apply)
      + secondary_location                 = (known after apply)
      + secondary_queue_endpoint           = (known after apply)
      + secondary_queue_host               = (known after apply)
      + secondary_queue_microsoft_endpoint = (known after apply)
      + secondary_queue_microsoft_host     = (known after apply)
      + secondary_table_endpoint           = (known after apply)
      + secondary_table_host               = (known after apply)
      + secondary_table_microsoft_endpoint = (known after apply)
      + secondary_table_microsoft_host     = (known after apply)
      + secondary_web_endpoint             = (known after apply)
      + secondary_web_host                 = (known after apply)
      + secondary_web_internet_endpoint    = (known after apply)
      + secondary_web_internet_host        = (known after apply)
      + secondary_web_microsoft_endpoint   = (known after apply)
      + secondary_web_microsoft_host       = (known after apply)
      + sftp_enabled                       = false
      + shared_access_key_enabled          = true
      + table_encryption_key_type          = "Service"
      + tags                               = {
          + "env"    = "dev"
          + "region" = "global"
        }

      + blob_properties (known after apply)

      + network_rules {
          + bypass                     = (known after apply)
          + default_action             = "Allow"
          + ip_rules                   = [
              + "174.128.60.160",
              + "174.128.60.162",
              + "185.44.13.36",
              + "195.56.119.209",
              + "195.56.119.212",
              + "203.170.48.2",
              + "204.153.55.4",
              + "213.184.231.20",
              + "85.223.209.18",
              + "86.57.255.94",
            ]
          + virtual_network_subnet_ids = (known after apply)
        }

      + queue_properties (known after apply)

      + routing (known after apply)

      + share_properties (known after apply)
    }

  # azurerm_storage_data_lake_gen2_filesystem.gen2_data will be created
  + resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
      + default_encryption_scope = (known after apply)
      + group                    = (known after apply)
      + id                       = (known after apply)
      + name                     = "data"
      + owner                    = (known after apply)
      + storage_account_id       = (known after apply)

      + ace (known after apply)
    }

  # random_string.suffix will be created
  + resource "random_string" "suffix" {
      + id          = (known after apply)
      + length      = 2
      + lower       = true
      + min_lower   = 0
      + min_numeric = 0
      + min_special = 0
      + min_upper   = 0
      + number      = true
      + numeric     = true
      + result      = (known after apply)
      + special     = false
      + upper       = false
    }

Plan: 5 to add, 0 to change, 0 to destroy.

Changes to Outputs:
  + resource_group_name = (known after apply)

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Saved the plan to: terraform.plan

To perform exactly these actions, run the following command to apply:
    terraform apply "terraform.plan"
Releasing state lock. This may take a few moments...

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>
c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>terraform apply terraform.plan
Acquiring state lock. This may take a few moments...
random_string.suffix: Creating...
random_string.suffix: Creation complete after 0s [id=9d]
azurerm_resource_group.bdcc: Creating...
azurerm_resource_group.bdcc: Still creating... [10s elapsed]
azurerm_resource_group.bdcc: Creation complete after 10s [id=/subscriptions/1a10117a-61s/rg-]
azurerm_databricks_workspace.bdcc: Creating...
azurerm_storage_account.bdcc: Creating...
azurerm_databricks_workspace.bdcc: Still creating... [10s elapsed]
azurerm_storage_account.bdcc: Still creating... [10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [20s elapsed]
azurerm_storage_account.bdcc: Still creating... [20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [30s elapsed]
azurerm_storage_account.bdcc: Still creating... [30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [40s elapsed]
azurerm_storage_account.bdcc: Still creating... [40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [50s elapsed]
azurerm_storage_account.bdcc: Still creating... [50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m0s elapsed]
azurerm_storage_account.bdcc: Still creating... [1m0s elapsed]
azurerm_storage_account.bdcc: Creation complete after 1m7s [id=/subscriptions/1a15/resourceGroups/rg-de/providers/Microsoft.Storage/storageAccounts/d]
azurerm_storage_data_lake_gen2_filesystem.gen2_data: Creating...
azurerm_storage_data_lake_gen2_filesystem.gen2_data: Creation complete after 2s [id=https://dev.dfs.core.windows.net/data]
azurerm_databricks_workspace.bdcc: Still creating... [1m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [3m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [4m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [5m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [6m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [7m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [8m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [9m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [10m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [11m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [12m0s elapsed]
azurerm_databricks_workspace.bdcc: Creation complete after 12m1s [id=/subscriptions/15/resourceGroups/r/providers/Microsoft.Databricks/workspaces/dbw-dev-
Releasing state lock. This may take a few moments...

Apply complete! Resources: 5 added, 0 changed, 0 destroyed.

Outputs:

resource_group_name = "r"
```
Here are the created resources in the Azure GUI:

![dbricks_resources](https://github.com/user-attachments/assets/f5e071b2-ab4d-4ad5-93c2-7802ca2c8a1b)

Here you can see the GUI of the Databricks:

![databricks_gui](https://github.com/user-attachments/assets/2de7f45c-9294-4daf-ad7b-7be40cf2a8b0)

Then to being able to use secrets in the Databricks GUI, first I created an access token:

![accesstoken](https://github.com/user-attachments/assets/2998d547-85f8-412b-93c2-1ec720d3558e)

Then configured the Databricks CLi with the token:

```python
c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks configure --token
Databricks Host (should begin with https://): https://adb6.azuredatabricks.net
Token:

```

After that I created the actual secret-scope in the CLI:

```python
databricks secrets create-scope --scope streaming --initial-manage-principal users

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets list-scopes
Scope      Backend     KeyVault URL
---------  ----------  --------------
streaming  DATABRICKS  N/A
```

## Tasks completion

First I wrote the "streaming-simulation" upload locally. The method was to upload all the parquets to the destination container of a particular day, then having a small delay and upload the next batch.
I declared the sensitive paramaters in a .env file and loaded them in the actual python file.

```python
import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Name of the container in your Azure Blob Storage
container_name = os.getenv("CONTAINER_NAME")

# Local root folder containing your weather data (with subfolders like year=..., month=..., day=...)
local_root_folder = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/"

# Delay between days (in seconds)
delay_seconds = 3

# Create a BlobServiceClient using the connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

#MAIN LOGIC: Upload day-by-day with delay in between

# Walk through folders
for year in sorted(os.listdir(local_root_folder)):
    year_path = os.path.join(local_root_folder, year)
    if not os.path.isdir(year_path):
        continue  # Skip if not a directory

    for month in sorted(os.listdir(year_path)):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            print(f"Uploading data for: {day_path}")

            # Upload all .parquet files for the current day (no delay between files)
            for filename in os.listdir(day_path):
                if filename.endswith(".parquet"):
                    local_file_path = os.path.join(day_path, filename)

                    # Build the relative blob path (preserving folder structure inside 'hotel-weather/')
                    relative_path = os.path.relpath(local_file_path, local_root_folder)
                    blob_path = f"hotel-weather/{relative_path.replace(os.sep, '/')}"

                    print(f"Uploading: {blob_path}")

                    # Open the file and upload it to Azure Blob Storage
                    with open(local_file_path, "rb") as file:
                        container_client.upload_blob(name=blob_path, data=file, overwrite=True)

            print(f"Finished uploading files for {day_path}.\n")

            # Wait before moving to the next day
            time.sleep(delay_seconds)

print("All weather data successfully uploaded day by day!")
```

I tested it, here you can see the fraction of output of the code:

```python
Uploading data for: c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=29
Uploading: hotel-weather/year=2017/month=09/day=29/part-00023-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00026-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00099-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00123-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00144-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00148-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00151-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00156-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00176-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=29/part-00184-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Finished uploading files for c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=29.

Uploading data for: c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=30
Uploading: hotel-weather/year=2017/month=09/day=30/part-00023-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00026-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00099-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00123-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00144-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00148-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00151-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00156-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00176-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00184-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Finished uploading files for c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=30.

All weather data successfully uploaded day by day!

Process finished with exit code 0
```

And the uploaded data:

![batched_data_dire](https://github.com/user-attachments/assets/ac0efe0b-4663-4d67-8f14-7955295659d9)


With this simple code I checked the data and type of the columns:

```python
import pandas as pd
import glob

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
files=  glob.glob("c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017/month=09/day=30/part-00023-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet")

df = pd.read_parquet(files[0])
print(df.head())
```

The output:

```python
                address  avg_tmpr_c  avg_tmpr_f       city country geoHash             id  latitude  longitude                   name   wthr_date
0  Super 8 Manhattan Ks        21.0        69.8  Manhattan      US    9ygq  1322849927170  39.18027  -96.55681  200 Tuttle Creek Blvd  2017-09-30
```

Also checked the actual type of the columns:

```python
import pandas as pd

df= pd.read_parquet("c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017/month=09/day=30/part-00023-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet")

print(df.dtypes)
```

The output was:

```python
address        object
avg_tmpr_c    float64
avg_tmpr_f    float64
city           object
country        object
geoHash        object
id             object
latitude      float64
longitude     float64
name           object
wthr_date      object
dtype: object
```

Then I created the secrets to the secret-scope via CMD:

```python
c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets list-scopes
Scope      Backend     KeyVault URL
---------  ----------  --------------
streaming  DATABRICKS  N/A

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets put --scope streaming --key AZURE_STORAGE_ACCOUNT_NAME

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets put --scope streaming --key AZURE_STORAGE_ACCOUNT_KEY

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets put --scope streaming --key AZURE_CONTAINER_NAME

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>databricks secrets list --scope streaming
Key name                      Last updated
--------------------------  --------------
AZURE_CONTAINER_NAME         1745478036643
AZURE_STORAGE_ACCOUNT_KEY    1745477993780
AZURE_STORAGE_ACCOUNT_NAME   1745477909713

c:\data_eng\házi\6\m13_sparkstreaming_python_azure-master\terraform>
```
Then I inspeced the usage of the Auto Loader and wrote a script for the first task: 
### Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Storage Account. Using Spark calculate in Databricks Notebooks for each city each day

This code reads streaming weather data from Azure Blob Storage using Spark Structured Streaming and Auto Loader. It defines a schema for the Parquet input files, casts the weather date to timestamp, and extracts year, month, and day for grouping. It calculating the number of distinct hotels and temperature statistics (average, max, and min). Finally, it writes the aggregated results to the console every 20 seconds in complete output mode, with checkpointing enabled to track the stream’s progress. The goal was to not the overload the streaming processing unit, due to the limitations of resources of the free tier. That's teh reason ehy I used these options: ".option("cloudFiles.maxFilesPerTrigger", 15)", ".trigger(processingTime="20 seconds")". Also used another options to avoid the reproccessing of the datas: ".option("cloudFiles.includeExistingFiles", "false")", ".option("cloudFiles.useIncrementalListing", "true")".

```python
from pyspark.sql.functions import year, month, dayofmonth, col, approx_count_distinct, avg, max, min
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Read secrets for accessing Azure Blob Storage
storage_account = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_NAME")
storage_key = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_KEY")
container = dbutils.secrets.get(scope="streaming", key="AZURE_CONTAINER_NAME")

# Set Spark configuration to access Azure Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    storage_key
)

# Define schema for the input Parquet files
schema = StructType() \
    .add("address", StringType()) \
    .add("avg_tmpr_c", DoubleType()) \
    .add("avg_tmpr_f", DoubleType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("geoHash", StringType()) \
    .add("id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("name", StringType()) \
    .add("wthr_date", StringType())  # Will be casted to TimestampType later

# Load streaming data using Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.includeExistingFiles", "false")
      .option("cloudFiles.useIncrementalListing", "true")
      .option("cloudFiles.maxFilesPerTrigger", 15)
      .schema(schema)
      .load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/hotel-weather/"))

# Convert wthr_date string to TimestampType (to make it easier for grouping)
df = df.withColumn("wthr_date", col("wthr_date").cast(TimestampType()))

# Add year, month, and day columns for grouping
df_with_date_parts = df.withColumn("year", year("wthr_date")) \
    .withColumn("month", month("wthr_date")) \
    .withColumn("day", dayofmonth("wthr_date"))

# Aggregate data per city per day
aggregated = (
    df_with_date_parts
    .groupBy("city", "year", "month", "day")  # Grouping by city and the date parts
    .agg(
        approx_count_distinct("id").alias("distinct_hotels"),  # Counting distinct hotels
        avg("avg_tmpr_c").alias("avg_temp"),  # Average temperature
        max("avg_tmpr_c").alias("max_temp"),  # Max temperature
        min("avg_tmpr_c").alias("min_temp")   # Min temperature
    )
)

# Write the aggregated streaming output to console with complete mode
query = (
    aggregated.writeStream
    .outputMode("complete")  # Use complete mode for streaming aggregations
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", f"wasbs://{container}@{storage_account}.blob.core.windows.net/checkpoints/hotel-weather-agg")  # Set checkpoint directory
    .trigger(processingTime="20 seconds")
    .start()
)
```
I uploaded locally with a script the daily datas to the blob store, with 15 sec delay between the daily sets to simulate the day-by-day arrival of the datas:

```python
import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Name of the container in your Azure Blob Storage
container_name = os.getenv("CONTAINER_NAME")

# Local root folder containing your weather data (with subfolders like year=..., month=..., day=...)
local_root_folder = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/"

# Delay between days (in seconds)
delay_seconds = 15

# Create a BlobServiceClient using the connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

# Walk through folders
for year in sorted(os.listdir(local_root_folder)):
    year_path = os.path.join(local_root_folder, year)
    if not os.path.isdir(year_path):
        continue  # Skip if not a directory

    for month in sorted(os.listdir(year_path)):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            print(f"Uploading data for: {day_path}")

            # Upload all .parquet files for the current day (no delay between files)
            for filename in os.listdir(day_path):
                if filename.endswith(".parquet"):
                    local_file_path = os.path.join(day_path, filename)

                    # Build the relative blob path (preserving folder structure inside 'hotel-weather/')
                    relative_path = os.path.relpath(local_file_path, local_root_folder)
                    blob_path = f"hotel-weather/{relative_path.replace(os.sep, '/')}"

                    print(f"Uploading: {blob_path}")

                    # Open the file and upload it to Azure Blob Storage
                    with open(local_file_path, "rb") as file:
                        container_client.upload_blob(name=blob_path, data=file, overwrite=True)

            print(f"Finished uploading files for {day_path}.\n")

            # Wait before moving to the next day
            time.sleep(delay_seconds)

print("All weather data successfully uploaded day by day!")
```

My approach was to start both the local script and the notebook's cell simultanesly and observe the aggregated results on the console.

Here you can see some of the aggregated outputs. By the later there are mixed data of days due to the processing delays. The processing of the stream is carried out with micro batching, and due to the 20 sec of processing time options the data of different days were jammed.
```python
Batch: 0
-------------------------------------------
+-----------------+----+-----+---+---------------+--------+--------+--------+
|city             |year|month|day|distinct_hotels|avg_temp|max_temp|min_temp|
+-----------------+----+-----+---+---------------+--------+--------+--------+
|Abbeville        |2016|10   |1  |1              |18.8    |18.8    |18.8    |
|Jefferson        |2016|10   |1  |1              |14.9    |14.9    |14.9    |
|Long Beach       |2016|10   |1  |1              |20.2    |20.2    |20.2    |
|Springfield      |2016|10   |1  |1              |14.8    |14.8    |14.8    |
|Guernsey         |2016|10   |1  |1              |19.4    |19.4    |19.4    |
|Harrisonburg     |2016|10   |1  |1              |15.7    |15.7    |15.7    |
|Palm Harbor      |2016|10   |1  |2              |26.4    |26.4    |26.4    |
|Blackwell        |2016|10   |1  |1              |18.6    |18.6    |18.6    |
|Englewood        |2016|10   |1  |1              |16.9    |16.9    |16.9    |
|Fort Walton Beach|2016|10   |1  |1              |21.3    |21.3    |21.3    |
|Mobridge         |2016|10   |1  |1              |18.9    |18.9    |18.9    |
|Biloxi           |2016|10   |1  |1              |21.6    |21.6    |21.6    |
|Mobile           |2016|10   |1  |2              |20.5    |20.5    |20.5    |
|San Clemente     |2016|10   |1  |1              |19.8    |19.8    |19.8    |
|Plainville       |2016|10   |1  |1              |11.3    |11.3    |11.3    |
|Byron Center     |2016|10   |1  |1              |15.2    |15.2    |15.2    |
|Philadelphia     |2016|10   |1  |2              |15.4    |15.4    |15.4    |
|Ramey            |2016|10   |1  |1              |21.6    |21.6    |21.6    |
|Clatskanie       |2016|10   |1  |1              |10.9    |10.9    |10.9    |
|Enterprise       |2016|10   |1  |2              |3.8     |3.8     |3.8     |
+-----------------+----+-----+---+---------------+--------+--------+--------+

-------------------------------------------
Batch: 55
-------------------------------------------
+----------------+----+-----+---+---------------+------------------+--------+--------+
|city            |year|month|day|distinct_hotels|avg_temp          |max_temp|min_temp|
+----------------+----+-----+---+---------------+------------------+--------+--------+
|Paris           |2016|10   |31 |233            |10.699999999999994|10.7    |10.7    |
|West Yellowstone|2017|8    |12 |1              |16.4              |16.4    |16.4    |
|Vincennes       |2016|10   |23 |1              |7.1               |7.1     |7.1     |
|Oconomowoc      |2016|10   |21 |1              |5.6               |5.6     |5.6     |
|Canton          |2016|10   |28 |1              |18.7              |18.7    |18.7    |
|Auburn          |2016|10   |15 |1              |11.2              |11.2    |11.2    |
|Galena          |2016|10   |20 |1              |8.3               |8.3     |8.3     |
|Rangeley        |2017|8    |29 |2              |12.6              |12.6    |12.6    |
|Perry           |2017|8    |7  |1              |23.0              |23.0    |23.0    |
|Long Eddy       |2017|9    |6  |1              |13.9              |13.9    |13.9    |
|Navajo Dam      |2017|8    |19 |1              |24.6              |24.6    |24.6    |
|Burdett         |2017|8    |11 |1              |19.9              |19.9    |19.9    |
|New Orleans     |2017|9    |2  |3              |26.899999999999995|26.9    |26.9    |
|Forest City     |2017|8    |17 |1              |27.2              |27.2    |27.2    |
|Roanoke         |2017|9    |6  |1              |22.0              |22.0    |22.0    |
|Studio City     |2016|10   |9  |1              |24.0              |24.0    |24.0    |
|Warren          |2016|10   |30 |1              |9.6               |9.6     |9.6     |
|Navajo Dam      |2016|10   |25 |1              |13.5              |13.5    |13.5    |
|Honolulu        |2017|8    |31 |1              |26.3              |26.3    |26.3    |
|Batesville      |2017|8    |21 |1              |28.0              |28.0    |28.0    |
+----------------+----+-----+---+---------------+------------------+--------+--------+
only showing top 20 rows
```

Here you can see the dashboard of the whole streaming process:

![dboard_fasza](https://github.com/user-attachments/assets/aad97566-e63b-4e84-ac3a-81fedce935b0)

As you can see the input and processing rate was very similar, there was no overload during the streaming.

You can see the created datas in the blob storage:

![1_utani_data_cont](https://github.com/user-attachments/assets/b34b6de6-3a51-4bb6-b731-61e8c4d0d6cb)

![1_utani_data_cont2](https://github.com/user-attachments/assets/d6b978ec-ee94-4bf0-9fb0-b0e1c3b7cb5d)

![1_utani_data_cont3](https://github.com/user-attachments/assets/8c9c894d-50f6-4bf5-9d2e-44a4dacea115)

Another strange phenomenon is that the avg,max and min temparetures are the same in most of the cases, because the low number of patterns - as you can see there many cases where there are only 1 hotel in the particular city - and there might be, that the logged datas are just the same as it happened by Paris. I tested the data prior this task execution and even though you can see, that it appears 233 times for the day 2016-10-31, all the temperature datas are the same:

Here you can see the appearance for the particular day:
```python
import pandas as pd

# Parquet file reading
df = pd.read_parquet("c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2016/month=10/day=31/")

# Choose a city
city_name = "Paris"

# Number of appearance
count = (df["city"] == city_name).sum()

print(f"The city '{city_name}' appears {count} times in the dataset.")
```
```python
C:\data_eng\házi\6\.venv\Scripts\python.exe C:\data_eng\házi\6\counting.py 
The city 'Paris' appears 232 times in the dataset.

Process finished with exit code 0
```

Here you can see the max temperature for that day:
```python
import pandas as pd

parquet_path = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2016/month=10/day=31/"
city_name = "Paris"  # the actual city's name

# Parquet reading
df = pd.read_parquet(parquet_path, engine="pyarrow")

# Filter for the city
city_df = df[df["city"] == city_name]

# Max temp calculation
max_temp = city_df["avg_tmpr_c"].max()
print(f"{city_name} Max temp in the city: {max_temp} °C")
```
```python
Paris Max temp in the city: 10.7 °C

Process finished with exit code 0
```

And here you can see the minimum temperature for Paris for that particular day:
```python
import pandas as pd

parquet_path = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2016/month=10/day=31/"
city_name = "Paris"  # the actual city's name

# Parquet reading
df = pd.read_parquet(parquet_path, engine="pyarrow")

# Filter for the city
city_df = df[df["city"] == city_name]

# Min temp calculation
min_temp = city_df["avg_tmpr_c"].min()
print(f"{city_name} Min temp in the city: {min_temp} °C")
```
```python
Paris Max temp in the city: 10.7 °C

Process finished with exit code 0
```

### Execution plan:

After the writeStream starting I won't be able to call the aggregated.explain(), because the aggregated would work as a process by this time, so I made a batch-only version from the aggregated DataFrame (with the same operation logic), but instead of readStream I got the data from the read. Also I used just a small collection of data for this task.

The actual cell:

```python
df_batch = (spark.read
            .schema(schema)
            .parquet(f"wasbs://{container}@{storage_account}.blob.core.windows.net/hotel-weather/"))

df_batch = df_batch.withColumn("wthr_date", col("wthr_date").cast(TimestampType()))

df_batch = df_batch.withColumn("year", year("wthr_date")) \
                   .withColumn("month", month("wthr_date")) \
                   .withColumn("day", dayofmonth("wthr_date"))

aggregated_batch = (
    df_batch.groupBy("city", "year", "month", "day")
    .agg(
        approx_count_distinct("id").alias("distinct_hotels"),
        avg("avg_tmpr_c").alias("avg_temp"),
        max("avg_tmpr_c").alias("max_temp"),
        min("avg_tmpr_c").alias("min_temp")
    )
)

# By run the explain to  I get the execution plan
aggregated_batch.explain(True)

```

The actual execution plan with comments:

```python
== Parsed Logical Plan == 
# Aggregation operation on city, year, month, and day to calculate distinct hotels, average temperature, max temperature, and min temperature
# This step is computationally expensive because it involves grouping the data by multiple columns and performing aggregation functions on potentially large datasets.
'Aggregate ['city, 'year, 'month, 'day], ['city, 'year, 'month, 'day, 'approx_count_distinct('id) AS distinct_hotels#285638, 'avg('avg_tmpr_c) AS avg_temp#285639, 'max('avg_tmpr_c) AS max_temp#285640, 'min('avg_tmpr_c) AS min_temp#285641]
+- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year#285582, month#285595, dayofmonth(cast(wthr_date#285569 as date)) AS day#285609]
   +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year#285582, month(cast(wthr_date#285569 as date)) AS month#285595]
      +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year(cast(wthr_date#285569 as date)) AS year#285582]
         +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, cast(wthr_date#285557 as timestamp) AS wthr_date#285569]
            +- Relation [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285557] parquet

== Analyzed Logical Plan == 
# Data types of each column after analysis: city (string), year (int), month (int), day (int), distinct_hotels (bigint), avg_temp (double), max_temp (double), min_temp (double)
# Analysis step ensures data types are correct, but does not involve heavy computation.
Aggregate [city#285550, year#285582, month#285595, day#285609], [city#285550, year#285582, month#285595, day#285609, approx_count_distinct(id#285553, 0.05, 0, 0) AS distinct_hotels#285638L, avg(avg_tmpr_c#285548) AS avg_temp#285639, max(avg_tmpr_c#285548) AS max_temp#285640, min(avg_tmpr_c#285548) AS min_temp#285641]
+- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year#285582, month#285595, dayofmonth(cast(wthr_date#285569 as date)) AS day#285609]
   +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year#285582, month(cast(wthr_date#285569 as date)) AS month#285595]
      +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285569, year(cast(wthr_date#285569 as date)) AS year#285582]
         +- Project [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, cast(wthr_date#285557 as timestamp) AS wthr_date#285569]
            +- Relation [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285557] parquet

== Optimized Logical Plan == 
# The plan after optimization, focusing on better performance for aggregate operations
# Optimizing the grouping and aggregate steps to minimize unnecessary data shuffling and improve query efficiency.
Aggregate [city#285550, year#285582, month#285595, day#285609], [city#285550, year#285582, month#285595, day#285609, approx_count_distinct(id#285553, 0.05) AS distinct_hotels#285638L, avg(avg_tmpr_c#285548) AS avg_temp#285639, max(avg_tmpr_c#285548) AS max_temp#285640, min(avg_tmpr_c#285548) AS min_temp#285641]
+- Project [avg_tmpr_c#285548, city#285550, id#285553, year(cast(wthr_date#285569 as date)) AS year#285582, month(cast(wthr_date#285569 as date)) AS month#285595, dayofmonth(cast(wthr_date#285569 as date)) AS day#285609]
   +- Project [avg_tmpr_c#285548, city#285550, id#285553, cast(wthr_date#285557 as timestamp) AS wthr_date#285569]
      +- Relation [address#285547, avg_tmpr_c#285548, avg_tmpr_f#285549, city#285550, country#285551, geoHash#285552, id#285553, latitude#285554, longitude#285555, name#285556, wthr_date#285557] parquet

== Physical Plan == 
# Initial stage of the physical plan, considering parallel execution and partitioning
# The physical plan is where Spark decides how to execute the plan, including how to partition data across multiple nodes and handle shuffling.
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan == 
   HashAggregate(keys=[city#285550, year#285582, month#285595, day#285609], functions=[finalmerge_approx_count_distinct(merge buffer#285872) AS approx_count_distinct(id#285553, 0.05)#285746L, finalmerge_avg(merge sum#285864, count#285865L) AS avg(avg_tmpr_c#285548)#285747, finalmerge_max(merge max#285867) AS max(avg_tmpr_c#285548)#285748, finalmerge_min(merge min#285869) AS min(avg_tmpr_c#285548)#285749], output=[city#285550, year#285582, month#285595, day#285609, distinct_hotels#285638L, avg_temp#285639, max_temp#285640, min_temp#285641])
   +- Exchange hashpartitioning(city#285550, year#285582, month#285595, day#285609, 200), ENSURE_REQUIREMENTS, [plan_id=108021]
      +- HashAggregate(keys=[city#285550, year#285582, month#285595, day#285609], functions=[partial_approx_count_distinct(id#285553, 0.05) AS buffer#285872, partial_avg(avg_tmpr_c#285548) AS (sum#285864, count#285865L), partial_max(avg_tmpr_c#285548) AS max#285867, partial_min(avg_tmpr_c#285548) AS min#285869], output=[city#285550, year#285582, month#285595, day#285609, buffer#285872, sum#285864, count#285865L, max#285867, min#285869])
         +- Project [avg_tmpr_c#285548, city#285550, id#285553, year(cast(wthr_date#285569 as date)) AS year#285582, month(cast(wthr_date#285569 as date)) AS month#285595, dayofmonth(cast(wthr_date#285569 as date)) AS day#285609]
            +- Project [avg_tmpr_c#285548, city#285550, id#285553, cast(wthr_date#285557 as timestamp) AS wthr_date#285569]
               +- FileScan parquet [avg_tmpr_c#285548,city#285550,id#285553,wthr_date#285557] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[wasbs://[REDACTED]@[REDACTED].blob.core.windows.net/hotel-weather], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<avg_tmpr_c:double,city:string,id:string,wthr_date:string>
```
Aggregation (Aggregate):
This step groups the data by certain columns and calculates aggregated values (e.g., count, average, max, min). It can be an expensive operation, as Spark needs to shuffle a lot of data across the cluster to perform these calculations, especially with large datasets.

Shuffling (Exchange):
The data needs to be redistributed across nodes so that related records are processed together. This is an expensive operation because it takes a lot of time and resources, especially when working with large groups of data.

FileScan:
Reading Parquet files can be time-consuming, especially when dealing with large amounts of data or distributed storage. The efficiency of file reading depends on how the data is partitioned and stored.

### Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city)

By this task I also used the local uploading script to simulate to data streaming, then I created a streaming pipeline in Databricks using PySpark to process hotel and weather data from Azure Blob Storage. It reads the data in Parquet format, casts and extracts timestamps, and aggregates the information by city and date to calculate the number of distinct hotels and temperature statistics. The results are continuously written into a Delta table with checkpointing enabled. Finally, the aggregated data is read in batch mode, registered as a temporary SQL view, and queried to find the top 10 cities with the most distinct hotels and to retrieve detailed statistics for specific cities like Paris. Finally I created visualization for each 10 cities. 

The local uploading script:

```python
import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Name of the container in your Azure Blob Storage
container_name = os.getenv("CONTAINER_NAME")

# Local root folder containing your weather data (with subfolders like year=..., month=..., day=...)
local_root_folder = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/"

# Delay between days (in seconds)
delay_seconds = 15

# Create a BlobServiceClient using the connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

#MAIN LOGIC: Upload day-by-day with delay in between

# Walk through folders
for year in sorted(os.listdir(local_root_folder)):
    year_path = os.path.join(local_root_folder, year)
    if not os.path.isdir(year_path):
        continue  # Skip if not a directory

    for month in sorted(os.listdir(year_path)):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            print(f"Uploading data for: {day_path}")

            # Upload all .parquet files for the current day (no delay between files)
            for filename in os.listdir(day_path):
                if filename.endswith(".parquet"):
                    local_file_path = os.path.join(day_path, filename)

                    # Build the relative blob path (preserving folder structure inside 'hotel-weather/')
                    relative_path = os.path.relpath(local_file_path, local_root_folder)
                    blob_path = f"hotel-weather/{relative_path.replace(os.sep, '/')}"

                    print(f"Uploading: {blob_path}")

                    # Open the file and upload it to Azure Blob Storage
                    with open(local_file_path, "rb") as file:
                        container_client.upload_blob(name=blob_path, data=file, overwrite=True)

            print(f"Finished uploading files for {day_path}.\n")

            # Wait before moving to the next day
            time.sleep(delay_seconds)

print("All weather data successfully uploaded day by day!")
```

The first cell in the databricks:

```python
from pyspark.sql.functions import year, month, dayofmonth, col, approx_count_distinct, avg, max, min, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Read secrets for accessing Azure Blob Storage
storage_account = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_NAME")
storage_key = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_KEY")
container = dbutils.secrets.get(scope="streaming", key="AZURE_CONTAINER_NAME")

# Set Spark configuration to access Azure Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    storage_key
)

# Define schema for the input Parquet files
schema = StructType() \
    .add("address", StringType()) \
    .add("avg_tmpr_c", DoubleType()) \
    .add("avg_tmpr_f", DoubleType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("geoHash", StringType()) \
    .add("id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("name", StringType()) \
    .add("wthr_date", StringType())  # Will be casted to TimestampType later

# Streaming reading from hotel-weather directory
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .schema(schema)
      .option("cloudFiles.maxFilesPerTrigger", 15)
      .load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/hotel-weather/"))

# Timestamp type convert and date extract
df = df.withColumn("wthr_date", col("wthr_date").cast(TimestampType()))
df = df.withColumn("date", to_date("wthr_date"))

# Aggregation by city and date
agg_df = df.groupBy("city", "date").agg(
    approx_count_distinct("id").alias("distinct_hotels"),
    avg("avg_tmpr_c").alias("avg_temp"),
    max("avg_tmpr_c").alias("max_temp"),
    min("avg_tmpr_c").alias("min_temp")
)

```
The second cell:

```python
# Start a streaming write query on the aggregated DataFrame (agg_df)
agg_query = (agg_df.writeStream
             # Use "complete" output mode to write the full aggregated result each time (suitable for aggregations)
             .outputMode("complete")
             # Specify the sink format as Delta Lake
             .format("delta")
             # Set the location to store checkpoint data (to maintain streaming state and recover from failures)
             .option("checkpointLocation", f"wasbs://{container}@{storage_account}.blob.core.windows.net/checkpoints/hotel-weather-agg")  
             # Set the output path in Azure Blob Storage where the Delta table will be written
             .option("path", f"wasbs://{container}@{storage_account}.blob.core.windows.net/delta/agg_city")
             .trigger(processingTime="20 seconds")
             # Start the streaming query
             .start())
```
The streaming progress of the incoming data:

![vis_dashb](https://github.com/user-attachments/assets/550c99a5-533a-457f-a200-267a7980bbb9)

Third cell:

```python
  # Read the aggregated Delta table as a batch DataFrame from Azure Blob Storage
agg_batch_df = spark.read.format("delta").load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/delta/agg_city")

# Register the DataFrame as a temporary SQL view called "agg_view"
# This allows running SQL queries directly on the DataFrame using Spark SQL
agg_batch_df.createOrReplaceTempView("agg_view")
```

Fourth cell:

```python
%sql
-- Find top 10 cities with the most distinct hotels
SELECT city, SUM(distinct_hotels) AS total_hotels
FROM agg_view
GROUP BY city
ORDER BY total_hotels DESC
LIMIT 10
```
The output of the query:

![vis_top10](https://github.com/user-attachments/assets/fc6b7e99-d65e-4a8f-8503-1d4663973e7b)

Here come the query results and visualizations of the top 20 cities (by some cases I zoomed in the chart to a smaller time range to see the actual bars).

### Paris:

![par_table](https://github.com/user-attachments/assets/8714e779-ade5-4a32-8ab4-21bc62e439f5)

![vis_par](https://github.com/user-attachments/assets/5da9839f-4e5b-40b2-b275-b48355f089e2)

### London:

![tab_lon](https://github.com/user-attachments/assets/efb75a2d-e47c-47e1-a976-496ee981dc37)

![vis_lon](https://github.com/user-attachments/assets/4bf0a9cb-4bcd-499e-b63f-d9b644d63ce2)

### Milan:

![tab_mil](https://github.com/user-attachments/assets/6b2837b9-5677-4af8-92ad-05a90f5eace0)

![vis_mil](https://github.com/user-attachments/assets/a0afb36b-5330-494b-ad0c-0d3ce924f2a8)

### Barcelona:

![tab_bar](https://github.com/user-attachments/assets/9d2b230b-dbcd-4e80-bd92-6d9800ad7909)

![vis_bar](https://github.com/user-attachments/assets/dffcb011-0202-4726-bd58-72f416152651)

### Amsterdam:

![tab_amst](https://github.com/user-attachments/assets/992d1571-52b1-4d56-a1c2-e6b5aac7e6a6)

![vis_amst](https://github.com/user-attachments/assets/4b6e0514-ad80-49cc-8442-1fb30a24449d)

### Paddington:

![tab_pad](https://github.com/user-attachments/assets/aeb8a252-bf7d-4a5d-a491-eded3aa1b3f7)

![vis_pad](https://github.com/user-attachments/assets/bce8a28e-7dbf-43b8-b382-f7338bcfe6ef)

### Springfield:

![tab_spr](https://github.com/user-attachments/assets/6a19284f-d2e9-41f1-874d-5d98c35d6830)

![vis_spr](https://github.com/user-attachments/assets/9a366271-de4c-49e2-8441-b1feba202cb6)

### Houston:

![tab_hou](https://github.com/user-attachments/assets/68f330ff-6683-4c45-bbff-c9eb9009b495)

![vis_hou](https://github.com/user-attachments/assets/30b0e324-629a-4d19-8185-fcb7339f08bb)

### Alberquerque:

![tab_alb](https://github.com/user-attachments/assets/5816f888-ff38-4a60-923b-6dc5e1592418)

![vis_alb](https://github.com/user-attachments/assets/88bb9e3d-b23b-4800-98f6-969d22fb46ea)

### Virginia Beach:

![tabl_vb](https://github.com/user-attachments/assets/57d3118c-c734-4dfb-95b4-71e98517670d)

![vis_vb](https://github.com/user-attachments/assets/7ad6ff63-e791-4563-aafe-c4edd9714bcf)

### San Francisco:

![tab_SF](https://github.com/user-attachments/assets/1bd6c605-60ad-4fd1-91d9-08f9a002fb25)

![vis_sf](https://github.com/user-attachments/assets/54458137-6a5f-4d79-ac52-99da66b1c904)


Information about the used cluster:

![cluster](https://github.com/user-attachments/assets/7f5160f6-0a5b-4dc0-a3ab-a6a7f0c0d30f)

By this taks I applied the execution plan by the aggregation part and the streaming query.

Execution plan of the aggregation:

```python
== Parsed Logical Plan ==

# Perform aggregation: group by 'city' and 'date', and calculate distinct hotel count, average, max, and min temperatures.
'Aggregate ['city, 'date], ['city, 'date, 'approx_count_distinct('id) AS distinct_hotels, 'avg('avg_tmpr_c) AS avg_temp, 'max('avg_tmpr_c) AS max_temp, 'min('avg_tmpr_c) AS min_temp]

# Project the necessary columns and parse 'wthr_date' into a 'date' type.
+- ~Project [..., to_date(wthr_date) AS date]

# Cast the original 'wthr_date' field from string to timestamp.
   +- ~Project [..., cast(wthr_date as timestamp) AS wthr_date]

# Read the streaming data from a cloud file source (Parquet format) using Auto Loader.
      +- ~StreamingRelation DataSource(...)

== Analyzed Logical Plan ==

# Define output columns and types after analyzing the logical structure.
city: string, date: date, distinct_hotels: bigint, avg_temp: double, max_temp: double, min_temp: double

# Aggregation after resolving expressions and data types.
~Aggregate [city, date], [city, date, approx_count_distinct(id), avg(avg_tmpr_c), max(avg_tmpr_c), min(avg_tmpr_c)]

# Project the necessary columns and parse 'wthr_date' into a 'date' type.
+- ~Project [..., to_date(wthr_date) AS date]

# Cast 'wthr_date' string into timestamp.
   +- ~Project [..., cast(wthr_date as timestamp) AS wthr_date]

# Read streaming data from the cloud (Auto Loader).
      +- ~StreamingRelation DataSource(...)

== Optimized Logical Plan ==

# Final aggregation: keeping only essential columns and computation for efficiency.
~Aggregate [city, date], [city, date, approx_count_distinct(id), avg(avg_tmpr_c), max(avg_tmpr_c), min(avg_tmpr_c)]

# Drop unnecessary columns to minimize data movement; cast 'wthr_date' properly.
+- ~Project [avg_tmpr_c, city, id, cast(cast(wthr_date as timestamp) as date) AS date]

# Streaming data input remains unchanged.
   +- ~StreamingRelation DataSource(...)

== Physical Plan ==

# Final stage aggregation: produces the final output.
*(4) HashAggregate(keys=[city, date], functions=[finalmerge_...])

# Save final aggregated state to the State Store (for continuous streaming processing).
+- StateStoreSave [city, date], state info [...], Append, ...

# Merge aggregation buffers from previous steps.
   +- *(3) HashAggregate(keys=[city, date], functions=[merge_...])

# Restore previous partial aggregation states from State Store (important for fault-tolerance).
      +- StateStoreRestore [city, date], state info [...], ...

# Merge intermediate results grouped by 'city' and 'date'.
         +- *(2) HashAggregate(keys=[city, date], functions=[merge_...])

# Shuffle data by 'city' and 'date' to ensure correct partitioning for stateful operations.
            +- Exchange hashpartitioning(city, date, 200), REQUIRED_BY_STATEFUL_OPERATOR

# Partial aggregation: compute local aggregates before shuffling (efficient).
               +- *(1) HashAggregate(keys=[city, date], functions=[partial_...])

# Project necessary fields and cast 'wthr_date' properly.
                  +- *(1) Project [avg_tmpr_c, city, id, cast(cast(wthr_date as timestamp) as date)]

# Stream data read from cloud storage using Auto Loader.
                     +- StreamingRelation cloudFiles, [...]

```

Execution plan of the streaming query:

```python
# Final aggregation stage: compute final values (distinct hotels, avg temp, max temp, min temp) after merging intermediate results
*(4) HashAggregate(keys=[city#351571, date#189], functions=[finalmerge_approx_count_distinct(merge buffer#351809) AS approx_count_distinct(id#351574, 0.05)#322L, finalmerge_avg(merge sum#351801, count#351802L) AS avg(avg_tmpr_c#351569)#323, finalmerge_max(merge max#351804) AS max(avg_tmpr_c#351569)#324, finalmerge_min(merge min#351806) AS min(avg_tmpr_c#351569)#325], output=[city#351571, date#189, distinct_hotels#214L, avg_temp#215, max_temp#216, min_temp#217])

# Save the aggregated state into State Store (for fault-tolerant recovery and exactly-once semantics)
+- StateStoreSave [city#351571, date#189], state info [ checkpoint = wasbs://[REDACTED]@[REDACTED].blob.core.windows.net/checkpoints/hotel-weather-agg/state, runId = 67a0f8a3-8500-49d9-871c-eafcf831b2cc, opId = 0, ver = 65, numPartitions = 200], Complete, 0, 0, 2

    # Merge intermediate aggregation results from previous state
    +- *(3) HashAggregate(keys=[city#351571, date#189], functions=[merge_approx_count_distinct(merge buffer#351809) AS buffer#351809, merge_avg(merge sum#351801, count#351802L) AS (sum#351801, count#351802L), merge_max(merge max#351804) AS max#351804, merge_min(merge min#351806) AS min#351806], output=[city#351571, date#189, buffer#351809, sum#351801, count#351802L, max#351804, min#351806])

        # Restore previously saved state from State Store (for incremental aggregation)
        +- StateStoreRestore [city#351571, date#189], state info [ checkpoint = wasbs://[REDACTED]@[REDACTED].blob.core.windows.net/checkpoints/hotel-weather-agg/state, runId = 67a0f8a3-8500-49d9-871c-eafcf831b2cc, opId = 0, ver = 65, numPartitions = 200], 2

            # Partial merge of aggregation across partitions after shuffle
            +- *(2) HashAggregate(keys=[city#351571, date#189], functions=[merge_approx_count_distinct(merge buffer#351809) AS buffer#351809, merge_avg(merge sum#351801, count#351802L) AS (sum#351801, count#351802L), merge_max(merge max#351804) AS max#351804, merge_min(merge min#351806) AS min#351806], output=[city#351571, date#189, buffer#351809, sum#351801, count#351802L, max#351804, min#351806])

                # Shuffle the data by (city, date) keys for aggregation (required by the stateful operation)
                +- Exchange hashpartitioning(city#351571, date#189, 200), REQUIRED_BY_STATEFUL_OPERATOR, [plan_id=27405]

                    # First step of aggregation: compute partial aggregates within each partition
                    +- *(1) HashAggregate(keys=[city#351571, date#189], functions=[partial_approx_count_distinct(id#351574, 0.05) AS buffer#351809, partial_avg(avg_tmpr_c#351569) AS (sum#351801, count#351802L), partial_max(avg_tmpr_c#351569) AS max#351804, partial_min(avg_tmpr_c#351569) AS min#351806], output=[city#351571, date#189, buffer#351809, sum#351801, count#351802L, max#351804, min#351806])

                        # Select necessary columns and cast the date field properly
                        +- *(1) Project [avg_tmpr_c#351569, city#351571, id#351574, cast(cast(wthr_date#351578 as timestamp) as date) AS date#189]

                            # Convert from columnar batches to rows (necessary for aggregation processing)
                            +- *(1) ColumnarToRow

                                # Read parquet files containing weather data
                                +- FileScan parquet [avg_tmpr_c#351569,city#351571,id#351574,wthr_date#351578] Batched: true, DataFilters: [], Format: Parquet, Location: CloudFilesSourceFileIndex(1 paths)[wasbs://[REDACTED]@[REDACTED].blob.core.windows.net/hotel-weather], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<avg_tmpr_c:double,city:string,id:string,wthr_date:string>
```

### CI/CD

For the CI/CD task I made 3 notebooks, 1 for the first task and 2 for the second.

Notebook for the task Using Spark calculate in Databricks Notebooks for each city each day:

```python
from pyspark.sql.functions import year, month, dayofmonth, col, approx_count_distinct, avg, max, min
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Read secrets for accessing Azure Blob Storage
storage_account = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_NAME")
storage_key = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_KEY")
container = dbutils.secrets.get(scope="streaming", key="AZURE_CONTAINER_NAME")

# Set Spark configuration to access Azure Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    storage_key
)

# Define schema for the input Parquet files
schema = StructType() \
    .add("address", StringType()) \
    .add("avg_tmpr_c", DoubleType()) \
    .add("avg_tmpr_f", DoubleType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("geoHash", StringType()) \
    .add("id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("name", StringType()) \
    .add("wthr_date", StringType())  # Will be casted to TimestampType later

# Load streaming data using Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.includeExistingFiles", "false")
      .option("cloudFiles.useIncrementalListing", "true")
      .option("cloudFiles.maxFilesPerTrigger", 15)
      .schema(schema)
      .load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/hotel-weather/"))

# Convert wthr_date string to TimestampType (to make it easier for grouping)
df = df.withColumn("wthr_date", col("wthr_date").cast(TimestampType()))

# Add year, month, and day columns for grouping
df_with_date_parts = df.withColumn("year", year("wthr_date")) \
    .withColumn("month", month("wthr_date")) \
    .withColumn("day", dayofmonth("wthr_date"))

# Add watermark to handle late data
df_with_watermark = df_with_date_parts.withWatermark("wthr_date", "1 day")  # Watermark for late data handling

# Aggregate data per city per day
aggregated = (
    df_with_watermark
    .groupBy("city", "year", "month", "day")  # Grouping by city and the date parts
    .agg(
        approx_count_distinct("id").alias("distinct_hotels"),  # Counting distinct hotels
        avg("avg_tmpr_c").alias("avg_temp"),  # Average temperature
        max("avg_tmpr_c").alias("max_temp"),  # Max temperature
        min("avg_tmpr_c").alias("min_temp")   # Min temperature
    )
)

# Write the aggregated streaming output to console with complete mode
query = (
    aggregated.writeStream
    .outputMode("complete")  # Use complete mode for streaming aggregations
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", f"wasbs://{container}@{storage_account}.blob.core.windows.net/checkpoints/hotel-weather-agg")  # Set checkpoint directory
    .trigger(processingTime="20 seconds")
    .start()
)
```

For the second task, Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city) I made two notebooks to being able to initiate the data reading from delta table and the creation of the temporary table whenever we need, independently the reading, streaming processes. Also ptimized the query of the 10 biggest city querys to execute them in a single for cycle:

```python
from pyspark.sql.functions import year, month, dayofmonth, col, approx_count_distinct, avg, max, min, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Read secrets for accessing Azure Blob Storage
storage_account = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_NAME")
storage_key = dbutils.secrets.get(scope="streaming", key="AZURE_STORAGE_ACCOUNT_KEY")
container = dbutils.secrets.get(scope="streaming", key="AZURE_CONTAINER_NAME")

# Set Spark configuration to access Azure Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    storage_key
)

# Define schema for the input Parquet files
schema = StructType() \
    .add("address", StringType()) \
    .add("avg_tmpr_c", DoubleType()) \
    .add("avg_tmpr_f", DoubleType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("geoHash", StringType()) \
    .add("id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("name", StringType()) \
    .add("wthr_date", StringType())  # Will be casted to TimestampType later

# Streaming reading from hotel-weather directory
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .schema(schema)
      .option("cloudFiles.maxFilesPerTrigger", 15)
      .load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/hotel-weather/"))

# Timestamp type convert and date extract
df = df.withColumn("wthr_date", col("wthr_date").cast(TimestampType()))
df = df.withColumn("date", to_date("wthr_date"))

# Aggregation by city and date
agg_df = df.groupBy("city", "date").agg(
    approx_count_distinct("id").alias("distinct_hotels"),
    avg("avg_tmpr_c").alias("avg_temp"),
    max("avg_tmpr_c").alias("max_temp"),
    min("avg_tmpr_c").alias("min_temp")
)

# Start a streaming write query on the aggregated DataFrame (agg_df)
agg_query = (agg_df.writeStream
             # Use "complete" output mode to write the full aggregated result each time (suitable for aggregations)
             .outputMode("complete")
             # Specify the sink format as Delta Lake
             .format("delta")
             # Set the location to store checkpoint data (to maintain streaming state and recover from failures)
             .option("checkpointLocation", f"wasbs://{container}@{storage_account}.blob.core.windows.net/checkpoints/hotel-weather-agg")  
             # Set the output path in Azure Blob Storage where the Delta table will be written
             .option("path", f"wasbs://{container}@{storage_account}.blob.core.windows.net/delta/agg_city")
             .trigger(processingTime="20 seconds")
             # Start the streaming query
             .start())

```

```python
# Read the aggregated Delta table as a batch DataFrame from Azure Blob Storage
agg_batch_df = spark.read.format("delta").load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/delta/agg_city")

# Register the DataFrame as a temporary SQL view called "agg_view"
# This allows running SQL queries directly on the DataFrame using Spark SQL
agg_batch_df.createOrReplaceTempView("agg_view")

# Run the query and save the result in a variable
result = spark.sql("""
SELECT city, SUM(distinct_hotels) AS total_hotels
FROM agg_view
GROUP BY city
ORDER BY total_hotels DESC
LIMIT 10
""")

# Save the cities' names in a list
top_10_cities = [row['city'] for row in result.collect()]

# Print the list
print(top_10_cities)

# Iterate through the cities and display the aggregated data
for city in top_10_cities:
    query = f"""
    SELECT date, distinct_hotels, avg_temp, max_temp, min_temp
    FROM agg_view
    WHERE city = '{city}'
    ORDER BY date
    """
    display(sql(query))  

```

I also made a terraform main.tf terraform.tfvars and variables.tf files for the CI/CD execution.

Here you can see the actual main.tf:

```python
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.0.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.0.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rgy"
    storage_account_name = "de"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}

  subscription_id = var.AZURE_SUBSCRIPTION_ID
  tenant_id       = var.AZURE_TENANT_ID
  client_id       = var.AZURE_CLIENT_ID
  client_secret   = var.AZURE_CLIENT_SECRET
}

provider "databricks" {
  host  = var.DATABRICKS_HOST
  token = var.DATABRICKS_TOKEN
}


data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "stream" {
  cluster_name           = "Azure_Spark_Streaming"
  spark_version          = data.databricks_spark_version.latest_lts.id 
  node_type_id           = "Standard_E4d_v4"
  autotermination_minutes = 30

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

resource "azurerm_storage_account" "Azure_Spark_Streaming" {
  name                     = var.STORAGE_ACCOUNT_NAME
  resource_group_name       = var.RESOURCE_GROUP_NAME
  location                 = "West Europe"
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "data" {
  name                  = ""
  storage_account_name  = azurerm_storage_account.Azure_Spark_Streaming.name
  container_access_type = "container"
}


resource "databricks_notebook" "Task_1" {
  path     = "/cicd/Task_1"
  source   = "${path.module}/Task_1.dbc"
}

resource "databricks_notebook" "Task_2_a" {
  path     = "/cicd/Task_2_a"
  source   = "${path.module}/Task_2_a.dbc"
}

resource "databricks_notebook" "Task_2_b" {
  path     = "/cicd/Task_2_b"
  source   = "${path.module}/Task_2_b.dbc"
}

resource "databricks_job" "Task_1" {
  name = "Task_1"

  task {
    task_key = "Task_1"
    
    notebook_task {
      notebook_path = databricks_notebook.Task_1.path
    }

    existing_cluster_id = databricks_cluster.stream.id
  }
}

resource "databricks_job" "Task_2_a" {
  name = "Task_2_a"

  task {
    task_key = "Task_2_a"
    
    notebook_task {
      notebook_path = databricks_notebook.Task_2_a.path
    }

    existing_cluster_id = databricks_cluster.stream.id
  }
}

resource "databricks_job" "Task_2_b" {
  name = "Task_2_b"

  task {
    task_key = "Task_2_b"
    
    notebook_task {
      notebook_path = databricks_notebook.Task_2_b.path
    }

    existing_cluster_id = databricks_cluster.stream.id
  }
}
```

For the simulation of the streaming data I used the same python script as by the former executions.

I run the terraform init, terraform plan, then the terraform apply command:

```python
c:\data_eng\házi\6>terraform init
Initializing the backend...
Initializing provider plugins...
- Reusing previous version of hashicorp/azurerm from the dependency lock file
- Reusing previous version of databricks/databricks from the dependency lock file
- Using previously-installed databricks/databricks v1.75.0
- Using previously-installed hashicorp/azurerm v4.27.0

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.

c:\data_eng\házi\6>terraform plan
data.databricks_spark_version.latest_lts: Reading...
data.databricks_spark_version.latest_lts: Read complete after 0s [id=15.4.x-scala2.12]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # azurerm_storage_account.Azure_Spark_Streaming will be created
  + resource "azurerm_storage_account" "Azure_Spark_Streaming" {
      + access_tier                        = (known after apply)
      + account_kind                       = "StorageV2"
      + account_replication_type           = "LRS"
      + account_tier                       = "Standard"
      + allow_nested_items_to_be_public    = true
      + cross_tenant_replication_enabled   = false
      + default_to_oauth_authentication    = false
      + dns_endpoint_type                  = "Standard"
      + https_traffic_only_enabled         = true
      + id                                 = (known after apply)
      + infrastructure_encryption_enabled  = false
      + is_hns_enabled                     = false
      + large_file_share_enabled           = (known after apply)
      + local_user_enabled                 = true
      + location                           = "westeurope"
      + min_tls_version                    = "TLS1_2"
      + name                               = ""
      + nfsv3_enabled                      = false
      + primary_access_key                 = (sensitive value)
      + primary_blob_connection_string     = (sensitive value)
      + primary_blob_endpoint              = (known after apply)
      + primary_blob_host                  = (known after apply)
      + primary_blob_internet_endpoint     = (known after apply)
      + primary_blob_internet_host         = (known after apply)
      + primary_blob_microsoft_endpoint    = (known after apply)
      + primary_blob_microsoft_host        = (known after apply)
      + primary_connection_string          = (sensitive value)
      + primary_dfs_endpoint               = (known after apply)
      + primary_dfs_host                   = (known after apply)
      + primary_dfs_internet_endpoint      = (known after apply)
      + primary_dfs_internet_host          = (known after apply)
      + primary_dfs_microsoft_endpoint     = (known after apply)
      + primary_dfs_microsoft_host         = (known after apply)
      + primary_file_endpoint              = (known after apply)
      + primary_file_host                  = (known after apply)
      + primary_file_internet_endpoint     = (known after apply)
      + primary_file_internet_host         = (known after apply)
      + primary_file_microsoft_endpoint    = (known after apply)
      + primary_file_microsoft_host        = (known after apply)
      + primary_location                   = (known after apply)
      + primary_queue_endpoint             = (known after apply)
      + primary_queue_host                 = (known after apply)
      + primary_queue_microsoft_endpoint   = (known after apply)
      + primary_queue_microsoft_host       = (known after apply)
      + primary_table_endpoint             = (known after apply)
      + primary_table_host                 = (known after apply)
      + primary_table_microsoft_endpoint   = (known after apply)
      + primary_table_microsoft_host       = (known after apply)
      + primary_web_endpoint               = (known after apply)
      + primary_web_host                   = (known after apply)
      + primary_web_internet_endpoint      = (known after apply)
      + primary_web_internet_host          = (known after apply)
      + primary_web_microsoft_endpoint     = (known after apply)
      + primary_web_microsoft_host         = (known after apply)
      + public_network_access_enabled      = true
      + queue_encryption_key_type          = "Service"
      + resource_group_name                = ""
      + secondary_access_key               = (sensitive value)
      + secondary_blob_connection_string   = (sensitive value)
      + secondary_blob_endpoint            = (known after apply)
      + secondary_blob_host                = (known after apply)
      + secondary_blob_internet_endpoint   = (known after apply)
      + secondary_blob_internet_host       = (known after apply)
      + secondary_blob_microsoft_endpoint  = (known after apply)
      + secondary_blob_microsoft_host      = (known after apply)
      + secondary_connection_string        = (sensitive value)
      + secondary_dfs_endpoint             = (known after apply)
      + secondary_dfs_host                 = (known after apply)
      + secondary_dfs_internet_endpoint    = (known after apply)
      + secondary_dfs_internet_host        = (known after apply)
      + secondary_dfs_microsoft_endpoint   = (known after apply)
      + secondary_dfs_microsoft_host       = (known after apply)
      + secondary_file_endpoint            = (known after apply)
      + secondary_file_host                = (known after apply)
      + secondary_file_internet_endpoint   = (known after apply)
      + secondary_file_internet_host       = (known after apply)
      + secondary_file_microsoft_endpoint  = (known after apply)
      + secondary_file_microsoft_host      = (known after apply)
      + secondary_location                 = (known after apply)
      + secondary_queue_endpoint           = (known after apply)
      + secondary_queue_host               = (known after apply)
      + secondary_queue_microsoft_endpoint = (known after apply)
      + secondary_queue_microsoft_host     = (known after apply)
      + secondary_table_endpoint           = (known after apply)
      + secondary_table_host               = (known after apply)
      + secondary_table_microsoft_endpoint = (known after apply)
      + secondary_table_microsoft_host     = (known after apply)
      + secondary_web_endpoint             = (known after apply)
      + secondary_web_host                 = (known after apply)
      + secondary_web_internet_endpoint    = (known after apply)
      + secondary_web_internet_host        = (known after apply)
      + secondary_web_microsoft_endpoint   = (known after apply)
      + secondary_web_microsoft_host       = (known after apply)
      + sftp_enabled                       = false
      + shared_access_key_enabled          = true
      + table_encryption_key_type          = "Service"

      + blob_properties (known after apply)

      + network_rules (known after apply)

      + queue_properties (known after apply)

      + routing (known after apply)

      + share_properties (known after apply)

      + static_website (known after apply)
    }

  # azurerm_storage_container.data will be created
  + resource "azurerm_storage_container" "" {
      + container_access_type             = "container"
      + default_encryption_scope          = (known after apply)
      + encryption_scope_override_enabled = true
      + has_immutability_policy           = (known after apply)
      + has_legal_hold                    = (known after apply)
      + id                                = (known after apply)
      + metadata                          = (known after apply)
      + name                              = ""
      + resource_manager_id               = (known after apply)
      + storage_account_name              = "devwesteuropecy"
    }

  # databricks_cluster.stream will be created
  + resource "databricks_cluster" "stream" {
      + autotermination_minutes      = 30
      + cluster_id                   = (known after apply)
      + cluster_name                 = "Azure_Spark_Streaming"
      + custom_tags                  = {
          + "ResourceClass" = "SingleNode"
        }
      + default_tags                 = (known after apply)
      + driver_instance_pool_id      = (known after apply)
      + driver_node_type_id          = (known after apply)
      + enable_elastic_disk          = (known after apply)
      + enable_local_disk_encryption = (known after apply)
      + id                           = (known after apply)
      + node_type_id                 = "Standard_E4d_v4"
      + num_workers                  = 0
      + spark_conf                   = {
          + "spark.databricks.cluster.profile" = "singleNode"
          + "spark.master"                     = "local[*]"
        }
      + spark_version                = "15.4.x-scala2.12"
      + state                        = (known after apply)
      + url                          = (known after apply)
    }

  # databricks_job.Task_1 will be created
  + resource "databricks_job" "Task_1" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_1"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_1"

          + notebook_task {
              + notebook_path = "/cicd/Task_1"
            }
        }
    }

  # databricks_job.Task_2_a will be created
  + resource "databricks_job" "Task_2_a" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_2_a"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_2_a"

          + notebook_task {
              + notebook_path = "/cicd/Task_2_a"
            }
        }
    }

  # databricks_job.Task_2_b will be created
  + resource "databricks_job" "Task_2_b" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_2_b"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_2_b"

          + notebook_task {
              + notebook_path = "/cicd/Task_2_b"
            }
        }
    }

  # databricks_notebook.Task_1 will be created
  + resource "databricks_notebook" "Task_1" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_1"
      + source         = "./Task_1.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.Task_2_a will be created
  + resource "databricks_notebook" "Task_2_a" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_2_a"
      + source         = "./Task_2_a.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.Task_2_b will be created
  + resource "databricks_notebook" "Task_2_b" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_2_b"
      + source         = "./Task_2_b.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

Plan: 9 to add, 0 to change, 0 to destroy.

Changes to Outputs:
  + job_ids = {
      + Task_1   = (known after apply)
      + Task_2_a = (known after apply)
      + Task_2_b = (known after apply)
    }
╷
│ Warning: Argument is deprecated
│
│   with azurerm_storage_container.,
│   on main.tf line 66, in resource "azurerm_storage_container" "":
│   66:   storage_account_name  = azurerm_storage_account.Azure_Spark_Streaming.name
│
│ the `storage_account_name` property has been deprecated in favour of `storage_account_id` and will be removed in version 5.0 of the Provider.
╵

───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't guarantee to take exactly these actions if you run "terraform apply" now.

c:\data_eng\házi\6>terraform apply
data.databricks_spark_version.latest_lts: Reading...
data.databricks_spark_version.latest_lts: Read complete after 1s [id=15.4.x-scala2.12]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # azurerm_storage_account.Azure_Spark_Streaming will be created
  + resource "azurerm_storage_account" "Azure_Spark_Streaming" {
      + access_tier                        = (known after apply)
      + account_kind                       = "StorageV2"
      + account_replication_type           = "LRS"
      + account_tier                       = "Standard"
      + allow_nested_items_to_be_public    = true
      + cross_tenant_replication_enabled   = false
      + default_to_oauth_authentication    = false
      + dns_endpoint_type                  = "Standard"
      + https_traffic_only_enabled         = true
      + id                                 = (known after apply)
      + infrastructure_encryption_enabled  = false
      + is_hns_enabled                     = false
      + large_file_share_enabled           = (known after apply)
      + local_user_enabled                 = true
      + location                           = "westeurope"
      + min_tls_version                    = "TLS1_2"
      + name                               = ""
      + nfsv3_enabled                      = false
      + primary_access_key                 = (sensitive value)
      + primary_blob_connection_string     = (sensitive value)
      + primary_blob_endpoint              = (known after apply)
      + primary_blob_host                  = (known after apply)
      + primary_blob_internet_endpoint     = (known after apply)
      + primary_blob_internet_host         = (known after apply)
      + primary_blob_microsoft_endpoint    = (known after apply)
      + primary_blob_microsoft_host        = (known after apply)
      + primary_connection_string          = (sensitive value)
      + primary_dfs_endpoint               = (known after apply)
      + primary_dfs_host                   = (known after apply)
      + primary_dfs_internet_endpoint      = (known after apply)
      + primary_dfs_internet_host          = (known after apply)
      + primary_dfs_microsoft_endpoint     = (known after apply)
      + primary_dfs_microsoft_host         = (known after apply)
      + primary_file_endpoint              = (known after apply)
      + primary_file_host                  = (known after apply)
      + primary_file_internet_endpoint     = (known after apply)
      + primary_file_internet_host         = (known after apply)
      + primary_file_microsoft_endpoint    = (known after apply)
      + primary_file_microsoft_host        = (known after apply)
      + primary_location                   = (known after apply)
      + primary_queue_endpoint             = (known after apply)
      + primary_queue_host                 = (known after apply)
      + primary_queue_microsoft_endpoint   = (known after apply)
      + primary_queue_microsoft_host       = (known after apply)
      + primary_table_endpoint             = (known after apply)
      + primary_table_host                 = (known after apply)
      + primary_table_microsoft_endpoint   = (known after apply)
      + primary_table_microsoft_host       = (known after apply)
      + primary_web_endpoint               = (known after apply)
      + primary_web_host                   = (known after apply)
      + primary_web_internet_endpoint      = (known after apply)
      + primary_web_internet_host          = (known after apply)
      + primary_web_microsoft_endpoint     = (known after apply)
      + primary_web_microsoft_host         = (known after apply)
      + public_network_access_enabled      = true
      + queue_encryption_key_type          = "Service"
      + resource_group_name                = "r"
      + secondary_access_key               = (sensitive value)
      + secondary_blob_connection_string   = (sensitive value)
      + secondary_blob_endpoint            = (known after apply)
      + secondary_blob_host                = (known after apply)
      + secondary_blob_internet_endpoint   = (known after apply)
      + secondary_blob_internet_host       = (known after apply)
      + secondary_blob_microsoft_endpoint  = (known after apply)
      + secondary_blob_microsoft_host      = (known after apply)
      + secondary_connection_string        = (sensitive value)
      + secondary_dfs_endpoint             = (known after apply)
      + secondary_dfs_host                 = (known after apply)
      + secondary_dfs_internet_endpoint    = (known after apply)
      + secondary_dfs_internet_host        = (known after apply)
      + secondary_dfs_microsoft_endpoint   = (known after apply)
      + secondary_dfs_microsoft_host       = (known after apply)
      + secondary_file_endpoint            = (known after apply)
      + secondary_file_host                = (known after apply)
      + secondary_file_internet_endpoint   = (known after apply)
      + secondary_file_internet_host       = (known after apply)
      + secondary_file_microsoft_endpoint  = (known after apply)
      + secondary_file_microsoft_host      = (known after apply)
      + secondary_location                 = (known after apply)
      + secondary_queue_endpoint           = (known after apply)
      + secondary_queue_host               = (known after apply)
      + secondary_queue_microsoft_endpoint = (known after apply)
      + secondary_queue_microsoft_host     = (known after apply)
      + secondary_table_endpoint           = (known after apply)
      + secondary_table_host               = (known after apply)
      + secondary_table_microsoft_endpoint = (known after apply)
      + secondary_table_microsoft_host     = (known after apply)
      + secondary_web_endpoint             = (known after apply)
      + secondary_web_host                 = (known after apply)
      + secondary_web_internet_endpoint    = (known after apply)
      + secondary_web_internet_host        = (known after apply)
      + secondary_web_microsoft_endpoint   = (known after apply)
      + secondary_web_microsoft_host       = (known after apply)
      + sftp_enabled                       = false
      + shared_access_key_enabled          = true
      + table_encryption_key_type          = "Service"

      + blob_properties (known after apply)

      + network_rules (known after apply)

      + queue_properties (known after apply)

      + routing (known after apply)

      + share_properties (known after apply)

      + static_website (known after apply)
    }

  # azurerm_storage_container.data will be created
  + resource "azurerm_storage_container" "" {
      + container_access_type             = "container"
      + default_encryption_scope          = (known after apply)
      + encryption_scope_override_enabled = true
      + has_immutability_policy           = (known after apply)
      + has_legal_hold                    = (known after apply)
      + id                                = (known after apply)
      + metadata                          = (known after apply)
      + name                              = ""
      + resource_manager_id               = (known after apply)
      + storage_account_name              = ""
    }

  # databricks_cluster.stream will be created
  + resource "databricks_cluster" "stream" {
      + autotermination_minutes      = 30
      + cluster_id                   = (known after apply)
      + cluster_name                 = "Azure_Spark_Streaming"
      + custom_tags                  = {
          + "ResourceClass" = "SingleNode"
        }
      + default_tags                 = (known after apply)
      + driver_instance_pool_id      = (known after apply)
      + driver_node_type_id          = (known after apply)
      + enable_elastic_disk          = (known after apply)
      + enable_local_disk_encryption = (known after apply)
      + id                           = (known after apply)
      + node_type_id                 = "Standard_E4d_v4"
      + num_workers                  = 0
      + spark_conf                   = {
          + "spark.databricks.cluster.profile" = "singleNode"
          + "spark.master"                     = "local[*]"
        }
      + spark_version                = "15.4.x-scala2.12"
      + state                        = (known after apply)
      + url                          = (known after apply)
    }

  # databricks_job.Task_1 will be created
  + resource "databricks_job" "Task_1" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_1"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_1"

          + notebook_task {
              + notebook_path = "/cicd/Task_1"
            }
        }
    }

  # databricks_job.Task_2_a will be created
  + resource "databricks_job" "Task_2_a" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_2_a"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_2_a"

          + notebook_task {
              + notebook_path = "/cicd/Task_2_a"
            }
        }
    }

  # databricks_job.Task_2_b will be created
  + resource "databricks_job" "Task_2_b" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "Task_2_b"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "Task_2_b"

          + notebook_task {
              + notebook_path = "/cicd/Task_2_b"
            }
        }
    }

  # databricks_notebook.Task_1 will be created
  + resource "databricks_notebook" "Task_1" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_1"
      + source         = "./Task_1.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.Task_2_a will be created
  + resource "databricks_notebook" "Task_2_a" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_2_a"
      + source         = "./Task_2_a.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.Task_2_b will be created
  + resource "databricks_notebook" "Task_2_b" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = (known after apply)
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/cicd/Task_2_b"
      + source         = "./Task_2_b.dbc"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

Plan: 9 to add, 0 to change, 0 to destroy.

Changes to Outputs:
  + job_ids = {
      + Task_1   = (known after apply)
      + Task_2_a = (known after apply)
      + Task_2_b = (known after apply)
    }
╷
│ Warning: Argument is deprecated
│
│   with azurerm_storage_container.data,
│   on main.tf line 66, in resource "azurerm_storage_container" "":
│   66:   storage_account_name  = azurerm_storage_account.Azure_Spark_Streaming.name
│
│ the `storage_account_name` property has been deprecated in favour of `storage_account_id` and will be removed in version 5.0 of the Provider.
╵

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

databricks_notebook.Task_1: Creating...
databricks_notebook.Task_2_b: Creating...
databricks_notebook.Task_2_a: Creating...
databricks_cluster.stream: Creating...
databricks_notebook.Task_1: Creation complete after 1s [id=/cicd/Task_1]
databricks_notebook.Task_2_b: Creation complete after 1s [id=/cicd/Task_2_b]
databricks_notebook.Task_2_a: Creation complete after 1s [id=/cicd/Task_2_a]
azurerm_storage_account.Azure_Spark_Streaming: Creating...
databricks_cluster.stream: Still creating... [10s elapsed]
databricks_cluster.stream: Still creating... [20s elapsed]
databricks_cluster.stream: Still creating... [30s elapsed]
databricks_cluster.stream: Still creating... [40s elapsed]
databricks_cluster.stream: Still creating... [50s elapsed]
databricks_cluster.stream: Still creating... [1m0s elapsed]
databricks_cluster.stream: Still creating... [1m10s elapsed]
databricks_cluster.stream: Still creating... [1m20s elapsed]
databricks_cluster.stream: Still creating... [1m30s elapsed]
databricks_cluster.stream: Still creating... [1m40s elapsed]
databricks_cluster.stream: Still creating... [1m50s elapsed]
databricks_cluster.stream: Still creating... [2m0s elapsed]
databricks_cluster.stream: Still creating... [2m10s elapsed]
databricks_cluster.stream: Still creating... [2m20s elapsed]
databricks_cluster.stream: Still creating... [2m30s elapsed]
databricks_cluster.stream: Still creating... [2m40s elapsed]
databricks_cluster.stream: Still creating... [2m50s elapsed]
databricks_cluster.stream: Still creating... [3m0s elapsed]
databricks_cluster.stream: Still creating... [3m10s elapsed]
databricks_cluster.stream: Still creating... [3m20s elapsed]
databricks_cluster.stream: Still creating... [3m30s elapsed]
databricks_cluster.stream: Still creating... [3m40s elapsed]
databricks_cluster.stream: Still creating... [3m50s elapsed]
databricks_cluster.stream: Still creating... [4m0s elapsed]
databricks_cluster.stream: Still creating... [4m10s elapsed]
databricks_cluster.stream: Still creating... [4m20s elapsed]
databricks_cluster.stream: Still creating... [4m30s elapsed]
databricks_cluster.stream: Still creating... [4m40s elapsed]
databricks_cluster.stream: Still creating... [4m50s elapsed]
databricks_cluster.stream: Still creating... [5m0s elapsed]
databricks_cluster.stream: Creation complete after 5m3s [id=0428-061818-5npivnq1]
databricks_job.Task_1: Creating...
databricks_job.Task_2_b: Creating...
databricks_job.Task_2_a: Creating...
databricks_job.Task_2_b: Creation complete after 1s [id=916817301917295]
databricks_job.Task_1: Creation complete after 1s [id=973408353027284]
databricks_job.Task_2_a: Creation complete after 1s [id=317276640645714]
```

After the successful terraform execution I checked the created notebooks and cluster:

![cicd_cluster](https://github.com/user-attachments/assets/10777003-463a-4f38-bd6b-ba073fbcd331)

![cicd_nbooks](https://github.com/user-attachments/assets/0a78dfbe-a0cc-446f-848a-3c1b2afaabb5)


### Using Spark calculate in Databricks Notebooks for each city each day

I initiated the job from CMD:

```python
c:\data_eng\házi\6>databricks jobs run-now --job-id 321424226379040
WARN: Your CLI is configured to use Jobs API 2.0. In order to use the latest Jobs features please upgrade to 2.1: 'databricks jobs configure --version=2.1'. Future versions of this CLI will default to the new Jobs API. Learn more at https://docs.databricks.com/dev-tools/cli/jobs-cli.html
{
  "run_id": 1020340660154109,
  "number_in_job": 1020340660154109
}
```

![cicd_tas1_job](https://github.com/user-attachments/assets/8824872d-ed73-42a4-8b93-49681547bcf0)

![cicd_task1_output](https://github.com/user-attachments/assets/6a8fccda-c051-4044-86af-a67c8dfc6f92)

![cicd_dashb_1](https://github.com/user-attachments/assets/b1d9bb01-d3a9-4a8c-89ab-9a1c56cb33c9)


Fraction of the data streaming scripts' output:

```python
Uploading data for: c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=30
Uploading: hotel-weather/year=2017/month=09/day=30/part-00023-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00026-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00099-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00123-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00144-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00148-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00151-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00156-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00176-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Uploading: hotel-weather/year=2017/month=09/day=30/part-00184-e75efed7-c7e2-474d-9d80-f70b0ff83dfb.c000.snappy.parquet
Finished uploading files for c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/year=2017\month=09\day=30.

All weather data successfully uploaded day by day!

Process finished with exit code 0
```

After the first task I deleted all the logs from the container:

![cicd_empty_cont](https://github.com/user-attachments/assets/06d29506-52f2-4b46-81ef-5aed4208d934)

### Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city)

As I wrote earlier the second task was executed in two parts, but unfortunately, the Task_2_b is processed as a batch in a job quite quickly, I couldn't visualize the data in the job. The solution was to start the notebook and do the visualization manually.
So while the Task_2_a notebook was executing the streaming I manually created the visualization for the 10 bigges cities.

![cicid_task2s](https://github.com/user-attachments/assets/01c97fd6-8ab2-4a60-a4ef-a16f9bc6861f)


I initiated the job of the Task_2_a from CMD:
```python
c:\data_eng\házi\6>databricks jobs run-now --job-id 317276640645714
WARN: Your CLI is configured to use Jobs API 2.0. In order to use the latest Jobs features please upgrade to 2.1: 'databricks jobs configure --version=2.1'. Future versions of this CLI will default to the new Jobs API. Learn more at https://docs.databricks.com/dev-tools/cli/jobs-cli.html
{
  "run_id": 840361529742952,
  "number_in_job": 840361529742952
}
```

The dashboard of the Task_2_a notebook:

![cicd_task2_dashb](https://github.com/user-attachments/assets/cd64aa57-fb2c-4827-864b-289e5ef862b9)

Originally I initiated the job of the Task_2_b from CMD:

```python
c:\data_eng\házi\6>databricks jobs run-now --job-id 916817301917295
WARN: Your CLI is configured to use Jobs API 2.0. In order to use the latest Jobs features please upgrade to 2.1: 'databricks jobs configure --version=2.1'. Future versions of this CLI will default to the new Jobs API. Learn more at https://docs.databricks.com/dev-tools/cli/jobs-cli.html
{
  "run_id": 316012330974125,
  "number_in_job": 316012330974125
}

```
But to being able to visualize the tables I initiated the notebook from the Workspace menu.

Here you can see the list of the 10 biggest cities, then the tables of them and the related visualizations:

![cicd_table_par](https://github.com/user-attachments/assets/d6f8ec9e-13d0-4cba-bb92-e4ac7ee43a10)

### Paris:

![cicd_tab_par](https://github.com/user-attachments/assets/31d39f9d-0930-4d6a-8bca-7ef25058f53c)

### London:

![cicd_tab_lon](https://github.com/user-attachments/assets/ef3e594e-db03-4272-b536-33f2601cb628)

![cicd_vis_lon](https://github.com/user-attachments/assets/c05ed102-af37-4029-8e8a-a53f925df688)

### Milan:

![cicd_tab_mil](https://github.com/user-attachments/assets/87a1a313-4091-4f1d-aa3f-2b1cb25e9b05)

![cicd_vis_mil](https://github.com/user-attachments/assets/c9a2d59c-f44e-4611-8267-34bb96a28594)

### Amsterdam:

![cicd_tab_ams](https://github.com/user-attachments/assets/c102990a-bb27-4f66-88d7-f7ca09f3bf73)

![cicd_vis_ams](https://github.com/user-attachments/assets/e08e3a14-dafc-4d50-899b-64d3e55ee85c)

### Paddington:

![cicd_tab_padd](https://github.com/user-attachments/assets/2be2615f-93b6-4c79-99ea-e7536f1d8a10)

![cicd_vis_padd](https://github.com/user-attachments/assets/93909e11-17ba-4ec7-8f15-df3f902d00a9)

### Springfield

![cicd_tab_spr](https://github.com/user-attachments/assets/4cbe4543-2cd2-4ddc-a658-a2dd7e9d99f2)

![cicd_vis_spf](https://github.com/user-attachments/assets/130ea1c4-d62d-4c97-afbb-5874b206b47f)

### Memphis:

![cicd_tab_mem](https://github.com/user-attachments/assets/70ff98cb-9191-4db9-8ab7-4c50f7dc3a35)

![cicd_vis_mem](https://github.com/user-attachments/assets/13c93adf-5bb4-4706-b2e3-71adb0485ac0)

### Alberquerque

![cicd_tab_alb](https://github.com/user-attachments/assets/87ebb254-b823-4b30-ad18-3bf372c62d1f)

![cicid_vis_alb](https://github.com/user-attachments/assets/22aa80a9-daae-43b9-b7fd-4b1e8ef94583)

### Elko:

![cicd_tab_elk](https://github.com/user-attachments/assets/77249281-c45f-43de-95bf-20e44c5da468)

![cicd_vis_elk](https://github.com/user-attachments/assets/e2018bcc-4fcd-498f-8225-f4baccc925ef)





































