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

Then I uploaded the source data to the new data container:

![uploading_source](https://github.com/user-attachments/assets/458aac34-2bfe-4a3a-9509-4c7d25b35792)

![uploaded_data](https://github.com/user-attachments/assets/da1a78cc-5c85-4f2d-8867-0ebe0dd8afc7)

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




