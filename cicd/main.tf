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
    resource_group_name  = ""
    storage_account_name = ""
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
  name                  = "data"
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




