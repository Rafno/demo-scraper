resource "azurerm_resource_group" "datadevops_rg" {
  location = var.resource_group_location
  name     = "DataDevops"
}

resource "azurerm_network_security_group" "container_nsg" {
  name                = "container_nsg"
  location            = var.resource_group_location
  resource_group_name = azurerm_resource_group.datadevops_rg.name
}

resource "azurerm_storage_account" "blob_sa" {
  name                     = "homelabaccount"
  resource_group_name      = azurerm_resource_group.datadevops_rg.name
  location                 = azurerm_resource_group.datadevops_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  tags = {
    environment = "staging"
  }

  network_rules {
    default_action             = "Deny"
    virtual_network_subnet_ids = [] # Leave empty if you don't want to restrict by subnet
    ip_rules = [
       // IPs
    ]
  }
}

resource "azurerm_storage_container" "blob_container" {
  name                  = "landing-blob"
  storage_account_name  = azurerm_storage_account.blob_sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "duckdb_blob_container" {
  name                  = "duckdb-blob"
  storage_account_name  = azurerm_storage_account.blob_sa.name
  container_access_type = "private"
}
