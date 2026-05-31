output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "storage_account_key" {
  value     = azurerm_storage_account.main.primary_access_key
  sensitive = true
}

output "bronze_container" {
  value = azurerm_storage_container.bronze.name
}

output "silver_container" {
  value = azurerm_storage_container.silver.name
}

output "gold_container" {
  value = azurerm_storage_container.gold.name
}