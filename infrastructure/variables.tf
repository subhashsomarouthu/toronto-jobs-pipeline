variable "resource_group_name" {
  description = "Azure resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "canadacentral"
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  type        = string
}