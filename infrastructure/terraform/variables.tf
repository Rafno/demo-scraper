variable "resource_group_location" {
  type        = string
  default     = "ukwest"
  description = "Location of the resource group."
}

variable "resource_group_name_prefix" {
  type        = string
  default     = "rg"
  description = "Prefix of the resource group name that's combined with a random ID so name is unique in your Azure subscription."
}


variable "allowed_ip_ranges" {
  description = "Dagster IP"
  type        = list(string)
  default = [
 //IPS
  ]
}
