variable "map_with_interpolation" {
  type = map(string)
}

output "map_with_interpolation" {
  value = var.map_with_interpolation
}

variable "string_with_interpolation" {
  type = string
}

output "string_with_interpolation" {
  value = var.string_with_interpolation
}
