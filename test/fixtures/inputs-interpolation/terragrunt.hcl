inputs = {
  map_with_interpolation    = jsondecode(file("stuff.json"))
  string_with_interpolation = "literal $${not_a_var} end"
}
