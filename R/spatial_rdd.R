make_spatial_rdd <- function(jobj, type, ...) {
  structure(list(.jobj = jobj), class = paste0(c(type, "spatial"), "_rdd"))
}
