make_spatial_rdd <- function(type, jobj, ...) {
  subclass <- paste0(type, "_rdd")
  structure(list(.jobj = jobj), class = c(subclass, "spatial_rdd"))
}
