#' Find axis-aligned minimum bounding box of a geometry.
#'
#' Given a spatial RDD, find an axis-aligned minimum bounding box (envelope)
#' containing the geometrical object(s) represented by the RDD.
#'
#' @param x A spatial RDD.
#'
#' @export
boundary <- function(x) {
  x$.jobj %>% invoke("boundary") %>% make_boundary()
}

#' Construct a bounding box object.
#'
#' Construct a axis-aligned rectangular bounding box object.
#'
#' @param sc The Spark connection.
#' @param min_x Minimum x-value of the bounding box, can be +/- Inf.
#' @param max_x Maximum x-value of the bounding box, can be +/- Inf.
#' @param min_y Minimum y-value of the bounding box, can be +/- Inf.
#' @param max_y Maximum y-value of the bounding box, can be +/- Inf.
#'
#' @export
new_boundary <- function(sc, min_x = -Inf, max_x = Inf, min_y = -Inf, max_y = Inf) {
  make_boundary(
    invoke_new(
      sc,
      "org.locationtech.jts.geom.Envelope",
      as.numeric(min_x),
      as.numeric(max_x),
      as.numeric(min_y),
      as.numeric(max_y)
    )
  )
}

make_boundary <- function(jobj) {
  structure(list(.jobj = jobj), class = "boundary")
}
