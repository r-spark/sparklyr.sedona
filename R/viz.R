#' Visualization routine for Sedona spatial RDD.
#'
#' Generate a visual representation of geometrical object(s) within a Sedona
#' spatial RDD.
#'
#' @param x A Sedona spatial RDD.
#' @param resolution_x Resolution on the x-axis.
#' @param resolution_y Resolution on the y-axis.
#' @param output_location Location of the output image. This should be the
#'   desired path of the image file excluding extension in its file name.
#' @param output_format File format of the output image. Currently PNG, GIF, and
#'  SVG formats are supported (default: PNG).
#' @param boundary Only render data within the given rectangular boundary.
#'   The `boundary` parameter can be set to either a numeric vector of
#'   c(min_x, max_y, min_y, max_y) values, or with a bounding box object
#'   e.g., new_bounding_box(sc, min_x, max_y, min_y, max_y), or NULL
#'   (the default). If `boundary` is NULL, then the minimum bounding box of the
#'   input spatial RDD will be computed and used as boundary for rendering.
#' @param browse Whether to open the rendered image in a browser (default:
#'   interactive()).
#' @param color_of_variation Which color channel will vary depending on values
#'   of data points. Must be one of "red", "green", or "blue". Default: red.
#' @param base_color Color of any data point with value 0. Must be a numeric
#'   vector of length 3 specifying values for red, green, and blue channels.
#'   Default: c(0, 0, 0).
#' @param shade Whether data point with larger magnitude will be displayed with
#'   darker color. Default: TRUE.
#'
#' @name sedona_visualization_routines
NULL

#' Visualize a Sedona spatial RDD using a heatmap.
#'
#' Generate a heatmap of geometrical object(s) within a Sedona spatial RDD.
#'
#' @inheritParams sedona_visualization_routines
#' @param blur_radius Controls the radius of a Gaussian blur in the resulting
#'   heatmap.
#'
#' @family Sedona visualization routines
#' @export
sedona_render_heatmap <- function(
                                  x,
                                  resolution_x,
                                  resolution_y,
                                  output_location,
                                  output_format = c("PNG", "GIF", "SVG"),
                                  boundary = NULL,
                                  blur_radius = 10L,
                                  browse = interactive()) {
  sc <- spark_connection(x$.jobj)
  output_format <- match.arg(output_format)

  boundary <- validate_boundary(x, boundary)
  viz_op <- invoke_new(
    sc,
    "org.apache.sedona.viz.extension.visualizationEffect.HeatMap",
    as.integer(resolution_x),
    as.integer(resolution_y),
    boundary$.jobj,
    FALSE,
    as.integer(blur_radius)
  )

  x %>% gen_raster_image(
    viz_op = viz_op,
    output_location = output_location,
    output_format = output_format
  )
  if (browse) {
    browseURL(paste0(output_location, ".", tolower(output_format)))
  }

  invisible(NULL)
}

#' Visualize a Sedona spatial RDD using a scatter plot.
#'
#' Generate a scatter plot of geometrical object(s) within a Sedona spatial RDD.
#'
#' @inheritParams sedona_visualization_routines
#' @param reverse_coords Whether to reverse spatial coordinates in the plot
#'   (default: FALSE).
#'
#' @family Sedona visualization routines
#' @export
sedona_render_scatter_plot <- function(
                                  x,
                                  resolution_x,
                                  resolution_y,
                                  output_location,
                                  output_format = c("PNG", "GIF", "SVG"),
                                  boundary = NULL,
                                  color_of_variation = c("red", "green", "blue"),
                                  base_color = c(0, 0, 0),
                                  shade = TRUE,
                                  reverse_coords = FALSE,
                                  browse = interactive()) {
  sc <- spark_connection(x$.jobj)
  output_format <- match.arg(output_format)
  color_of_variation <- match.arg(color_of_variation)
  validate_base_color(base_color)

  boundary <- validate_boundary(x, boundary)
  viz_op <- invoke_new(
    sc,
    "org.apache.sedona.viz.extension.visualizationEffect.ScatterPlot",
    as.integer(resolution_x),
    as.integer(resolution_y),
    boundary$.jobj,
    reverse_coords
  )

  x %>% gen_raster_image(
    viz_op = viz_op,
    output_location = output_location,
    output_format = output_format,
    color_settings = list(
      color_of_variation = color_of_variation,
      base_color = base_color,
      shade = TRUE
    )
  )
  if (browse) {
    browseURL(paste0(output_location, ".", tolower(output_format)))
  }

  invisible(NULL)
}

validate_base_color <- function(base_color) {
  if (!is.numeric(base_color) || length(base_color) != 3) {
    stop("Base color (`base_color`) must be a numeric vector of length 3 ",
         "specifying values for red, green, and blue channels ",
         "(e.g., c(0, 0, 0)).")
  }
}

validate_boundary <- function(x, boundary) {
  sc <- spark_connection(x$.jobj)

  if (is.null(boundary)) {
    minimum_bounding_box(x)
  } else if (inherits(boundary, "bounding_box")) {
    boundary
  } else if (is.numeric(boundary)) {
    if (length(boundary) != 4) {
      stop("Boundary specification with numeric vector must consist of ",
           "exactly 4 values: c(min_x, max_x, min_y, max_y).")
    }
    do.call(new_bounding_box, append(list(sc), as.list(boundary)))
  } else {
    stop("Boundary specification must be either NULL, a numeric vector of ",
         "c(min_x, max_x, min_y, max_y) values, or a bounding box object")
  }
}

gen_raster_image <- function(
                             x,
                             viz_op,
                             output_location,
                             output_format,
                             color_settings = NULL) {
  sc <- spark_connection(x$.jobj)

  image_generator <- invoke_new(
    sc,
    "org.apache.sedona.viz.core.ImageGenerator"
  )
  if (!is.null(color_settings)) {
    customize_color_params <- list(viz_op, "CustomizeColor") %>%
      append(as.list(as.integer(unlist(color_settings$base_color)))) %>%
      append(
        list(
          255L, # gamma
          sc$state$object_cache$awt_color[[color_settings$color_of_variation]],
          color_settings$shade
        )
      )
    do.call(invoke, customize_color_params)
  }
  invoke(viz_op, "Visualize", java_context(sc), x$.jobj)
  invoke(
    image_generator,
    "SaveRasterImageAsLocalFile",
    invoke(viz_op, "rasterImage"),
    output_location,
    sc$state$object_cache$image_types[[output_format]]
  )
}
