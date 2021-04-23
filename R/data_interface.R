#' Create a typed SpatialRDD from a delimiter-separated values file.
#'
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD) from a delimiter-separated values file.
#' The input file can contain spatial attributes (e.g., longitude and latidude)
#' and other attributes. Currently only inputs with spatial attributes occupying
#' a contiguous range of columns (i.e.,
#' [first_spatial_col_index, last_spatial_col_index]) are supported.
#'
#' @param sc A \code{spark_connection}.
#' @param location Location of the input file.
#' @param delimiter Delimiter within each record. Must be one of
#'   ',', '\\t', '?', '\\'', '"', '_', '-', '\%', '~', '|', ';'
#' @param type Type of the SpatialRDD (must be one of "point", "polygon", or
#'   "linestring".
#' @param first_spatial_col_index Zero-based index of the left-most column
#'   containing spatial attributes (default: 0).
#' @param last_spatial_col_index Zero-based index of the right-most column
#'   containing spatial attributes (default: NULL). Note last_spatial_col_index
#'   does not need to be specified when creating a PointRDD because it will
#'   automatically have the implied value of (first_spatial_col_index + 1).
#'   For all other types of RDDs, if last_spatial_col_index is unspecified, then
#'   it will assume the value of -1 (i.e., the last of all input columns).
#' @param contains_non_geom_attributes Whether the input contains non-
#'   geometrical attributes (default: TRUE).
#' @param memory Whether to cache the resulting RDD in memory (default: TRUE).
#' @param repartition The number of partitions to have in the resulting RDD
#'   (default: 1).
#'
#' @export
sedona_read_dsv <- function(
                            sc,
                            location,
                            delimiter = c(",", "\t", "?", "'", "\"", "_", "-", "%", "~", "|", ";"),
                            type = c("point", "polygon", "linestring"),
                            first_spatial_col_index = 0L,
                            last_spatial_col_index = NULL,
                            contains_non_geom_attributes = TRUE,
                            memory = TRUE,
                            repartition = 1L) {
  delimiter <- to_delimiter_enum_value(sc, match.arg(delimiter))
  type <- match.arg(type)
  rdd_cls <- paste0(
    "org.apache.sedona.core.spatialRDD.",
    switch(
      type,
      point = "Point",
      polygon = "Polygon",
      linestring = "LineString"
    ),
    "RDD"
  )
  first_spatial_col_index <- as.integer(first_spatial_col_index)
  if (type != "point") {
    last_spatial_col_index <- last_spatial_col_index %||% -1L
  } else {
    if (!is.null(last_spatial_col_index)) {
      if (as.integer(last_spatial_col_index) != first_spatial_col_index + 1L) {
        stop("last_spatial_col_index must be either unspecified or be equal to ",
             "(first_spatial_col_index + 1) for PointRDD")
      }
    } else {
      last_spatial_col_index <- first_spatial_col_index + 1L
    }
  }
  last_spatial_col_index <- as.integer(last_spatial_col_index)
  fmt <- (
    if (identical(type, "point")) {
      if (!is.null(last_spatial_col_index) &&
          last_spatial_col_index != first_spatial_col_index + 1L) {
      }
      invoke_new(
        sc,
        "org.apache.sedona.core.formatMapper.PointFormatMapper",
        first_spatial_col_index,
        delimiter,
        contains_non_geom_attributes
      )
    } else {
      fmt_cls <- paste0(
        "org.apache.sedona.core.formatMapper.",
        if (identical(type, "polygon")) {
          "Polygon"
        } else {
          "LineString"
        },
        "FormatMapper"
      )

      invoke_new(
        sc,
        fmt_cls,
        first_spatial_col_index,
        last_spatial_col_index,
        delimiter,
        contains_non_geom_attributes
      )
    }
  )

  rdd <- invoke_new(
    sc,
    rdd_cls,
    java_context(sc),
    location,
    min(as.integer(repartition %||% 1L), 1L),
    fmt
  )
  if (memory) {
    invoke(rdd, "analyze", sc$state$enums$storage_level$memory_and_disk)
  }

  make_spatial_rdd(type, rdd)
}

to_delimiter_enum_value <- function(sc, delimiter) {
  delimiter <- switch(
    delimiter,
    "," = "CSV",
    "\t" = "TSV",
    "?" = "QUESTIONMARK",
    "'" = "SINGLEQUOTE",
    "\"" = "QUOTE",
    "_" = "UNDERSCORE",
    "-" = "DASH",
    "%" = "PERCENT",
    "~" = "TILDE",
    "|" = "PIPE",
    ";" = "SEMICOLON",
    stop("Unsupported delimiter '", delimiter, "'")
  )

  sc$state$enums$delimiter[[delimiter]]
}
