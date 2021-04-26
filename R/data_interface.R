#' Create a SpatialRDD from an external data source.
#' Import spatial object from an external data source into a Sedona SpatialRDD.
#'
#' @param sc A \code{spark_connection}.
#' @param location Location of the data source.
#' @param type Type of the SpatialRDD (must be one of "point", "polygon", or
#'   "linestring".
#' @param contains_non_geom_attributes Whether the input contains non-
#'   geometrical attributes (default: TRUE).
#' @param storage_level Storage level of the RDD (default: MEMORY_ONLY).
#' @param repartition The minimum number of partitions to have in the resulting
#'   RDD (default: 1).
#'
#' @name sedona_typed_spatial_rdd_data_source
NULL

#' Create a typed SpatialRDD from a delimiter-separated values data source.
#'
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD) from a data source containing delimiter-separated values.
#' The data source can contain spatial attributes (e.g., longitude and latidude)
#' and other attributes. Currently only inputs with spatial attributes occupying
#' a contiguous range of columns (i.e.,
#' [first_spatial_col_index, last_spatial_col_index]) are supported.
#'
#' @inheritParams sedona_typed_spatial_rdd_data_source
#' @param delimiter Delimiter within each record. Must be one of
#'   ',', '\\t', '?', '\\'', '"', '_', '-', '\%', '~', '|', ';'
#' @param first_spatial_col_index Zero-based index of the left-most column
#'   containing spatial attributes (default: 0).
#' @param last_spatial_col_index Zero-based index of the right-most column
#'   containing spatial attributes (default: NULL). Note last_spatial_col_index
#'   does not need to be specified when creating a PointRDD because it will
#'   automatically have the implied value of (first_spatial_col_index + 1).
#'   For all other types of RDDs, if last_spatial_col_index is unspecified, then
#'   it will assume the value of -1 (i.e., the last of all input columns).
#'
#' @export
sedona_read_typed_dsv <- function(
                                  sc,
                                  location,
                                  delimiter = c(",", "\t", "?", "'", "\"", "_", "-", "%", "~", "|", ";"),
                                  type = c("point", "polygon", "linestring"),
                                  first_spatial_col_index = 0L,
                                  last_spatial_col_index = NULL,
                                  contains_non_geom_attributes = TRUE,
                                  storage_level = "MEMORY_ONLY",
                                  repartition = 1L) {
  delimiter <- to_delimiter_enum_value(sc, match.arg(delimiter))
  rdd_cls <- rdd_cls_from_type(type)
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

  invoke_new(
    sc,
    rdd_cls,
    java_context(sc),
    location,
    min(as.integer(repartition %||% 1L), 1L),
    fmt
  ) %>%
    set_storage_level(storage_level) %>%
    make_spatial_rdd(type)
}

#' Create a typed SpatialRDD from a shapefile data source.
#'
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD) from a shapefile data source.
#'
#' @inheritParams sedona_typed_spatial_rdd_data_source
#'
#' @export
sedona_read_typed_shapefile <- function(
                                        sc,
                                        location,
                                        type = c("point", "polygon", "linestring"),
                                        storage_level = "MEMORY_ONLY") {
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader",
    paste0("readTo", to_camel_case(type), "RDD"),
    java_context(sc),
    location
  ) %>%
    set_storage_level(storage_level) %>%
    make_spatial_rdd(type)
}

#' Create a typed SpatialRDD from a GeoJSON data source.
#'
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD) from a GeoJSON data source.
#'
#' @inheritParams sedona_typed_spatial_rdd_data_source
#'
#' @export
sedona_read_typed_geojson <- function(
                                      sc,
                                      location,
                                      type = c("point", "polygon", "linestring"),
                                      contains_non_geom_attributes = TRUE,
                                      storage_level = "MEMORY_ONLY",
                                      repartition = 1L) {
  rdd_cls <- rdd_cls_from_type(type)
  delimiter <- sc$state$enums$delimiter$GEOJSON

  invoke_new(
    sc,
    rdd_cls,
    java_context(sc),
    location,
    delimiter,
    contains_non_geom_attributes,
    min(as.integer(repartition %||% 1L), 1L)
  ) %>%
    set_storage_level(storage_level) %>%
    make_spatial_rdd(type)
}

#' Create a SpatialRDD from a GeoJSON data source.
#'
#' Create a generic SpatialRDD from a GeoJSON data source.
#'
#' @inheritParams sedona_typed_spatial_rdd_data_source
#' @param allow_invalid_geometries Whether to allow topology-invalid
#'   geometries to exist in the resulting RDD.
#' @param skip_syntactically_invalid_geometries Whether to allows Sedona to
#'   automatically skip syntax-invalid geometries, rather than throwing
#'   errorings.
#'
#' @export
sedona_read_geojson <- function(
                                sc,
                                location,
                                allow_invalid_geometries = TRUE,
                                skip_syntactically_invalid_geometries = TRUE,
                                storage_level = "MEMORY_ONLY",
                                repartition = 1L) {
  raw_text_rdd <- invoke(
    java_context(sc),
    "textFile",
    location,
    min(as.integer(repartition %||% 1L), 1L)
  )
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.GeoJsonReader",
    "readToGeometryRDD",
    raw_text_rdd,
    allow_invalid_geometries,
    skip_syntactically_invalid_geometries
  ) %>%
    set_storage_level(storage_level) %>%
    make_spatial_rdd(NULL)
}

#' Create a SpatialRDD from a Well-Known Binary (WKB) data source.
#'
#' Create a generic SpatialRDD from a hex-encoded Well-Known Binary (WKB) data
#' source.
#'
#' @inheritParams sedona_typed_spatial_rdd_data_source
#' @param wkb_col Zero-based index of column containing hex-encoded WKB data
#'   (default: 0).
#' @param allow_invalid_geometries Whether to allow topology-invalid
#'   geometries to exist in the resulting RDD.
#' @param skip_syntactically_invalid_geometries Whether to allows Sedona to
#'   automatically skip syntax-invalid geometries, rather than throwing
#'   errorings.
#'
#' @export
sedona_read_wkb <- function(
                            sc,
                            location,
                            wkb_col = 0L,
                            allow_invalid_geometries = TRUE,
                            skip_syntactically_invalid_geometries = TRUE,
                            storage_level = "MEMORY_ONLY",
                            repartition = 1L) {
  raw_text_rdd <- invoke(
    java_context(sc),
    "textFile",
    location,
    min(as.integer(repartition %||% 1L), 1L)
  )
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.WkbReader",
    "readToGeometryRDD",
    raw_text_rdd,
    as.integer(wkb_col),
    allow_invalid_geometries,
    skip_syntactically_invalid_geometries
  ) %>%
    set_storage_level(storage_level) %>%
    make_spatial_rdd(NULL)
}

rdd_cls_from_type <- function(type = c("point", "polygon", "linestring")) {
  type <- match.arg(type)

  paste0(
    "org.apache.sedona.core.spatialRDD.",
    to_camel_case(type),
    "RDD"
  )
}

to_camel_case <- function(type) {
  switch(
    type,
    point = "Point",
    polygon = "Polygon",
    linestring = "LineString"
  )
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

set_storage_level <- function(rdd, storage_level) {
  sc <- spark_connection(rdd)
  storage_level <- sc$state$object_cache$storage_levels[[storage_level]] %||% {
    storage_level_obj <- invoke_static(
      sc, "org.apache.spark.storage.StorageLevel", storage_level
    )
    sc$state$object_cache$storage_levels[[storage_level]] <- storage_level_obj

    storage_level_obj
  }
  invoke(rdd, "analyze", storage_level)

  rdd
}
