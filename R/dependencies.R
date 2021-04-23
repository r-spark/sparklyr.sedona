spark_dependencies <- function(spark_version, scala_version, ...) {
  if (spark_version[1, 1] == "3") {
    spark_version <- "3.0"
    scala_version <- scala_version %||% "2.12"
  } else if (spark_version[1, 1] == "2" && spark_version[1, 2] == "4" ) {
    spark_version <- "2.4"
    scala_version <- scala_version %||% "2.11"
  } else {
    stop("Unsupported Spark version: ", spark_version)
  }

  spark_dependency(
    packages = (
      c(
        paste0(
          "org.apache.sedona:sedona-",
          c("core", "sql"),
          sprintf("-%s_%s:1.0.0-incubating", spark_version, scala_version)
        ),
        "org.datasyslab:geotools-wrapper:geotools-24.0",
        "org.datasyslab:sernetcdf:0.1.0",
        "org.locationtech.jts:jts-core:1.18.0",
        "org.wololo:jts2geojson:0.14.3"
      )
    ),
    initializer = function(sc) {
      invoke_static(
        sc,
        "org.apache.sedona.sql.utils.SedonaSQLRegistrator",
        "registerAll",
        spark_session(sc)
      )

      for (x in c(
                  "CSV",
                  "TSV",
                  "GEOJSON",
                  "WKT",
                  "WKB",
                  "COMMA",
                  "TAB",
                  "QUESTIONMARK",
                  "SINGLEQUOTE",
                  "QUOTE",
                  "UNDERSCORE",
                  "DASH",
                  "PERCENT",
                  "TILDE",
                  "PIPE",
                  "SEMICOLON")) {
        sc$state$enums$delimiter[[x]] <- invoke_static(
          sc, "org.apache.sedona.core.enums.FileDataSplitter", x
        )
      }
      for (x in c(
                  "POINT",
                  "POLYGON",
                  "LINESTRING",
                  "MULTIPOINT",
                  "MULTIPOLYGON",
                  "MULTILINESTRING",
                  "GEOMETRYCOLLECTION",
                  "CIRCLE",
                  "RECTANGLE")) {
        sc$state$enums$geometry_type[[x]] <- invoke_static(
          sc, "org.apache.sedona.core.enums.GeometryType", x
        )
      }
      sc$state$object_cache$storage_levels$memory_only <- invoke_static(
        sc, "org.apache.spark.storage.StorageLevel", "MEMORY_ONLY"
      )

      lockBinding(sym = "enums", env = sc$state)
    }
  )
}

.onAttach <- function(libname, pkgname) {
  options(spark.serializer = "org.apache.spark.serializer.KryoSerializer")
  options(
    spark.kryo.registrator = "org.apache.sedona.core.serde.SedonaKryoRegistrator"
  )
}
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
