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
          c("core", "sql", "viz"),
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
                  "csv",
                  "tsv",
                  "geojson",
                  "wkt",
                  "wkb",
                  "comma",
                  "tab",
                  "questionmark",
                  "singlequote",
                  "quote",
                  "underscore",
                  "dash",
                  "percent",
                  "tilde",
                  "pipe",
                  "semicolon")) {
        sc$state$enums$delimiter[[x]] <- invoke_static(
          sc, "org.apache.sedona.core.enums.FileDataSplitter", toupper(x)
        )
      }
      for (x in c(
                  "point",
                  "polygon",
                  "linestring",
                  "multipoint",
                  "multipolygon",
                  "multilinestring",
                  "geometrycollection",
                  "circle",
                  "rectangle")) {
        sc$state$enums$geometry_type[[x]] <- invoke_static(
          sc, "org.apache.sedona.core.enums.GeometryType", toupper(x)
        )
      }
      sc$state$object_cache$storage_levels$memory_only <- invoke_static(
        sc, "org.apache.spark.storage.StorageLevel", "MEMORY_ONLY"
      )
      for (x in c("png", "gif", "svg")) {
        sc$state$object_cache$image_types[[x]] <- invoke_static(
          sc, "org.apache.sedona.viz.utils.ImageType", toupper(x)
        )
      }
      for (x in c("red", "green", "blue")) {
        sc$state$object_cache$awt_color[[x]] <- invoke_static(
          sc, "java.awt.Color", toupper(x)
        )
      }

      lockBinding(sym = "enums", env = sc$state)
    }
  )
}

.onAttach <- function(libname, pkgname) {
  options(spark.serializer = "org.apache.spark.serializer.KryoSerializer")
  options(
    spark.kryo.registrator = "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
  )
}
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
