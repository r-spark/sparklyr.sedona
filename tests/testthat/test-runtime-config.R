context("runtime config")

sc <- testthat_spark_connection()

test_that("required runtime configurations are initialized correctly", {
  conf <- spark_session(sc) %>% invoke("conf")

  expect_equal(
    conf %>% invoke("get", "spark.serializer"),
    "org.apache.spark.serializer.KryoSerializer"
  )
  expect_equal(
    conf %>% invoke("get", "spark.kryo.registrator"),
    "org.apache.sedona.core.serde.SedonaKryoRegistrator"
  )
})

test_that("Sedona UDTs are registered correctly", {
  udts <- c(
    "org.locationtech.jts.geom.Geometry",
    "org.locationtech.jts.index.SpatialIndex"
  )
  for (udt in udts) {
    expect_true(
      invoke_static(
        sc,
        "org.apache.spark.sql.types.UDTRegistration",
        "exists",
        udt
      )
    )
  }
})
