context("data interface")

sc <- testthat_spark_connection()

test_that("sedona_read_dsv() creates PointRDD correctly", {
  pt_rdd <- sedona_read_dsv(
    sc,
    location = test_data("arealm-small.csv"),
    delimiter = ",",
    type = "point",
    first_spatial_col_index = 1,
    contains_non_geom_attributes = TRUE
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(pt_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(pt_rdd, c(-173.120769, -84.965961, 30.244859, 71.355134))
  for (idx in 0:8) {
    expect_equal(
      pt_rdd$.jobj %>%
        invoke(
          "%>%",
          list("rawSpatialRDD"),
          list("take", 9L),
          list("get", idx),
          list("getUserData")
        ),
      "testattribute0\ttestattribute1\ttestattribute2"
    )
  }
})

test_that("sedona_read_dsv() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_dsv(
    sc,
    location = test_data("primaryroads-polygon.csv"),
    delimiter = ",",
    type = "polygon",
    first_spatial_col_index = 0,
    contains_non_geom_attributes = FALSE
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(polygon_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(polygon_rdd, c(-158.104182, -66.03575, 17.986328, 48.645133))
})

test_that("sedona_read_dsv() creates LineStringRDD correctly", {
  linestring_rdd <- sedona_read_dsv(
    sc,
    location = test_data("primaryroads-linestring.csv"),
    delimiter = ",",
    type = "linestring",
    first_spatial_col_index = 0,
    contains_non_geom_attributes = FALSE
  )

  expect_equal(class(linestring_rdd), c("linestring_rdd", "spatial_rdd"))
  expect_equal(linestring_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(linestring_rdd, c(-123.393766, -65.648659, 17.982169, 49.002374))
})
