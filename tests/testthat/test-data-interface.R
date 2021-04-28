context("data interface")

sc <- testthat_spark_connection()

test_that("sedona_read_dsv_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_dsv_to_typed_rdd(
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

test_that("sedona_read_dsv_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_dsv_to_typed_rdd(
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

test_that("sedona_read_dsv_to_typed_rdd() creates LineStringRDD correctly", {
  linestring_rdd <- sedona_read_dsv_to_typed_rdd(
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

test_that("sedona_read_geojson_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_geojson_to_typed_rdd(
    sc,
    location = test_data("arealm-small.json"),
    type = "point",
    contains_non_geom_attributes = TRUE
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(pt_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(pt_rdd, c(-173.1208, -84.9660, 30.2449, 71.3551))
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

test_that("sedona_read_geojson_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_geojson_to_typed_rdd(
    sc,
    location = test_data("polygon.json"),
    type = "polygon",
    contains_non_geom_attributes = TRUE
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(polygon_rdd$.jobj %>% invoke("approximateTotalCount"), 1001)
  expect_false(is.null(polygon_rdd$.jobj %>% invoke("boundaryEnvelope")))
  first_2 <- polygon_rdd$.jobj %>%
    invoke("%>%", list("rawSpatialRDD"), list("take", 2L))
  expected_data <- c(
    "01\t077\t011501\t5\t1500000US010770115015\t010770115015\t5\tBG\t6844991\t32636",
    "01\t045\t021102\t4\t1500000US010450211024\t010450211024\t4\tBG\t11360854\t0"
  )
  for (i in seq_along(expected_data)) {
    expect_equal(
      first_2[[i]] %>% invoke("getUserData"),
      expected_data[[i]]
    )
  }
  expect_equal(
    polygon_rdd$.jobj %>%
      invoke("%>%", list("fieldNames"), list("toArray")) %>%
      unlist(),
    c("STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "AFFGEOID", "GEOID", "NAME", "LSAD", "ALAND", "AWATER")
  )
})

test_that("sedona_read_geojson() works as expected on geojson input with 'type' and 'geometry' properties", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("polygon.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 1001
  )
})

test_that("sedona_read_geojson() works as expected on geojson input without 'type' or 'geometry' properties", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("polygon-without-properties.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 10
  )
})

test_that("sedona_read_geojson() works as expected on geojson input with null property value", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("polygon-with-null-property-value.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 3
  )
})

test_that("sedona_read_geojson() can skip invalid geometries correctly", {
  geojson_rdd <- sedona_read_geojson(
    sc,
    test_data("polygon-with-invalid-geometries.json"),
    allow_invalid_geometries = TRUE,
    skip_syntactically_invalid_geometries = FALSE
  )

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 3
  )

  geojson_rdd <- sedona_read_geojson(
    sc,
    test_data("polygon-with-invalid-geometries.json"),
    allow_invalid_geometries = FALSE,
    skip_syntactically_invalid_geometries = FALSE
  )

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 2
  )
})

test_that("sedona_read_wkb() works as expected", {
  wkb_rdd <- sedona_read_wkb(sc, test_data("county_small_wkb.tsv"))

  expect_equal(
    wkb_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 103
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = test_data("point"),
    type = "point"
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(
    pt_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    100000
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = test_data("polygon"),
    type = "polygon"
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(
    polygon_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    20069
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates LineStringRDD correctly", {
  linestring_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = test_data("polyline"),
    type = "linestring"
  )

  expect_equal(class(linestring_rdd), c("linestring_rdd", "spatial_rdd"))
  expect_equal(
    linestring_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    15137
  )
})

test_that("sedona_read_shapefile() works as expected", {
  wkb_rdd <- sedona_read_shapefile(sc, test_data("polygon"))

  expect_equal(
    wkb_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 10000
  )
})
