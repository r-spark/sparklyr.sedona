context("build index")

test_that("sedona_build_index() works as expected", {
  sc <- testthat_spark_connection()

  pt_rdd <- sedona_read_dsv_to_typed_rdd(
    sc,
    location = test_data("arealm.csv"),
    type = "point"
  )

  sedona_build_index(pt_rdd, type = "quadtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.quadtree.Quadtree"
  )

  sedona_build_index(pt_rdd, type = "rtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.strtree.STRtree"
  )
})
