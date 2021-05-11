context("spatial queries")

sc <- testthat_spark_connection()

knn_query_pt_x <- -84.01
knn_query_pt_y <- 34.01
knn_query_pt_tbl <- DBI::dbGetQuery(
  sc,
  sprintf(
    "SELECT ST_GeomFromText(\"POINT(%f %f)\") AS `pt`",
    knn_query_pt_x,
    knn_query_pt_y
  )
)
knn_query_pt <- knn_query_pt_tbl$pt[[1]]
knn_query_size <- 100
polygon_sdf <- read_polygon_rdd() %>% sdf_register()
expected_knn_dists <- polygon_sdf %>%
  dplyr::mutate(dist = ST_Distance(geometry, ST_Point(knn_query_pt_x, knn_query_pt_y))) %>%
  dplyr::arrange(dist) %>%
  head(knn_query_size) %>%
  dplyr::pull(dist)

compute_dists <- function(sdf) {
  sdf %>%
    dplyr::mutate(dist = ST_Distance(geometry, ST_Point(knn_query_pt_x, knn_query_pt_y))) %>%
    dplyr::pull(dist)
}

test_that("KNN query works as expected for 'rdd' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_rdd <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "rdd"
    )

    expect_equal(
      knn_rdd %>% sdf_register() %>% compute_dists(), expected_knn_dists
    )
  }
})

test_that("KNN query works as expected for 'sdf' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_sdf <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "sdf"
    )

    expect_equal(knn_sdf %>% compute_dists(), expected_knn_dists)
  }
})

test_that("KNN query works as expected for 'raw' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_result <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "raw"
    )

    expect_equal(
      knn_result %>%
        lapply(function(pt) invoke(pt, "distance", knn_query_pt)) %>%
        unlist(),
      expected_knn_dists
    )
  }
})
