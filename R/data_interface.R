#' Read a TSV file into a Spark DataFrame
#'
#' Read a tabular data file into a Spark DataFrame.
#'
#' @param sc A \code{spark_connection}.
#' @param path Path to the input file (\samp{"hdfs://"}, \samp{"s3a://"} and \samp{"file://"} protocols are supported).
#' @param name Name to assign to the resulting Spark dataframe.
#' @param header Whether the first row of data be used as a header (default: FALSE)
#' @param columns A vector of column names or a named vector of column types.
#' @param repartition Number of partitions to have in the resulting Spark dataframe. Use 0 (the default) to avoid partitioning.
#' @param overwrite Whether to overwrite any existing Spark dataframe with the given name.
#' @param ... Additional arguments to sparklyr::spark_read_csv()
#'
#' @export
spark_read_tsv <- function(sc, path, name = NULL, header = FALSE, columns = NULL, repartition = 0, overwrite = FALSE, ...) {
  sparklyr::spark_read_csv(
    sc,
    path = path,
    name = name %||% sparklyr::random_string(),
    delimiter = "\t",
    header = header,
    columns = columns,
    repartition = repartition,
    overwrite = overwrite,
    ...
  )
}
