% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cache_helpers.R
\name{cache.dated}
\alias{cache.dated}
\title{Saves an object to the cache/ dir adding the date when the object was saved}
\usage{
cache.dated(object, use_feather = FALSE, cache_date = Sys.Date(),
  use_s3 = FALSE, bucket_name = NULL,
  AWS_ACCESS_KEY_ID = Sys.getenv("AWSAccessKeyId"),
  AWS_SECRET_ACCESS_KEY = Sys.getenv("AWSSecretKey"),
  AWS_DEFAULT_REGION = "eu-west-1", zip_data_bool = TRUE)
}
\description{
Saves an object to the cache/ dir adding the date when the object was saved
}
\keyword{cache}

