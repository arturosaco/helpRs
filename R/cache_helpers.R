#' Saves an object to the cache/ dir adding the date when the object was saved 
#' @keywords cache 
#' @export
#' @import magrittr
#' @import aws.s3

cache.dated <- function(object, use_feather = FALSE, cache_date = Sys.Date(), 
  use_s3 = FALSE,
  bucket_name = NULL,
  AWS_ACCESS_KEY_ID = Sys.getenv("AWSAccessKeyId"),
  AWS_SECRET_ACCESS_KEY = Sys.getenv("AWSSecretKey"), 
  AWS_DEFAULT_REGION = "eu-west-1",
  zip_data_bool = TRUE,
  bucket_folder_prefix = ""){

  object.name <- deparse(substitute(object))
  if(!use_s3){
    if(!use_feather){
      saveRDS(object, file = 
        paste0("cache/", cache_date %>% gsub("-", "_", .), "_", 
          object.name, ".rds"))
      print(paste0("cache/", cache_date %>% gsub("-", "_", .), "_", 
          object.name, ".rds"))
      invisible(NULL)
    } else {
      write_feather(object, path = 
        paste0("cache/", cache_date %>% gsub("-", "_", .), "_", 
          object.name, ".feather"))
      print(paste0("cache/", cache_date %>% gsub("-", "_", .), "_", 
          object.name, ".feather"))
      invisible(NULL)
    }
  } else {
    if(is.null(bucket_name))
      stop("Must specify bucket_name when using S3")
    if(any(c(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) == "")){
      stop("Couldn't pick up AWS credentials from ENV, please pass them manually")
    }
    Sys.setenv(
      "AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
      "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
      "AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION
    )
    
    if(zip_data_bool){
      # object.name <- paste0(bucket_folder_prefix, object.name)
      local_path <- paste0("cache/", object.name, ".rds")
      saveRDS(object, file = local_path)
      s3_path <- paste0(cache_date %>% gsub("-", "_", .), "_", object.name, ".zip")
      system(paste("zip", paste0("cache/", s3_path), local_path))

      print(s3_path)
      print(local_path)
      print("Uploading cache to S3")

      put_object_response <- put_object(
        file = paste0("cache/", s3_path), 
        object = paste0(bucket_folder_prefix, s3_path), bucket = bucket_name
      )

      if(put_object_response)
        file.remove(paste0("cache/", s3_path))

    } else {
      local_path <- paste0("cache/", object.name, ".csv")
      if(!any(class(object) %in% c("data.frame", "data.table")))
        stop("Only data.table-like objects are allowed when zip_data_bool = FALSE")
      write.csv(object, file = local_path, row.names = FALSE, quote = FALSE)
      s3_path <- paste0(cache_date %>% gsub("-", "_", .), "_", object.name, ".csv")
      s3_path <- paste0(bucket_folder_prefix, s3_path)
      print(s3_path)
      print(local_path)
      print("Uploading cache to S3")

      put_object_response <- put_object(
        file = local_path, 
        object = s3_path, bucket = bucket_name
      )

    }


    # put_object_response <- put_object(
    #   file = paste0("cache/", s3_path), 
    #   object = s3_path, bucket = bucket_name
    # )
    # print(put_object_response)

    Sys.unsetenv("AWS_ACCESS_KEY_ID")
    Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
    Sys.unsetenv("AWS_DEFAULT_REGION")
    return(TRUE)

  }
}

#' load the most recent version of an object, follows the naming conventions of cache.dated()
#' @keywords cache 
#' @export
#' @import magrittr
#' @import aws.s3

load.cache.dated <- function(object.name, 
  cache_date = NULL,
  use_s3 = FALSE, 
  bucket_name = NULL, force_s3_check = TRUE,
  AWS_ACCESS_KEY_ID = Sys.getenv("AWSAccessKeyId"),
  AWS_SECRET_ACCESS_KEY = Sys.getenv("AWSSecretKey"), 
  AWS_DEFAULT_REGION = "eu-west-1",
  zip_data_bool = TRUE,
  bucket_folder_prefix = ""
  ){

  if(!use_s3){
    files <- dir("cache")
    ##### 
        if(!is.null(cache_date)){
            file.matches <- grep(paste0("[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_", 
                object.name, "\\.(rds|feather)"),
                files, value = TRUE)
            matched.dates <- gsub(paste0("_", object.name, ".*"), "", file.matches) %>%
                as.Date(format = "%Y_%m_%d")
            if(!as.Date(cache_date) %in% matched.dates){
                stop("Requested cache date couldn't be found")
            } else {
                file.match <- file.matches[which(as.Date(cache_date) == matched.dates)]
            }
        } else {
            file.match <- grep(paste0("[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_", 
                object.name, "\\.(rds|feather)"),
                files, value = TRUE) %>% sort %>% tail(1) %>% 
                file.path("cache", .) 
        }
    ####
    print(file.match)
    if(grepl("\\.feather$", file.match))
      read_feather(file.match)
    else
      readRDS(file.match)
  } else {
    if(any(c(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) == "")){
      stop("Couldn't pick up AWS credentials from ENV, please pass them manually")
    }
    Sys.setenv(
      "AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
      "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
      "AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION
    )
    if(zip_data_bool){
      file_format_aux <- "zip"
      local_file_format_aux <- "rds"
      read_fun <- readRDS
    }
    else {
      file_format_aux <- "csv"
      local_file_format_aux <- "csv"
      read_fun <- fread
    }

    if((paste0(object.name, file_format_aux) %in% dir("cache")) & !force_s3_check){
      Sys.unsetenv("AWS_ACCESS_KEY_ID")
      Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
      Sys.unsetenv("AWS_DEFAULT_REGION")
      return(read_fun(paste0("cache/", object.name, file_format_aux)))
    } else {
      if(is.null(bucket_name))
        stop("Must specify bucket_name when using S3")
      bucket_df <- get_bucket_df(bucket_name)
      files <- bucket_df$Key
      ####
      if(!is.null(cache_date)){
        file.matches <- grep(paste0(bucket_folder_prefix, 
            "[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_", 
            object.name, "\\.(", file_format_aux, ")"),
            files, value = TRUE)
        matched.dates <- gsub(".*([0-9]{4}_[0-9]{2}_[0-9]{2}).*", "\\1", file.matches) %>%
            as.Date(format = "%Y_%m_%d")
            if(!as.Date(cache_date) %in% matched.dates){
                stop("Requested cache date couldn't be found")
            } else {
                file.match <- file.matches[which(as.Date(cache_date) == matched.dates)]
            }
      } else {
          file.match <- grep(paste0(bucket_folder_prefix, "[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_", object.name, "\\.", file_format_aux),
              files, value = TRUE) %>% sort %>% tail(1)
      }

      ####
      download_object_response <- save_object(file.match, 
        file = file.path("cache", file.match) , 
        bucket = bucket_name
      )
      if(zip_data_bool){
        unzip.response <- system(paste0("unzip -o ", download_object_response, " -d ./"))
        if(unzip.response == 0)
          file.remove(download_object_response)        
      }
      Sys.unsetenv("AWS_ACCESS_KEY_ID")
      Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
      Sys.unsetenv("AWS_DEFAULT_REGION")
      if(zip_data_bool){
        return(read_fun(paste0("cache/", object.name, ".", local_file_format_aux)))
      } else {
        return(read_fun(paste0("cache/", 
          gsub("\\.csv", "", file.match), 
            ".", local_file_format_aux))
        )
      }
    }    
  }
}



#' Deletes cache object that are older than clean.older.than.days days old
#' Requires the same file structure as cache.dated and load.cache.dated
#' @keywords cache 
#' @export
#' @import magrittr
#' @import dplyr

clean.cache.dated <- function(clean.older.than.days = 7, remove = FALSE){

  cached.files <- dir("cache") %>% 
    grepr("[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_[a-zA-Z0-9\\.]+\\.rds")

  cached.dates <- cached.files %>% 
    gsub("([0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}).+", "\\1", .) %>%
    gsub("_", "-", .) %>%
    as.Date

  cached.object <- cached.files %>% 
    gsub("([0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2})_", "", .) 

  all.files.df <- data.frame(object = cached.object, 
    file = cached.files, 
    cache.date = cached.dates)

  all.files.df %>%
    plyr::ddply("object", function(sub){
      sub %>% 
        arrange(cache.date) %>%
        head(nrow(sub) - 1)
    }) ->
  files.to.remove.df

  if(nrow(files.to.remove.df) >= nrow(all.files.df))
    stop("Something went wrong and I stopped before deleting anything")
  # clean.older.than.days <- 30

  files.to.remove.df %>% 
    mutate(cache.age = Sys.Date() - cache.date) %>%
    filter(cache.age >= clean.older.than.days) %>%
    use_series(file) %>%
    as.character ->
  files.to.remove.chr

  system.rm.string <- files.to.remove.chr %>%
    paste0("cache/", .) %>%
    paste(collapse = " ") %>%
    paste("rm", .)
  if(remove)
    system(system.rm.string)
  return(files.to.remove.chr)
}



