#' Turns a unixtimestamp into a POSIXct date
#' @param time unixtimestamp to convert.
#' @param time class output class.
#' @keywords time
#' @export

unix2POSIXct <- function (time)   structure(time, class = 
  c("POSIXt", "POSIXct"))

#' Counts the number of NAs in a data.frame
#' @param data.
#' @keywords counts
#' @export

count.na <- function(data) sapply(data, function(x) sum(is.na(x)))

#' Plumbing for lsos()
#' @param pos a.
#' @param pattern b.
#' @param oreder.by c.
#' @param decreasing d.
#' @param head e.
#' @param n f.

.ls.objects <- function (pos = 1, pattern, order.by,
                        decreasing=FALSE, head=FALSE, n=5) {
    napply <- function(names.x, fn) sapply(names.x, function(x)
                                         fn(get(x, pos = pos)))
    names.x <- ls(pos = pos, pattern = pattern)
    obj.class <- napply(names.x, function(x) as.character(class(x))[1])
    obj.mode <- napply(names.x, mode)
    obj.type <- ifelse(is.na(obj.class), obj.mode, obj.class)
    obj.size <- round(napply(names.x, object.size) / 1048576, 2)
    obj.dim <- t(napply(names.x, function(x)
                        as.numeric(dim(x))[1:2]))
    vec <- is.na(obj.dim)[, 1] & (obj.type != "function")
    obj.dim[vec, 1] <- napply(names.x, length)[vec]
    out <- data.frame(obj.type, obj.size, obj.dim)
    names(out) <- c("Type", "Size(MB)", "Rows", "Columns")
    if (!missing(order.by))
        out <- out[order(out[[order.by]], decreasing=decreasing), ]
    if (head)
        out <- head(out, n)
    out
}

#' Lists object with sizes
#' @param n number of objects to display.
#' @keywords ls
#' @export
lsos <- function(..., n = 10) {
    .ls.objects(..., order.by = "Size(MB)", decreasing = TRUE, head = TRUE, n = n)
}


#' Remove everything but functions from the global environment
#' @keywords rm
#' @export

rm.var <- function(){
  objs <- ls(pos = ".GlobalEnv")
  rm(list = objs[sapply(objs, function(x) class(get(x))) != "function"], 
    pos = ".GlobalEnv")
  invisible(NULL)
}


#' Wrapper of grep that makes it more magrittr friendly
#' @keywords grep
#' @export

grepr <- function(string.vec, pattern){
  grep(pattern, string.vec, value = TRUE)
}


#' Saves an object to the cache/ dir adding the date when the object was saved 
#' @keywords cache 
#' @export
#' @import magrittr

cache.dated <- function(object, use_feather = FALSE){
  object.name <- deparse(substitute(object))
    if(!use_feather){
    saveRDS(object, file = 
      paste0("cache/", Sys.Date() %>% gsub("-", "_", .), "_", 
        object.name, ".rds"))
    print(paste0("cache/", Sys.Date() %>% gsub("-", "_", .), "_", 
        object.name, ".rds"))
    invisible(NULL)
  } else {
    write_feather(object, path = 
      paste0("cache/", Sys.Date() %>% gsub("-", "_", .), "_", 
        object.name, ".feather"))
    print(paste0("cache/", Sys.Date() %>% gsub("-", "_", .), "_", 
        object.name, ".feather"))
    invisible(NULL)
  }
}

#' load the most recent version of an object, follows the namings conventions of cache.dated()
#' @keywords cache 
#' @export
#' @import magrittr

load.cache.dated <- function(object.name){
  files <- dir("cache")
  file.match <- grep(paste0("[0-9]{4,4}_[0-9]{2,2}_[0-9]{2,2}_", object.name, "\\.(rds|feather)"),
    files, value = TRUE) %>% sort %>% tail(1) %>% 
    file.path("cache", .) 
  print(file.match)
  if(grepl("\\.feather$", file.match))
    read_feather(file.match)
  else
    readRDS(file.match)
}


#' Wrapper for rbindlist that checks the stack every object in the input list that's a data.table
#' @keywords cache 
#' @export
#' @import magrittr
#' @import data.table

rbindlist.valid <- function(list.data.tables.x){
  lapply(list.data.tables.x, function(data.table.x){
    if("data.table" %in% class(data.table.x) && nrow(data.table.x) > 0)
      return(data.table.x)
    else
      return(NULL)
  }) %>% rbindlist
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


#' Returns the cleaned url using the list of top level domains from tldextract
#' @keywords url
#' @export
#' @import magrittr
#' @import stringr
#' @import tldextract
#' @import urltools

clean.urls.f <- function(original.urls, tld = getTLD()){
    original.urls %>%
        str_trim %>% 
        gsub("(https?\\:\\/\\/)?(\\/?www.?[0-9]?\\.+)?", "", .) %>%
        gsub("/$", "", .) %>%
        domain ->
    clean.urls
    clean.urls.tld <- tldextract(clean.urls, tldnames = tld) 
    clean.urls.out <- paste(clean.urls.tld$domain, clean.urls.tld$tld, sep = ".")
    return(clean.urls.out)
}


#' Sends error email with error log attached
#' @keywords email
#' @export
#' @import rJava
#' @import mailR

error.email.f <- function(error_log.path, error.display.name, 
    admin.email, error.email){
    function(e){
        sink()
        send.mail(from = error.email,
          to = admin.email,
          subject = paste("Error in", error.display.name),
          body = as.character(e),
          # html = TRUE,
          # inline = TRUE,
          attach.files = error_log.path,
          smtp = list(host.name = "gmail-smtp-in.l.google.com", port = 25),
          authenticate = FALSE,
          send = TRUE,
          debug = FALSE)
    }
}


#' Wraps source of script adding email sending on error and dbWriting on success
#' @keywords source
#' @export
#' @import RPostgres
#' @import futile.logger

source.wrapper <- function(script.path, error_log.path,
    admin.email, error.email){
    flog.trace(paste("Running", script.path), name = "main")
    tryCatch({
        source(script.path)
    }, error = error.email.f(error_log.path = error_log.path,
        error.display.name = script.path, admin.email, error.email))
    flog.trace(paste("Finished running", script.path), name = "main")
}

#' helper to turn an R vector to SQL format
#' @keywords source
#' @export

vector.to.SQL <- function(x){
  paste0("(", paste(paste0("'", x, "'"), collapse = ","), ")")
} 

#' get the first element of the class types for every element in a list
#' @keywords source
#' @export

list.class <- function(x) sapply(x, function(x) class(x)[1])
