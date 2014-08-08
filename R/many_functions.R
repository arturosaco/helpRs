#' Turns a unixtimestamp into a POSIXct date
#' @param time unixtimestamp to convert.
#' @param time class output class.
#' @keywords time
#' @export
#' @examples
#' unix2POSIXct()
unix2POSIXct <- function (time)   structure(time, class = 
  c("POSIXt", "POSIXct"))

#' Counts the number of NAs in a data.frame
#' @param data.
#' @keywords counts
#' @export
#' @examples
#' count.na()
count.na <- function(data) sapply(data, function(x) sum(is.na(x)))

#' Plumbing for lsos()
#' @param pos a.
#' @param pattern b.
#' @param oreder.by c.
#' @param decreasing d.
#' @param head e.
#' @param n f.
#' @keywords ls
#' @examples
#' .ls.objects()
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
#' @examples
#' lsos()
lsos <- function(..., n = 10) {
    .ls.objects(..., order.by = "Size(MB)", decreasing = TRUE, head = TRUE, n = n)
}


#' Remove everything but functions from the global environment
#' @keywords rm
#' @export
#' @examples
#' rm.var()
rm.var <- function(){
  objs <- ls(pos = ".GlobalEnv")
  rm(list = objs[sapply(objs, function(x) class(get(x))) != "function"], 
    pos = ".GlobalEnv")
  invisible(NULL)
}
