# modified from https://stat.ethz.ch/pipermail/r-sig-db/2013q4/001313.html
dbRobustWriteTable <- function(conn, user, password, host, dbname, name, port, value, tries, num.rec = 10000, verbose = TRUE) {
    numFullChunks <- nrow(value)%/%num.rec
    lengthLastChunk <- nrow(value)%%num.rec
    if (numFullChunks >= 1) {
        writeSeqFullChunks <- data.frame(Start = seq(0,numFullChunks-1,1)*num.rec+1, Stop = seq(1,numFullChunks,1)*num.rec)
    }
    if (lengthLastChunk > 0) {
      writeSeqLastChunk <- data.frame(Start = numFullChunks*num.rec+1, Stop = numFullChunks*num.rec+lengthLastChunk)
    } else {
      writeSeqLastChunk <- data.frame()
    }
    if (numFullChunks >= 1) {
        writeSeqAllChunks <- rbind(writeSeqFullChunks,writeSeqLastChunk)
    } else { writeSeqAllChunks <- writeSeqLastChunk }

    for(i in 1:nrow(writeSeqAllChunks)) {
            try <- 0
            if (verbose == TRUE) {
              print(paste("writting chuck ", i, " out of ", nrow(writeSeqAllChunks), " chuncks!", sep=""))}
            rowSeq <- seq(writeSeqAllChunks$Start[i],writeSeqAllChunks$Stop[i],1)
            while (!dbWriteTable(conn = conn, name = name, value = value[rowSeq,], overwrite = FALSE, append = TRUE, row.names = FALSE) & try < tries) {
                conn  <- dbConnect(MySQL(),user=user,password=password,host=host,dbname=dbname, port = port)
                try <- try + 1
                if (try == tries) { stop("EPIC FAIL") }
                print(paste("Fail number",try,"epical fail at",tries,"tries.",sep = " "))
            }
    }
}


dbReconnect <- function( conn = NULL, idFile = NULL, drv = MySQL() ) {
  # idFile is the file where you store username, password, databasename, host and port
    if (  is.null(idFile) ) {
        stop('Can\'t reconnect, no id.')
    }
    if ( !is.null(conn) ) {
      tryCatch(
        expr = dbDisconnect( conn = conn ),
        error = function(cond) {
          print(cond)
        }
      )
    }
    ev = new.env()
    load( file = idFile, envir = ev )
    ev$drv <- drv
    conn <- with(
        data = ev,
        expr = dbConnect(
            drv = drv,
            user = user,
            password = pass,
            dbname = dbname,
            host = host
        )
    ); rm(ev)
    return(conn)

}
