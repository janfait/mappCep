cepAggregator <- setRefClass("cepAggregator",
     fields = list(
       debug="logical",
       cache="list",
       segments="list",
       filters="list",
       paths = "list",
       constants = "list",
       data="list"
     ),
     methods = list(
       debugPrint = function(a){
         if(.self$debug){
           if(is.data.frame(a)){
             str(a)
           }else{
             print(a)
           }
         }
       },
       loadPackages = function(){
         if(any(!require("lubridate"),!require("dplyr"),!require("stringr"))){
           stop("Please install the following packages to use this Class: dplyr, lubridate, stringr")
         }
       },
       makeDataRDE = function(pattern="",path=NULL,recursive=T,lazyLoad=F){
         
       },
       makeDataRA = function(pattern="",path=NULL,recursive=T,lazyLoad=F){
         
         if(lazyLoad){
           .self$data$activity <- .self$getData(lazyLoad=T)
           return(NULL)
         }
         if(is.null(path)){
           path <- .self$paths$exports
         }
         .self$debugPrint(paste("Looking for files like",pattern,"in",path))
         files <- list.files(
           path=path,
           pattern=pattern,
           full.names = T,
           recursive = recursive
         )
         .self$debugPrint(paste("Selected",length(files),"files"))
         stopifnot(length(files)>0)
         .self$debugPrint(paste(files[1:10],"...",collapse=","))
         dataList <- lapply(files,function(f) {
           x<-read.csv(f,sep=";",header=T,stringsAsFactors=F)
           if(is.data.frame(x)){
             .self$debugPrint(paste("Reading file",f,"with",nrow(x),"and",ncol(x),"columns"))
           }else{
             .self$debugPrint(paste("Error in reading the data frame"))
             return(NULL)
           }
           return(x)
         })
         .self$debugPrint("Starting binding of data")
         d <- try(do.call("rbind",dataList[!sapply(dataList,is.null)]))
         if(inherits(d,"try-error")){
           .self$debugPrint("Failed to bind the data")
         }else{
           .self$debugPrint("Binding successful")
         }
         
         colnames(d) <-c("act.id","msg.sbjct","u.id","email","msg.id","name","msgcat.id","sender.id","sender.name","send.date","send.time","bounce","bounce.date","bounce.time","open.date","open.time","click.date","click.time","conv.date","conv.date","click.online","click.social","fwd","unsub")
         d <- unique(d)
         d$conv.date <- NULL
         #new vars
         d$domain <- gsub('.*@(.*)','\\1',d$email)
         d$open.date <- ifelse(nchar(d$click.date)>5 & nchar(d$open.date)<2,d$click.date,d$open.date)
         d$open.time <- ifelse(nchar(d$click.time)>5 & nchar(d$open.time)<2,d$click.time,d$open.time)
         d$send.hour = as.numeric(substr(d$send.time,0,2))
         d$send.timeoftheday = factor(findInterval(d$send.hour,c(6,11,15,20)),levels = 0:4, labels = c("Night","Morning","Early Afternoon","Late Afternoon","Evening"))
         .self$debugPrint("New vars computed")
         #filter
         d <- d[!str_detect(d$domain,.self$filters$patterns$domain),]
         d <- d[!str_detect(d$email,.self$filters$patterns$email),]
         d <- d[!str_detect(d$msg.sbjct,.self$filters$patterns$message),]
         d <- d[!str_detect(d$name,.self$filters$patterns$message),]
         d <- d[!str_detect(d$sender.name,.self$filters$patterns$group),]
         d <- d[!(d$sender.id %in% .self$filters$ids$group),]
         .self$debugPrint("Unwanted records filtered")
         .self$debugPrint(paste("Succesfully bound files to a data frame with",nrow(d),"rows and following columns:"))
         .self$debugPrint(colnames(d))
         .self$debugPrint(paste("Retrieve the dataset at $data$activity slot of your cepAggregator class instance"))
         .self$data$activity <- d
         
         invisible(return(TRUE))
       },
       makeEvents = function(data=NULL){
         if(is.null(data)){
           d <- .self$data$activity
         }else{
           d <- data
         }
         d$event <- ifelse(d$click.date!="","click",ifelse(d$open.date!="","open","send"))
         return(d)
       },
       makeMessages = function(limit=NULL,groupby=NULL){
         if(length(.self$data$activity)==0){
           .self$data$activity <- .self$getData(lazyLoad = T)
         }
         if(is.null(groupby)){
           groupby <- "msg.id,msg.sbjct,name,sender.id,sender.name,send.date"
           groupby <- unlist(strsplit(groupby,","))
         }else{
           groupby <- unlist(strsplit(groupby,","))
         }
         dots <- lapply(groupby,as.symbol)

         if(is.null(limit)){
           rows <- nrow(.self$data$activity)
         }else{
           rows <- as.numeric(limit)
         }
         .self$debugPrint(paste("Making messages"))
         msgs <- (
           .self$data$activity[1:rows,]
           %>% dplyr::group_by_(.dots=dots)
           %>% dplyr::summarise(
             sends = length(send.time),
             opens = sum(nchar(open.date)>5),
             bounces = sum(nchar(bounce.date)>5),
             accepted = sum(sends-bounces),
             clicks = sum(nchar(click.date)>5),
             unsubs = sum(unsub),
             delivery.rate = round(accepted/sends,2),
             open.rate = round(opens/sends,2),
             bounce.rate = round(bounces/sends,2),
             click.rate = round(clicks/sends,2),
             unsub.rate = round(unsubs/sends,2),
             click.thru.rate = round(clicks/opens,2),
             sends.cat = findInterval(sends,c(100,1000,10000,50000)),
             msg.label = paste(msg.id,name,sender.name,sep="|",collapse=""),
             msg.code = paste(msg.id,name,collapse=""),
             msg.wave = .self$getMessageWave(msg.code),
             msg.wave.variant = .self$getMessageWaveVariant(msg.code),
             msg.wave.nr = .self$getMessageWaveBranch(msg.code)
           )
           %>% dplyr::filter(sends>.self$constants$minSends)
           %>% dplyr::filter(opens>.self$constants$minOpens)
           %>% dplyr::group_by()
           %>% dplyr::mutate(
             open.rate.rank = rank(open.rate),
             click.rate.rank = rank(click.rate),
             sends.rank = rank(sends),
             bounce.rate.rank = rank(bounce.rate),
             unsub.rate.rank = rank(unsub.rate)
           )
         )
         msgs$send.year = year(msgs$send.date)
         msgs$send.month = month(msgs$send.date)
         msgs$send.week = week(msgs$send.date)
         msgs$send.dayoftheweek = wday(msgs$send.date,label=T)
         msgs$send.month.label = paste(format(as.Date(msgs$send.date), '%Y'),format(as.Date(msgs$send.date), '%m'),sep="-")
         msgs$send.week.label = paste(format(as.Date(msgs$send.date), '%Y'),msgs$send.week,sep="-")
         msgs[] <- lapply(msgs,str_replace_all,"\n\r;","")
         msgs[,c("sends","opens","clicks","unsubs","bounces","accepted")] <- lapply(msgs[,c("sends","opens","clicks","unsubs","bounces","accepted")],as.numeric)
         
         .self$data$messages <- msgs
         .self$debugPrint(paste("Created messages dataset with",nrow(msgs),"rows"))
         .self$debugPrint(paste("Retrieve the dataset at $data$messages slot of your cepAggregator class instance"))
         invisible(return(TRUE))
       },
       aggregateMessages = function(msgs=NULL,groupby=NULL,addTotals=F,cummulate=F){
         if(is.null(msgs)){
           msgs <- .self$data$messages
         }
         .self$debugPrint(paste("Aggregating messages by",groupby))
         if(!is.null(groupby)){
           groupby <- unlist(strsplit(groupby,","))
           dots <- lapply(groupby,as.symbol)
         }else{
           dots <- NULL
         }
         
         agg <- msgs %>% dplyr::group_by_(.dots=dots) %>% dplyr::summarise(
           sends = sum(as.numeric(sends),na.rm=T),
           accepted = sum(as.numeric(accepted),na.rm=T),
           opens = sum(as.numeric(opens),na.rm=T),
           clicks = sum(as.numeric(clicks),na.rm=T),
           bounces = sum(as.numeric(bounces),na.rm=T),
           unsubs = sum(as.numeric(unsubs),na.rm=T),
           open.rate = round(opens/sends,2),
           click.rate = round(clicks/sends,2),
           click.thru.rate = round(clicks/opens,2),
           bounce.rate = round(bounces/sends,2),
           unsub.rate = round(unsubs/sends,2)
         )
         if(cummulate){
           agg <- agg %>% group_by_(.dots=dots[!(dots %in% c("send.month","send.month.label","send.year","send.week","send.week.label"))]) %>% mutate(
             cum.sends = cumsum(sends),
             cum.opens = cumsum(opens),
             cum.clicks = cumsum(clicks),
             cum.open.rate = round(cum.opens/cum.sends,2),
             cum.click.rate = round(cum.clicks/cum.sends,2),
             cum.click.thru.rate = round(cum.clicks/cum.opens,2)
           )
         }

         if(addTotals){
           .self$debugPrint(paste("Adding totals"))
           totalRow <- .self$aggregateMessages(msgs,groupby=NULL,cummulate = cummulate)
           totalRow[is.na(totalRow)]<-0
           totalRowHead <- data.frame(
             matrix(rep("-",length(groupby)),1, length(groupby),dimnames=list(c(), groupby)),
             stringsAsFactors=F
           )  
           totalRow <- cbind(totalRowHead,totalRow)
           agg <- rbind(as.data.frame(agg),totalRow)
         }
         return(agg)
       },
       makeNewMessageFlag = function(name=NULL,from=c("name","sender.name"),pattern=""){
         
       },
       getMessageTags = function(slot="message"){
         return(.self$data[paste0(slot,"tags")])
       },
       makeMessageTags = function(msgs=NULL,minLength=3,minFreq=10,negFilter=NULL,sep="_| "){
         if(is.null(msgs)){
           msgs <- .self$data$messages
         }
         tags <- c()
         msgTags <- lapply(unique(msgs$name),function(n){
           t <- unlist(strsplit(n,sep))
           t <- t[nchar(t)>=minLength]
           if(!is.null(negFilter)){
             t <- t[!grepl(negFilter,t)]
           }
           t <- tolower(t)
           return(list(name=n,tags=t))
         })
         tags <-unlist(lapply(msgTags,function(i) i$tags))
         tags <- names(table(tags))[table(tags)>minFreq]
         .self$data$tags <- tags
         .self$data$messagetags <- msgTags
         return(TRUE)
       },
       makeUsers = function(limit=NULL){
         if(length(.self$data$activity)==0){
           .self$data$activity <- .self$getData(lazyLoad = T)
         }
         if(is.null(limit)){
           rows <- nrow(.self$data$activity)
         }else{
           rows <- as.numeric(limit)
         }
         .self$debugPrint(paste("Making users"))
         usrs <- (
           .self$data$activity[1:rows,]
           %>% dplyr::group_by(u.id,email,domain)
           %>% dplyr::summarise(
             first.send=min(send.date),
             age = as.numeric(difftime(Sys.Date(),as.Date(first.send),units = "days")),
             last.send=max(send.date),
             first.open=min(open.date,na.rm=T),
             last.open=max(open.date,na.rm=T),
             last.click=max(click.date,na.rm=T),
             sends=length(send.time),
             opens=sum(nchar(open.date)>5),
             opens.last.365= sum(opens[send.date>as.character(Sys.Date()-365)]),
             opens.last.30 = sum(opens[send.date>as.character(Sys.Date()-30)]),
             opens.last.90 = sum(opens[send.date>as.character(Sys.Date()-90)]),
             clicks=sum(nchar(click.date)>5),
             bounces=sum(nchar(bounce.date)>5),
             accepted=(sends-bounces),
             unsubs=sum(unsub),
             open.rate=round(opens/sends,2),
             click.rate=round(clicks/sends,2),
             click.thru.rate=ifelse(clicks>0,round(clicks/opens,2),0),
             zombies = ifelse(sends>20 & opens==0,1,0),
             lovers = ifelse(sends>20 & open.rate>0.5,1,0),
             readers = ifelse(sends>20 & open.rate>0.5 & click.rate==0.0,1,0)
           )
         )
         .self$data$users <- usrs
         .self$debugPrint(paste("Created users dataset with",nrow(usrs),"rows"))
         invisible(return(TRUE))
       },
       makeContacts = function(pattern=NULL,path=NULL){
         if(is.null(pattern)){
           stop("Define a file 'pattern' argument that identifies CEP user profile exports")
         }
         if(is.null(path)){
           path <- .self$paths$exports
         }
         files <- list.files(path=path,pattern=pattern,full.names = T,recursive=T)
         .self$debugPrint(files)
         file <- max(files)
         .self$debugPrint(paste("Loading file",file))
         contacts <- read.csv(file,header=T,stringsAsFactors = F,sep=";")
         .self$debugPrint(paste("Contacts loaded with",nrow(contacts)," rows"))
         .self$data$contacts <- contacts
         invisible(TRUE)
       },
       getData = function(from=NULL,to=Sys.Date(),limit=NULL,lazyLoad=F){
         if(length(.self$data$activity)>0){
           d <- .self$data$activity
         }else{
           if(lazyLoad){
             d <- readRDS(paste0(.self$paths$store,"activity.rds"))
           }else{
             stop("Data not found, run makeData() or add the lazyLoad=T parameter")
           }
         }
         if(is.null(from)){
           from <- min(d$send.date)
         }
         d <- d[d$send.date>=as.character(from) & d$send.date<=as.character(to),]
         if(is.null(limit)){
           limit <- nrow(d)
         }
         d <- d[1:limit,]
         return(d)
       },
       getUsers = function(limit=NULL,lazyLoad=F,addContacts=F){
         if(length(.self$data$users)>0){
           d <- .self$data$users
         }else{
           if(lazyLoad){
             d <- readRDS(paste0(.self$paths$store,"users.rds"))
           }else{
             stop("Users not found, run makeUsers() or add the lazyLoad=T parameter")
           }
         }
         if(is.null(limit)){
           limit <- nrow(d)
         }
         d <- d[1:limit,]
         if(addContacts){
           if(length(.self$data$contacts)==0){
             .self$data$contacts <- .self$getContacts(lazyLoad=T)
           }
           d <- merge(d,.self$data$contacts,by.x="u.id",by.y="user.PK",all.x=T,all.y=F)
         }
         return(d)
       },
       getMessageHistory = function(users=NULL,messages=NULL){
         if(is.null(users)){
           stop("Users not provided, please provide at least 1 CEP User ID")
         }
         if(is.null(messages)){
           m <- .self$data$messages
         }
         if(is.null(messages)){
           stop("Messages not provided nor found in the $data$messages slot")
         }
         m <- m[m$uid %in% users,]
         return(m)
       },
       getMessages = function(from=NULL,to=Sys.Date(),limit=NULL,lazyLoad=F){
         if(length(.self$data$messages)>0){
           d <- .self$data$messages
         }else{
           if(lazyLoad){
             d <- readRDS(paste0(.self$paths$store,"messages.rds"))
           }else{
             stop("Messages not found, run makeMessages() or add the lazyLoad=T parameter")
           }
         }
         if(is.null(from)){
           from<-min(d$send.date)
         }
         d <- d[d$send.date>=from & d$send.date<=to,]
         if(is.null(limit)){
           limit<-nrow(d)
         }
         d <- d[1:limit,]
         return(d)
       },
       getMessageData = function(msgs=NULL){
         if(is.null(msgs)){
           stop("Supply the messages dataset, with msg.id column")
         }
         if(length(.self$data$activity)==0){
           .self$data$activity <- .self$getData(lazyLoad=T)
         }
         d <- .self$data$activity
         d <- d[d$msg.id %in% msgs$msg.id,]
         return(d)
       },
       getMessageUsers = function(msgs=NULL,interaction=NULL,addContacts=F){
         if(is.null(interaction)){
           interaction <- "sent"
         }
         stopifnot(interaction %in% c("sent","opened","clicked","bounced","unsubscribed"))
         if(length(.self$data$users)==0){
           .self$data$users <- .self$getUsers(lazyLoad=T)
         }
         d <- .self$getMessageData(msgs=msgs)
         d <- switch(interaction,
                sent = d,
                opened = d[d$open.date!="",],
                clicked = d[d$click.date!="",],
                bounce = d[d$bounce.date!="",],
                unsubscribed = d[d$unsub.date!="",]
         )
         u <- .self$data$users
         u <- u[u$u.id %in% d$u.id,]
         if(addContacts){
           if(length(.self$data$contacts)==0){
             .self$data$contacts <- .self$getContacts(lazyLoad=T)
           }
           u <- merge(u,.self$data$contacts,by.x="u.id",by.y="user.PK",all.x=T,all.y=F)
         }
         return(u)
       },
       compareMessageUsers = function(msgs=NULL,pattern=NULL,levelLimit=10,table=F){
         if(is.null(msgs)){
           stop("Supply the messages dataset, with msg.id column")
         }
         interactions <- c("sent","opened","clicked")
         d <- lapply(1:length(interactions),function(x) {
           audience <- .self$getMessageUsers(msgs = msgs, interaction=interactions[x],addContacts = T)
           audience$interaction <- interactions[x]
           return(audience)
         })
         d <- lapply(d,function(x){
           x <- x[,sapply(x,function(y) length(unique(y)))<levelLimit]
           x <- x[,grepl(paste0("interaction|",pattern),colnames(x))]
         })
         d <- do.call("rbind",d)
         if(table){
           comp <- lapply(colnames(d),function(x) table(d[,x],d[,'interaction']))
         }else{
           comp <- lapply(colnames(d),function(x) chisq.test(d[,x],d[,'interaction']))
         }
         return(comp)
       },
       getContacts = function(limit=NULL,lazyLoad=F){
         if(lazyLoad){
           d <- readRDS(paste0(.self$paths$store,"contacts.rds"))
         }else{
           d <- .self$data$contacts
         }
         return(d)
       },
       saveData = function(from=NULL,to=Sys.Date(),filename=NULL){
         if(is.null(filename)){
           filename <- paste0(.self$paths$store,"activity.rds")
         }
         d <- .self$getData(from=from,to=to)
         .self$debugPrint(paste("Data has",nrow(d),"rows"))
         .self$debugPrint(paste0("Saving data as ",filename))
         try(saveRDS(d,filename))
         invisible(return(TRUE))
       },
       saveMessages = function(from=NULL,to=Sys.Date(),filename=NULL){
         if(is.null(filename)){
           filename <- paste0(.self$paths$store,"messages.rds")
         }
         d <- .self$getMessages(from=from,to=to,limit=NULL)
         d <- .self$data$messages
         .self$debugPrint(paste("Messages have",nrow(d),"rows"))
         .self$debugPrint(paste("Saving messages as ",filename))
         try(saveRDS(d,filename))
         invisible(return(TRUE))
       },
       saveUsers = function(filename=NULL){
         if(is.null(filename)){
           filename <- paste0(.self$paths$store,"users.rds")
         }
         d <- .self$getUsers(limit=NULL)
         d <- .self$data$users
         .self$debugPrint(paste("Users have",nrow(d),"rows"))
         .self$debugPrint(paste("Saving users to ",filename))
         try(saveRDS(d,filename))
         invisible(return(TRUE))
       },
       saveContacts = function(filename=NULL){
         if(is.null(filename)){
           filename <- paste0(.self$paths$store,"contacts.rds")
         }
         d <- .self$getContacts(limit=NULL)
         d <- .self$data$contacts
         .self$debugPrint(paste("Saving contacts to ",filename))
         try(saveRDS(d,filename))
         invisible(return(TRUE))
       },
       getMessageArea = function(input){
         
       },
       getMessageWave = function(input){
         wave <- str_extract(input,"W([1-9])")
         wave <- str_trim(str_replace(wave,"W",""))
         if(is.na(wave)){
           return(1)
         }else{
           return(as.numeric(wave))
         }
       },
       getMessageWaveVariant = function(input){
         variant <- str_extract(input,"\\ W([1-9])(a|b|c|d|e|f|g)\\ ")
         if(is.na(variant)){
           return("W1")
         }else{
           return(variant)
         }
       },
       getMessageWaveBranch = function(input){
         b <- "R"
         b <- ifelse(grepl(.self$constants$nonresponder,input),"NR","R")
         return(b)
       },
       initialize = function(path=NULL,debug=F,filters=list(minSends=5)){
         .self$debug <- debug
         .self$debugPrint("Initializing CEP Aggregator")
         .self$loadPackages()
         .self$filters$patterns <- list(
           email = "orock|jan\\.fait|bluehornet|tdapps|xxx|feriancek|mapp\\.com|besiktasli|alexandra\\.green|julie\\.graham|Selinarose0|veronikaag|gburikova|communeer|ntara|teradata|ecircle|test|ponnies|mlv|tdappstest|angela.dein|jason\\.silverman\aprimo|student",
           domain = "mapp|ecircle|teradata|bluehornet|appoxee|aprimo",
           message = "seed list|Vishnu|Social Media Update|downtime|Downtime|Anmeldung|Trigger|Services Integration|Maintenance|Abmeldung|Test|Release_Unsubscribe|Inscription|Iscrizione|Password|Heartbleed|Mot de Passe|_ALF|List-Unsubscribe|Template|test",
           group = "seed list|Vishnu|Social Media Update|downtime|Downtime|Anmeldung|Trigger|Services Integration|Maintenance|Abmeldung|Test|Release_Unsubscribe|Inscription|Iscrizione|Password|Heartbleed|Mot de Passe|_ALF|List-Unsubscribe|Template|test"
         )
         .self$filters$ids <- list(
           message = c(),
           group = c(353454572,353503103),
           user = c() 
         )
         .self$constants$nonresponder <- "NR|nr"
         .self$constants$minSends <- filters$minSends
         .self$constants$minOpens <- 0
         .self$constants$maxMessageId <- 700000000
         .self$data <- list()

         if(is.null(path)){
           .self$debugPrint(paste("'path' was not specified, setting path to working directory"))
           .self$paths$store <- getwd()
           .self$paths$exports <- getwd()
         }else{
           .self$paths$store <- path
           .self$paths$exports <- path
         }
         .self$debugPrint("Initialization of CEP complete")
 
       }
     )
)