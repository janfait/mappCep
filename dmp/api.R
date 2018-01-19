mappDmp <- setRefClass("mappDmp",
   fields=list(
     debug = "logical",
     settings = "list",
     root = "character",
     endpoints = "list",
     username = "character",
     password = "character",
     authentication = "list",
     dictionary = "list",
     cache = "list",
     data = "list",
     log="character",
     filter = "list",
     paths = "list"
   ),
   methods=list(
     debugPrint = function(x){
       if(.self$debug){
         invisible(.self$log <- paste(.self$log,print(x),"\n"))
       }
     },
     loadPackages = function(){
       .self$debugPrint("Loading packages")
       if(!all(require("httr",quietly = T),require("jsonlite",quietly = T),require("dplyr",quietly = T),require("urltools",quietly = T),require("lubridate",quietly = T))){
         stop("The MappDmp class requires the following R packages: httr,lubridate,jsonlite,urltools,dplyr")
       }
     },
     daysAgo = function(days=0){
       return(as.character(Sys.Date()-days))
     },
     validateFilters = function(input){
       
       if(missing(input)){
         .self$debugPrint("Running with defualt date filters")
         output <- .self$dictionary$filters$default
         return(output)
       }
       
       if(is.list(input)){
         if("date" %in% unlist(input) & "date_start" %in% names(unlist(input)) & "date_end" %in% names(unlist(input))){
           if(all(sapply(input,is.list)) & sum(sapply(input,is.list))==length(input)){
             output <- lapply(input,function(f){
               if(!(f$dimension %in% c("date","pixel_id","platform"))){
                 f$dimension<- paste0("flx_",f$dimension)
               }
               return(f)
             })
             
           }else{
             stop("Filters must be a list of lists, some of the elements of the top node are not lists")
           }
         }else{
           stop("When specified, filters must contain the 'date' dimension. Example: list(dimension='date',date_start='2017-01-01',date_end='2017-01-31'")
         }
       }else{
         stop("filters must be a list of lists. Example: list(list(dimension='x' ...),list(dimension='y',....))")
       }
       return(output)
     },
     validateIdentifiers = function(input){
       
       what <- deparse(substitute(input))
       .self$debugPrint(paste("Validating",what))
       
       if(missing(input)){
         output <- .self$dictionary[[what]]$default
         output <- unlist(strsplit(output,","))
         output <- paste0("flx_",output)
         .self$debugPrint(paste("Selected",what,":",paste(output,collapse=",")))
       }else if(input=="*"){
         output <- .self$dictionary[[what]]$all
         output <- unlist(strsplit(output,","))
         output <- paste0("flx_",output)
       }else{
         output <- .self$dictionary[[what]]$all
         input <- unlist(strsplit(input,","))
         output <- unlist(strsplit(output,","))
         .self$debugPrint(paste("Removing the non-existent:",paste(input[!(input %in% output)],collapse=",")))
         output <- input[input %in% output]
         output <- paste0("flx_",output)
         .self$debugPrint(paste("Selected are:",paste(output,collapse=",")))
       }
       return(output)
     },
     prepareQuery = function(dimensions,measures,filters,limit){
       dimensions <- .self$validateIdentifiers(dimensions)
       measures <- .self$validateIdentifiers(measures)
       filters <- .self$validateFilters(filters)
       .self$debugPrint(paste("Validation complete"))
       .self$debugPrint(dimensions)
       .self$debugPrint(measures)
       if(missing(limit)){
         limit <- .self$dictionary$limit$default
       }
       .self$debugPrint(paste("Preparing query"))
       query <- list(dimensions=dimensions,measures=measures,filters=filters,limit=limit)
       .self$cache$query <- list()
       .self$cache$query$parsed <- query
       query <- jsonlite::toJSON(query,auto_unbox=T)
       .self$cache$query$json <- query
       .self$debugPrint(paste("JSON query:",query))
       query <- urltools::url_encode(query)
       .self$cache$query$encoded <- query
       query <- paste0("x=[",query,"]")
       return(query)
     },
     postCall = function(url,body){
       if(.self$checkLogin()){
         url <- paste0(url,"?csrf=",.self$authentication$csrf)
         .self$debugPrint(paste("Calling",url))
         call <- httr::POST(url,body=body)
         callResult <- httr::content(call,"parsed")
         return(callResult)
       }else{
         stop("Authentication has failed")
       }
     },
     getCall = function(url){
       if(.self$checkLogin()){
         .self$debugPrint(paste("Calling",url))
         call <- httr::GET(url)
         callResult <- httr::content(call,"parsed")
         return(callResult)
       }else{
         stop("Authentication has failed")
       }
     },
     login = function(){
       authString <- paste0("username=",.self$username,"&password=",.self$password)
       .self$debugPrint(paste("Authenticating with",.self$username))
       callResult <- httr::POST(url=.self$endpoints$auth,body=authString)
       .self$authentication <- httr::content(callResult,"parsed")
       if(length(.self$authentication$response$logged_in)==0){
         .self$debugPrint("Authentication failed")
         .self$debugPrint(.self$authentication$response)
         return(FALSE)
       }else{
         .self$debugPrint("Authentication successful")
         return(TRUE)
       }
     },
     getCurrentUser = function(){
       url <- paste0(.self$endpoints$currentuser)
       callContent <- .self$getCall(url)
       if(callContent$response$status=="OK"){
         data <- callContent$response$user
         return(data)
       }else{
         return(callContent)
       } 
     },
     checkLogin = function(){
       now <- as.POSIXlt(Sys.time(), "GMT")
       expiry <- as.POSIXlt(.self$authentication$debug$now,"GMT")+1800
       .self$debugPrint(paste("Checking login at",now,", Expires at",expiry))
       .self$debugPrint(paste("Token is valid:",now<=expiry))
       if(now>=expiry){
         .self$login()
       }else{
         return(TRUE)
       }
     },
     getPixels = function(){
       url <- paste0(.self$endpoints$trackinglist)
       callContent <- .self$getCall(url)
       if(callContent$response$status=="OK"){
         d <- callContent$response$beacons
         d$methods <- paste(unlist(d$permissions$methods),collapse=",")
         d$permissions <- NULL
         d <- as.data.frame(d)
         return(d)
       }else{
         return(callContent)
       } 
     },
     getNames = function(dimension=character(),search=NULL){
       if(substring(dimension,1,3)!='flx'){
         dimension <- paste0("flx_",dimension)
       }
       url <- paste0(.self$endpoints$names,"?dimension=",dimension,ifelse(is.null(search),"",paste0("&search=",search)))
       callContent <- .self$getCall(url)
       if(callContent$response$status=="OK"){
         data <- callContent$response$items
         data <- as.data.frame(do.call("rbind",lapply(data,unlist)),stringsAsFactors=F)
         return(data)
       }else{
         return(callContent)
       } 
     },
     makeDateVars = function(data){
       d <- .self$checkDataInput(data,c("date"))
       date <- as.Date(d$date)
       d$week <- lubridate::floor_date(date,"week")
       d$month <- lubridate::floor_date(date,"month")
       d$quarter <- update(date,months = ceiling(month(date)/3) * 3 - 2, mdays = 1)
       d$year <- lubridate::floor_date(date,"year")
       d$month.label <- paste(lubridate::year(date),lubridate::month(date,label = F),lubridate::month(date,label = T),sep="-")
       d$week.label <- paste(lubridate::year(date),lubridate::week(date),sep="-")
       d$year.label <- lubridate::year(date)
       return(d)
     },
     makeNewVars = function(data){
       d <- data
       if(.self$settings$makeNewVars){
         if("event_url" %in% colnames(d)){
           d$event_url_trunc <- tolower(gsub("\\?(.*)$","",d$event_url))
           d$event_url_trunc <- gsub("https://mapp.com|http://mapp.com","",d$event_url_trunc)
           d$event_url_trunc <- gsub("/fr/|/de/|/it/|/us/|/uk/","/home/",d$event_url_trunc)
           d$event_url_trunc <- gsub("//","/",d$event_url_trunc,fixed=T)
           params <- urltools::param_get(d$event_url,c("utm_medium","utm_source","utm_campaign","uid"))
           d <- cbind(d,params)
           d$uid[nchar(d$uid)!=10] <- 0
           d$uid <- as.numeric(d$uid)
         }
         if("interaction_timeonsite" %in% colnames(d)){
           seconds <- c("null"=0,"5 seconds"=5,"10 seconds"=10,"20 seconds"=20,"30 seconds"=30,"1 minute"=60,"1.5 minutes"=90,"2 minutes"=120,"3 minutes"=180,"4 minutes"=240,"5 minutes"=300)
           d$interaction_timeonsite_sec <- seconds[match(d[,"interaction_timeonsite"],names(seconds))]
         }
       }
       return(d)
     },
     checkDataInput = function(data=NULL,requiredCols=NULL){
       .self$debugPrint("Checking data input")
       if(is.null(data)){
         .self$debugPrint("Data not supplied, taking data from $data$latest slot")
         d <- .self$data$latest
       }else{
         d <- data
       }
       if(is.null(d)){
         stop("Data not supplied and not available in $data$latest slot")
       }
       if(!is.null(requiredCols)){
         if(any(!(requiredCols %in% colnames(d)))){
           stop(paste("Your data has to include the following columns:",requiredCols,collapse=" "))
         }
       }
       .self$debugPrint("Check sucessful")
       return(d)
     },
     makeTimeDistribution = function(data=NULL){
       d <- .self$checkDataInput(data,requiredCols=c("interaction_timeonsite_sec"))
       d <- d[d$interaction_type=='Smart Pixel - Time on Site' | d$interaction_type=="8",]
       distr <- d %>% dplyr::group_by(interaction_timeonsite_sec) %>% dplyr::summarise(n=n()) 
       return(distr)
     },
     makeSessionData = function(data=NULL,max_interruption=3600){
       d <- .self$checkDataInput(data=data,requiredCols=c("uuid","timestamp","date","interaction_timeonsite_sec"))
       d$timestamp_date <- as.POSIXct(d$timestamp)
       .self$debugPrint("Computing session variables")
       s <- d %>% dplyr::group_by(uuid,date) %>% dplyr::arrange(uuid,timestamp_date) %>%mutate(
         diff = as.numeric(timestamp_date - lag(timestamp_date,default = 0), units="secs"),
         session_start = cumsum(ifelse(diff>max_interruption,1,0)),
         session_page = match(event_url_trunc,unique(event_url_trunc)),
         session_duration = as.numeric(max(timestamp_date) - min(timestamp_date), units="secs"),
         page_depth = length(unique(event_url_trunc))
       )  %>% dplyr::ungroup() 
       return(s)
     },
     makeLabels = function(data){
       d<-data
       if(.self$settings$makeLabels){
         data[] <- lapply(colnames(d),function(col){
           if(col %in% unlist(strsplit(.self$dictionary$dimensions$named,","))){
             names <-dmp$getNames(dimension=col)
             d[,col]<-names$name[match(d[,col],names$id)]
           }else{
             d[,col]
           }
         })
       }
       return(d)
     },
     getDimensions = function(pattern=NULL){
       if(is.null(pattern)){
         dimensions <- unlist(strsplit(.self$dictionary$dimensions$all,","))
       }else{
         dimensions <- unlist(strsplit(.self$dictionary$dimensions$all[pattern],","))
       }
       return(dimensions)
     },
     getData = function(dimensions=NULL,measures=NULL,filters=NULL,limit=NULL,makeLabels=F,makeNewVars=F,query=NULL){
       .self$settings$makeLabels <- makeLabels
       .self$settings$makeNewVars <- makeNewVars
       
       if(is.null(query)){
         query <- .self$prepareQuery(dimensions,measures,filters,limit)
       }else{
         query <- urltools::url_encode(query)
         .self$cache$query$encoded <- query
         query <- paste0("x=[",query,"]")
       }
       callContent<- .self$postCall(url=.self$endpoints$data,body=query)
       if(callContent$response$status=="OK"){
         d <- callContent$response$data[[1]]$data
         d <- do.call("rbind",d)
         d <- as.data.frame(d)
         colnames(d)<-gsub("flx_","",colnames(d))
         d[] <- apply(d,2,unlist)
         d <- .self$makeLabels(d)
         d <- .self$makeNewVars(d)
         .seld$data$latest <- d
         return(d)
       }else{
         return(callContent)
       }
     },
     getExports = function(){
       callContent <- .self$getCall(.self$endpoints$listexports)
       if(callContent$response$status=="OK"){
         data <- callContent$response$exports
         return(data)
       }else{
         return(callContent)
       }
     },
     removeInvalidUsers = function(data=NULL,eventLimit=1000,blacklist=NULL){
        d <-.self$checkDataInput(data) 
       .self$debugPrint(paste("Received data with",nrow(d),"rows"))
       if(length(d$uuid)>0){
         d <- d[nchar(d$uuid)==36,]
         if(length(d$event_url)>0){
           debug <- unique(d[grepl("flx1debug",d$event_url),"uuid"])
           d <- d[!(d$uuid %in% debug),]
         }
         interactions <- table(d$uuid)
         valid <- names(interactions)[interactions<eventLimit]

         d <- d[d$uuid %in% valid,]
         if(!is.null(blacklist)){
           d <- d[!(d$uuid %in% blacklist),]
         }
         .self$debugPrint(paste("Returning data with",nrow(d),"rows"))
         return(d)
       }else{
         stop("Cannot remove invalid users from data without a 'uuid' column")
       }
     },
     getUniqueUsers = function(data=NULL){
       d <-.self$checkDataInput(data)
       if(length(d$uuid)>0){
         return(unique(d$uuid))
       }else{
         stop("Cannot remove invalid users from data without a 'uuid' column")
       }
     },
     getCustomEvents = function(data=NULL,users=NULL,from=NULL,pattern="",pixel=NULL){
       data <- .self$checkDataInput(data=data,requiredCols=c("interaction_type","conversion_dmp","segment_dmp"))
       if(!(is.null(pixel)) & length(data$pixel_id)>0){
         data <- data[data$pixel_id==pixel,]
       }
       .self$debugPrint(paste("Received data with",nrow(data),"rows"))
       data <- data[grepl("Conversion|10|15",data$interaction_type),]
       if(!is.null(users) & length(data$uuid)>0){
         .self$debugPrint(paste("Filtering for users"))
         data <- data[data$uuid %in% users,]
       }
        .self$debugPrint(paste("Returning data with",nrow(data),"rows"))
       if(length(data$date)>0){
         if(!is.null(from)){
           data <- data[data$date>=from,]
         }
       }else{
         .self$debugPrint("Not supplied the 'date' column, cannot filter out based on from/to parameters")
       }
       if(nrow(data)>0){
         data$event <- ""
         data$event[!is.na(data$segment_dmp)] <- data$segment_dmp[!is.na(data$segment_dmp)]
         data$event[!is.na(data$conversion_dmp)] <- data$conversion_dmp[!is.na(data$conversion_dmp)]
         data <- data[grepl(pattern,data$event),]
         return(data)
       }else{
         return(data)
       }
     },
     getCepUsers = function(data=NULL,params=c("uid"),pattern="icid=email|ICID=email|uid="){
       data <- .self$checkDataInput(data=data,requiredCols=c("event_url","uuid"))
       if(length(data['uid'])==0){
         data <- .self$makeNewVars(data)
       }
       data <- data[grepl(pattern,data$event_url) | grepl(pattern,data$event_referer_url),]
       return(unique(data$uuid))
     },
     applyFilter = function(data=NULL,filter=list(event_url=NULL,utm_source=NULL,utm_medium=NULL,utm_campaign=NULL)){
       data <- .self$checkDataInput(data=data)
       filter <- filter[!sapply(filter,is.null)]
       fData <- data
       if(length(filter)>0){
         for(name in names(filter)){
           if(name %in% colnames(data)){
             fData <- fData[grepl(filter[[name]],fData[,name]),]
           }
         }
         return(fData)
       }
       return(data)
     },
     createPaths = function(data=NULL,maxsteps=5,startcol=3,fromColumn="section"){
       
       requiredCols <- c("uuid","date","session_page",fromColumn)
       data <- .self$checkDataInput(data=data,requiredCols=requiredCols)
       data <- unique(data[,requiredCols])
       data["dmp_temp_step"] <- data[fromColumn]
       .self$debugPrint("Preparing step column")
       data[fromColumn]<-NULL
       .self$debugPrint("Aggregating")
       .self$debugPrint(colnames(data))
       data <- reshape2::dcast(data,"uuid + date ~ session_page",value.var = "dmp_temp_step",fun.aggregate = paste,collapse=";")
       .self$debugPrint(colnames(data))
       paths <- data[,startcol:(startcol+maxsteps)]
       paths.plot <- data.frame()
       .self$debugPrint("Preparing paths")
       for (i in 2:ncol(paths)) {
         paths.cache <- (
           paths
           %>% group_by(paths[ , i-1], paths[ , i])
           %>% summarise(n=n())
           %>% ungroup()
         ) 
         colnames(paths.cache)[1:2] <- c('from', 'to')
         paths.cache$from <- paste(paths.cache$from, '(', i-1, ')', sep='')
         paths.cache$to <- paste(paths.cache$to, '(', i, ')', sep='')
         paths.plot <- rbind(paths.plot, paths.cache)
       }
       return(paths.plot)
     },
     getCampaignEffect = function(data=NULL,medium="",source="",campaign="",url="",pattern="",days = 7,pixel = NULL){
       
       d <- .self$checkdInput(data,requiredCols = c("utm_medium","utm_source","utm_campaign"))
       campaignd <- d[grepl(medium,d$utm_medium) & grepl(source,d$utm_source) & grepl(campaign,d$utm_campaign) & grepl(url,d$event_url),]
       users <- .self$getUniqueUsers(d = campaignd,pixel=pixel)
       
       hitDates <- (
         campaignd
         %>% dplyr::group_by(uuid)
         %>% dplyr::summarise(
           hitdate = min(date,na.rm=T)
         )
       )
       
       d <- merge(d,hitDates,by="uuid")
       d <- .self$getCustomEvents(d,users=users,pattern=pattern)
       d <- .self$removeInvalidUsers(d = d)
       
       effect <- (
         d 
         %>% dplyr::group_by(uuid)
         %>% dplyr::filter(as.date(date)>=hitdate & as.date(date)<as.date(hitdate)+days)
         %>% dplyr::group_by(event,date)
         %>% dplyr::summarise(
           users = length(unique(uuid))
         )
         %>% dplyr::arrange(date)
       ) 
       return(effect)
       
     },
     removeAudienceUpload = function(data=NULL){
       d <- .self$checkDataInput(data,requiredCols=c("interaction_type","event_type"))
       d <- d[d$interaction_type!="20" & d$interaction_type!="Already Uploaded",]
       d <- d[d$event_type!="8" & d$interaction_type!="Audience Upload",]
       return(d)
     },
     getExport = function(id,getContent=T){
       url <- paste0(.self$endpoints$export,"?id=",id)
       call <- GET(url)
       if(getContent){
         file<- tempfile()
         try(writeBin(content(call,"raw"), file))
         d<-read.csv(file,sep=",",header=T,stringsAsFactors=F)
         rm(file)
         colnames(d)<-gsub("flx_","",colnames(d))
         d <- .self$makeLabels(d)
         d <- .self$makeNewVars(d)
         .self$data$latest <- d
         return(d)
       }else{
         file <- paste0("MappDMPExport_",Sys.time(),"_",sample(1000:9999,1),".csv")
         try(writeBin(content(call,"raw"), file))
         return(file)
       }
     },
     getBatch = function(dimensions,measures,filters,period=10,attempts=20,makeLabels=F,makeNewVars=F,getContent=T){
       .self$settings$makeLabels <- makeLabels
       .self$settings$makeNewVars <- makeNewVars
       query <- .self$prepareQuery(dimensions,measures,filters,limit=.self$dictionary$limit$maxvolume)
       callContent<- .self$postCall(url=.self$endpoints$batch,body=query)
       if(callContent$response$status=="OK"){
         exportId <- callContent$response$id
         exportReady <- F
         attemptCounter <- 0
         while(!exportReady){
           Sys.sleep(period)
           exports <- .self$getExports()
           exportReady <- any(sapply(exports,function(export) ifelse(export$id==exportId & export$state=='COMPLETED',TRUE,FALSE)))
           if(attemptCounter==attempts){
             stop("Was not able to retrieve the export after defined number of attempts")
           }
           .self$debugPrint(paste("Attempt number",attemptCounter))
           attemptCounter <- attemptCounter+1
         }
         export <- .self$getExport(id=exportId,getContent=getContent)
         return(export)
       }else if(callContent$response$status=="ERROR" & grepl("This report has already",callContent$response$error)){
         exportId <- callContent$response$id
         export <- .self$getExport(exportId)
         .self$data$latest <- export
         return(export)
       }else{
         return(callContent)
       }
     },
     saveData = function(slot=NULL,filename=NULL){
       if(is.null(filename)){
         filename <- paste(.self$paths$store,"dmp.rds")
       }
       if(is.null(slot)){
         d <- .self$data$latest
       }else{
         d <- .self$data[[slot]]
       }
       .self$debugPrint(paste("Saving to",filename))
       saveRDS(d,filename)
       return(TRUE)
     },
     initialize = function(username,password,debug=FALSE,store=getwd()){
       #set parameters to fields
       .self$settings <- list()
       .self$settings$makeLabels <- F
       .self$settings$makeNewVars <- F
       .self$paths$store <- store
       .self$debug <- debug
       .self$username <- username
       .self$password <- password
       .self$data <- list()
       #set dictionary
       .self$dictionary <- list(
         dimensions = list(
           all="advertiser_id,auction_id,browser,buyer_id,campaign_id,conversion_dmp,creative_id,creative_size,date,day_in_month,day_in_week,day_in_year,destination_url,device_brand,device_id_md5,device_id_sha1,device_id_openudid,device_id_odin,device_id_apple_ida,device_id_google_adid,device_type,event_referer_url,event_type,event_url,external_data,external_pixel_id,geo_city,geo_country,geo_lat,geo_long,geo_region,hour,insertion_order_id,interaction_type,interaction_value,interaction_webform,lineitem_id,month_in_year,operating_system,pixel_id,placement_id,platform,platform_exchange,publisher_id,segment_dmp,seller_id,site_domain,site_id,site_type,timestamp,user_agent,user_ip,user_ip_truncated,uuid,week_in_year,clicks_dmp,impressions_dmp,interaction_adhover,interaction_pagescroll,interaction_timeonsite,interactions_dmp,pixel_loads_dmp,record_sum,total_events_dmp,unique_users_approx_dmp",
           default="uuid,date,timestamp,pixel_id,event_type,segment_dmp,conversion_dmp,event_referer_url,event_url,interaction_type,interaction_pagescroll,interaction_timeonsite",
           named="segment_dmp,conversion_dmp,interaction_type,event_type"
         ),
         measures = list(
           all = "impressions_dmp,interactions_dmp,clicks_dmp",
           default = "impressions_dmp,interactions_dmp,clicks_dmp,"
         ),
         filters = list(
           default = as.data.frame(list(dimension = "date", date_start=Sys.Date()-30, date_end = Sys.Date()),stringsAsFactors = F)
         ),
         limit = list(
           default = "1000",
           maxvolume = "10000000"
         )
       )
       .self$debugPrint("Initializing Mapp DMP instance")
       invisible(.self$loadPackages())
       .self$root <- "https://platform.flxone.com/api"
       .self$endpoints <- list(
         auth=paste0(.self$root,"/auth"),
         api=paste0(.self$root,"/"),
         data=paste0(.self$root,"/viz/data"),
         batch=paste0(.self$root,"/viz/batch-export"),
         listexports=paste0(.self$root,"/viz/list-exports"),
         export=paste0(.self$root,"/viz/export"),
         names=paste0(.self$root,"/report-central/search"),
         trackinglist = paste0(.self$root,"/tracking/list"),
         currentuser = paste0(.self$root,"/user/current")
       )
       .self$cache <- list()
       if(!.self$login()){
         stop("Authentication has failed, please check your credentials")
       }
     }
   )
)








