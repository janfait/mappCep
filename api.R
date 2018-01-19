if(!exists("libsLoaded")){
  source("/home/user/scripts/libs.R")
}
# Instead of creating a class all at once:
cep <- setRefClass("cep", 
  fields = list(
    debug = "logical",
    protocol = "character",
    username = "character",
    password = "character",
    version = "character",
    instance = "character",
    mirror = "character",
    format = "character",
    initializeTime = "POSIXt",
    attributes = "list",
    constants = "list",
    group = "list",
    user = "list"
  ),
  methods = list(
    debugPrint = function(a){
      if(.self$debug){
        cat(deparse(substitute(a)),"\n")
        if(is.data.frame(a)){
          str(a)
        }else{
          str(a)
          print(a)
        }
      }
    },
    root = function(){
      return(paste(.self$mirror,.self$instance,"api/rest",.self$version,sep="/"))
    },
    reset = function(){
      handle <- handle_find(paste0(.self$protocol,.self$mirror))
      handle_reset(handle$url)
    },
    call = function(domain=c("system","user","group","message","membership","contact","automation","content"),method=character(),body=list(),params=list(),jsonize=TRUE){
        u <- paste(.self$root(),domain,method,sep="/")
        u <- gsub(pattern = "//",replacement = "/",u,fixed = T)
        u <- paste0(.self$protocol,u)
        if(missing(params) || length(params)==0){
          q <- u
        }else{
          params <- paste(paste(names(unlist(params)),unlist(params),sep="="),collapse="&")
          q <- paste(u,params,sep="?")
        }
        .self$debugPrint(q)
        if(!missing(body)){
          if(jsonize){
            body <- .self$jsonize(body,domain,method)
          }
          .self$debugPrint(body)
          call<-httr::POST(
            url = q,
            add_headers("Accept"=.self$format,"Content-Type"=.self$format),
            authenticate(.self$username,.self$password),
            body=body
          )
        }else if(grepl("delete",domain)){
          call<-httr::DELETE(
            url = q,
            add_headers("Accept"=.self$format,"Content-Type"=.self$format),
            authenticate(.self$username,.self$password)
          )
        }else{
          call<-httr::GET(
            url = q,
            add_headers("Accept"=.self$format,"Content-Type"=.self$format),
            authenticate(.self$username,.self$password)
          )
        }
        r <- content(call)
        return(r)
    },
    jsonize = function(body,domain,method){
      
      a <- .self$getAttributes(domain)

      if(domain=="user"){
        standard <- a$standard
        custom <- a$custom
        d<-data.frame(name=names(body),value=unlist(body),row.names = NULL)
        d<-d[d$name %in% c(custom,standard),]
        json <- jsonlite::toJSON(d)
      }
      if(domain=="group" || domain=="membership"){
        if(method=="clone"){
          d <- body
          d <- body[names(body) %in% a$standard]
          json <- jsonlite::toJSON(d,auto_unbox = T)
        }else{
          d<-data.frame(name=names(body),value=unlist(body),row.names = NULL)
          json <- jsonlite::toJSON(d)
        }
      }
      .self$debugPrint(json)
      return(json)
    },
    getAttributes = function(domain){
      
      standard <- c()
      custom <- c()
      campaign <- c()
      
      if(domain=="user"){
        standard <- paste0("user.",unlist(strsplit(.self$attributes$user$standard,",")))
        custom <- paste0("user.CustomAttribute.",unlist(strsplit(.self$attributes$user$custom,",")))
        campaign <- paste0(unlist(strsplit(.self$attributes$user$campaign,",")))
      }
      if(domain=="group"){
        standard <- paste0(unlist(strsplit(.self$attributes$group$standard,",")))
        custom <- paste0(unlist(strsplit(.self$attributes$group$custom,",")))
        campaign <- c()
      }
      return(list(standard=standard,custom=custom,campaign=campaign))
    },
    find = function(domain,params=list(),body=list(),identifier){
      
      if(domain=="user"){
        if(length(params$userId)>0){
          .self$debugPrint("Using Id")
          find <- .self$call(domain=domain, method="get",params=params)
          find <- find$id
        }else if(length(params$email)>0){
          .self$debugPrint("Using Email")
          find <- .self$call(domain=domain, method="getByEmail",params=params)
          find <- find$id
        }else if(!missing(identifier) & length(body[[identifier]])>0){
          .self$debugPrint("Using Identifier")
          find <- .self$call(domain=domain, method="getByIdentifier",params=params)
          find <- find$id
        }else{
          stop("Provide 'userId','email' in the params argument or provide an 'identifier' argument that has a value pair in the body")
        }
      }

      if(domain=="group"){
        if(length(params$groupId)>0){
          .self$debugPrint("Using Id")
          find <- .self$call(domain=domain, method="get",params=params)
          find <- find$id
        }else if(!missing(identifier)){
          .self$debugPrint("Using Identifier")
          .self$debugPrint(length(body[[identifier]]))
          findBody <- list(body[[identifier]])
          names(findBody)<-identifier
          .self$debugPrint(findBody)
          find <- .self$call(domain=domain, method="findIdsByAttributes",body=findBody)
          .self$debugPrint(str(find))
          if(length(find)>0){
            find <- find[[1]]
          }
        }else{
          stop("Provide 'groupId' in the params argument or provide an 'identifier' argument that has a value pair in the body")
        }
        return(find)
      }
    },
    upsert = function(domain,params,body,identifier){
      
      if(!(domain %in% c("user","group","membership"))){
        stop("Only 'user','group' and 'membership' domains are allowed for the upsert method")
      }

      if(domain=="user"){
        update <- .self$call(domain = domain,method = "updateProfileByEmail",params = params,body = body)
        if(is.null(update)){
          find <- .self$call(domain = "user", method = "getByEmail", params=params)
          result <- list(errorActor=F,errorCode=F,message="Success",resourceId=find$id)  
        }else{
          if(update$errorCode=="NO_SUCH_OBJECT"){
            create <- .self$call(domain=domain,method = "create",params = params,body = body)
            result <- create
          }else{
            result <- update
          }
        }
      }
      
      if(domain=="group"){
        find<- .self$find(domain,params,body,identifier)
        if(length(find)>0){
          update <- .self$call(domain=domain,method="setAttributes",params=list(groupId=find),body=body)
          if(is.null(update)){
            result <- list(errorActor=F,errorCode=F,message="Success",resourceId=find)
          }else{
            result <- update
          }
        }else{
          create <- .self$call(domain=domain,method="clone",params=list(groupId=.self$constants$defaultGroupId),body=body)
          if(length(create$errorCode)>0){
            result <- create 
          }else{
            update <- .self$call(domain=domain,method="setAttributes",params=list(groupId=create$id),body=body)
            result <- list(errorActor=F,errorCode=F,message="Success",resourceId=create$id)  
          }
        }  
      }

      if(domain=="membership"){
        create <- .self$call(domain=domain,method = "create",params = params)
        update <- .self$call(domain=domain,method = "updateAttributes",params = params,body = body)
        result <- list(errorActor=F,errorCode=F,message="Success",resourceId=NA) 
      }
      
      return(result)
    },
    addToContentStore = function(data="",contentId=NULL,name=NULL,type="application/json"){
      
      if(is.null(name)){
        stop("'name' parameter is required")
      }
      
      data <- as.character(RCurl::base64Encode(data))
      body <- list(name=name,content=data,contentType=type)
      body <- jsonlite::toJSON(body,auto_unbox = T)
      
      #check if we're updating
      if(!is.null(contentId)){
        params <- list(contentId=contentId)
        update <- .self$call(domain="content",method="update",params=params,body=body,jsonize = F)
        return(update)
      }else{
        params <- list(contentId=contentId)
        create <- .self$call(domain="content",method="store",body=body,jsonize = F)
        return(create)
      }
      
      
    },
    getGroupSettingTemplates = function(){
      groupSettings <- .self$call(domain="group",method="getAllGroupSettingsTemplates",params=list())
      return(groupSettings)
    },
    settingsExample = function(){
      settingsExample <-'{"cep": {"systemname": {"users":{"api":{"username":"username1","password":"xxxxx"},"ui":{"username":"username2","password":"xxxxx"}},
      "attributes": {"user":{"standard":"Identifier,PK,Email,Title,FirstName,LastName,ISOLanguageCode,ISOCountryCode","custom": "attribute1,attribute2","campaign": "attribute1,attribute2"},
      "group":{"standard":"name,email,description,includeTestUsers,includePreparedMessages","custom":"attribute1,attribute2"}},
      "constants":{"defaultGroupId":353588578}}}}'
      return(jsonlite::prettify(settingsExample))
    },
    settingsCheck = function(settings){
      return(settings)
    },
    initialize = function(system,settings,debug=F){
      
      if(missing(settings)){
        stop("Please supply the settings as JSON, list or environment object")
      }else{
        settings <- .self$settingsCheck(settings)
      }  
      if(!require("httr") || !require("jsonlite")){
        stop('Please install the "httr" and "jsonlite" packages before using this class')
      }
      
      #initialize with login values
      .self$debug <- debug
      .self$mirror <- "amundsen.shortest-route.com"
      .self$username <- settings$cep[[system]]$users$api$username
      .self$password <- settings$cep[[system]]$users$api$password
      .self$constants <- list(
        defaultGroupId=settings$cep[[system]]$constants$defaultGroupId,
        defaultGroupTemplateId=settings$cep[[system]]$constants$defaultGroupTemplateId
      )
      .self$instance <- system
      .self$protocol <- "https://"
      .self$format <- "application/json"
      .self$version <- "v6"
      .self$initializeTime <- Sys.time()
      .self$attributes <- settings$cep[[system]]$attributes
      
      
      #initialize polymorhph group
      
      
      #initialize polymorhphic user
      
      
      return(.self)
    }
  )
)



