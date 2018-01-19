source("/home/user/scripts/initialize.R")

createMany <- function(n=10,start=1,domain="",namepattern="",emailpattern="",cloneId=353603988){
  out <- lapply(start:n,function(i){
    body <- list(
      name = paste0("MappWeb_RC_",i),
      email = paste0("mappweb_rc_",i,"@news.mapp.com"),
      description = paste0("Mapp.com Resource center group ",i),
      includeTestUsers=F,
      includePreparedMessages=F
    )
    r<-e$call(domain="group",method="clone",params=list(groupId=353603988),body=body)
    return(r)
  })
  return(out)
}



tagToSolution <- data.frame(
  wcp_tag = c("Mobile Marketing","Email Marketing","Digital Marketing Services","Customer Engagement","Data Management","Social Media Marketing"),
  group_attribute = c("mobile","email","digital_services","email","dmp","social")
)

rc <- read.csv("/data/utilities/tx_wcp_resource_item_250717-1719.csv",sep=",",stringsAsFactors = F) 
rc <- rc[rc$api_group_id!="0",]
rc$wcp_tag <- unlist(lapply(strsplit(rc$tx_wcp_tags,";"),function(x) x[1]))
rc <- merge(rc,tagToSolution,by="wcp_tag")
rc$value <- TRUE
rc <-reshape2::dcast(rc,"uid+title+api_group_id~group_attribute",value.var = "value",fill = FALSE)

e <- cep$new(system="ecircle_marketing",settings=settings,debug=T)

rcResult <- apply(rc,1,function(r){
  
  body <- as.list(r)

  res <- e$call(
    domain="group",
    method="setAttributes",
    params=list(groupId=unname(r['api_group_id'])),
    body=body
  )
  return(res)
  
})





