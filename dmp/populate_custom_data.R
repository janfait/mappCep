#################
# SETUP
#################
rm(list=ls())

source("/home/user/scripts/libs.R")
source("/home/user/scripts/cep/dmp/api.R")
source("/home/user/scripts/cep/api.R")

#################
# CLASS INSTANCES
#################

dmp <- mappDmp$new(settings$dmp$username,settings$dmp$password,debug=T)
dmc <- cep$new("ecircle_marketing",settings=settings,debug=F)

################
# DATA
################

d <- readRDS("/data/dmp/dmp.rds")


groupMap <- data.frame(
  event_url = c("let-your-data-work|data-management-platform","","","let-your-data-work|data-management-platform"),
  event = c("Whitepaper|PDF","About","Feature","Page"),
  group_id = c(353604047,353602310,353602309,353603343)
)

################
# EDIT
################

#filter for events
d <- dmp$removeInvalidUsers(d)
u <- dmp$getCepUsers(d)
e <- dmp$getCustomEvents(data=d,users=u,pattern = "Email|DMP|PDF")
e <- dmp$applyFilter(data = e, filter=list(pixel_id="16234"))

e <- extractQueryParameter(e,parameter="icid",from=c("event_url","event_referer_url"),extractUserId = T)

#consolidate the UID
e$icid_uid <- extractUidFromIcid(e$icid)
e$uid_final <- ifelse(is.na(e$uid) | e$uid %in% c(0,""),e$icid_uid,e$uid)
#select PDF downloads and whitepaper events
e <- e[grepl("PDF|Whitepaper",e$event) & !is.na(e$uid_final),]

#create memberships
memberships <- apply(groupMap,1,function(g){
  apply(e,1,function(row){
    match_event_url <- grepl(g['event_url'],row['event_url'])
    match_event <- grepl(g['event'],row['event'])
    if(match_event_url & match_event){
      Sys.sleep(5)
      print("Inserting")
      m <- dmc$upsert(
        domain = "membership",
        params = list(groupId=unname(g['group_id']),userId=unname(row['uid_final'])),
        body=list(medium=row['utm_medium'],source=row['utm_source'],campaign=row['utm_campaign'],icid=row['icid'])
      )
      print(m)
    }
  })
})


