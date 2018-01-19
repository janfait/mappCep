#################
# SETUP
#################
rm(list=ls())

source("/home/user/scripts/libs.R")
source("/home/user/scripts/cep/dmp/api.R")
source("/home/user/scripts/ga/api.R")

#################
# CLASS INSTANCES
#################

dmp <- mappDmp$new(settings$dmp$username,settings$dmp$password,debug=T)
g <- googleAnalytics$new(settings=settings,debug=T)

################
# DATA
################

d <- readRDS("/data/dmp/dmp.rds")

################
# EDIT
################

#filter for events
d <- dmp$removeInvalidUsers(d)
d <- dmp$applyFilter(data=d,filter=list(pixel_id="16234",interaction_type="Time"))
d <- dmp$makeSessionData(d)

d$section <- apply(d,1,function(r) g$getTrafficDestination(r,fromColumn = "event_url"))
d1 <- d[grepl("",d$event_url,fixed=T),]
d2 <- dmp$createPaths(data=d1,fromColumn="section",startcol = 3)

################
# SAVE
################

saveRDS(d2,"/data/dmp/paths.rds")

################
# PLOT
################

p<-gvisSankey(
  data = d2,
  from='from',
  to='to',
  weight='n',
  options=list(
    height=900,
    width=1800,
    sankey="{link:{color:{fill:'lightblue'}}}"
  )
)

print(p, file="/var/www/html/webpaths.html")

