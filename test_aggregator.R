
##############
# SETUP
##############

source("/home/user/scripts/libs.R")
source("/home/user/scripts/system/ftp.R")
source("/home/user/scripts/cep/aggregator.R")

cepAgg <- cepAggregator$new(debug=T)
f <- ftp$new("ecircle_marketing",settings=settings)

##############
# DATA
##############

cepAgg$makeData(
  filepattern = "export-affinity_report-2017",
  location = settings$paths$cepexports
)
cepAgg$makeMessages()
cepAgg$makeUsers()

##############
# SAVE
##############

cepAgg$saveData()
cepAgg$saveMessages()
cepAgg$saveUsers()


