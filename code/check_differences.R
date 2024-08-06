#### Setup ####
list.of.packages <- c("rstudioapi", "data.table", "scales")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd <- dirname(getActiveDocumentContext()$path) 
setwd(wd)
setwd("../")

fts_sum = 0
fts_mike_sum = 0
unique_fts_ids = c()
unique_fts_mike_ids = c()

years = c(2018:2023)
for(year in years){
  message("\n")
  message(year)
  fts = fread(paste0("output/fts_", year, ".csv"))
  fts_mike = fread(paste0("output/fts_", year, "_mike.csv"))
  message("IDs found in latest run but not in older:")
  unique_fts_ids = unique(c(unique_fts_ids, fts$id))
  unique_fts_mike_ids = unique(c(unique_fts_mike_ids, fts_mike$id))
  message(paste(setdiff(fts$id, fts_mike$id), collapse=", "))
  message("IDs found in older run but not in latest:")
  message(paste(setdiff(fts_mike$id, fts$id), collapse=", "))
  message("Financial difference between old and new run:")
  fts_mike_sum = fts_mike_sum + sum(fts_mike$amountUSD, na.rm=T)
  fts_sum = fts_sum + sum(fts$amountUSD, na.rm=T)
  message(dollar(sum(fts_mike$amountUSD, na.rm=T) - sum(fts$amountUSD, na.rm=T)))
}

dollar(fts_sum)
dollar(fts_mike_sum)

dollar(fts_sum - fts_mike_sum)

setdiff(unique_fts_ids, unique_fts_mike_ids)
setdiff(unique_fts_mike_ids, unique_fts_ids)
