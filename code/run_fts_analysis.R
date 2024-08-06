#### Setup ####
list.of.packages <- c("rstudioapi", "data.table")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd <- dirname(getActiveDocumentContext()$path) 
setwd(wd)
setwd("../")

# Source Dan's fts_get_flows script
source("https://raw.githubusercontent.com/devinit/di_script_repo/main/gha/FTS/fts_get_flows.R")

# Source Dan's fts_split_rows script
source("https://raw.githubusercontent.com/devinit/di_script_repo/main/gha/FTS/fts_split_rows.R")

# Source Mike's changes to fts_curated
source("./code/fts_curated_flows.R")

# Source Dan's fts_curated_master for fts_save_master function
source("https://raw.githubusercontent.com/devinit/gha_automation/main/IHA/fts_curated_master.R")

# Run
fts_save_master(years=c(2000:2023), path="output")