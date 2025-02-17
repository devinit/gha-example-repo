fts_save_master <- function(years = 2000:2023, update_years = years, path = "IHA/datasets/"){
  fts_all <- fts_curated_flows(years, update_years = update_years, dataset_path = path)
  for(i in 1:length(years)){
    fwrite(fts_all[year == years[[i]]], paste0(path, "/fts_curated_", years[[i]], ".csv"))
  }
}

fts_read_master <- function(years = years){
  fts_curated_all <- list()
  for(i in 1:length(years)){
    year <- years[i]
    gh_url <- paste0("https://raw.githubusercontent.com/devinit/gha_automation/main/IHA/datasets/fts_curated_master/fts_curated_", year, ".csv")
    fts_curated_all[[i]] <- fread(gh_url, showProgress = F, encoding = "UTF-8")
  }
  fts_curated_all <- rbindlist(fts_curated_all, fill = T, use.names = T)
  #fts_curated_all <- fts_curated_all[, ..columns]
  return(fts_curated_all)
}
