fts_save_master <- function(years = 2000:2023, update_years = years, path = "IHA/datasets/"){
  fts_all <- fts_curated_flows(years, update_years = update_years)
  for(i in 1:length(years)){
    fwrite(fts_all[year == years[[i]]], paste0(path, "fts_curated_", years[[i]], ".csv"))
  }
}