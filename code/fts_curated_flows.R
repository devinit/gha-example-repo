###Function to download and curate FTS flows from the FTS API
#Queries are done by year, meaning the boundary column reflects this. We prioritise flows with incoming boundary classification where a flow is duplicated.

##Data which is removed
#Outgoing flows are removed.
#Duplicate flows which occur in multiple years are removed - the first occurrence is retained and then split equally between destination usage years.
#Pledges are removed.

##Transformations
#Negative dummy flows are added to remove the effect of internal plan flows, ensuring the total inputs and outputs are correct when aggregated.
#Multi-year flows (destination) are split equally between destination years.
#Deflation is done according to source organisation and destination year. Non-government source organisations use the OECD DAC deflator.
#Flows with multiple destinations are rendered 'Multi-recipient'.
#European Commission Institutions are coded manually as donor country "European Commission" and use the OECD EU Institutions deflator.

fts_curated_flows <- function(years = NULL, update_years = NA, dataset_path = "Test", base_year = 2022, weo_ver = NULL, dummy_intra_country_flows = T){
  suppressPackageStartupMessages(lapply(c("data.table", "jsonlite","rstudioapi"), require, character.only=T))
  
  if(!dir.exists(dataset_path)){
    dir.create(dataset_path)
  }
  fts_files <- list.files(path = dataset_path, pattern = "fts_")
  fts_list <- list()
  for(i in 1:length(years)){
    run <- T
    if(!(paste0("fts_", years[i], ".csv") %in% fts_files) | years[i] %in% update_years){
      message(paste0("Downloading ", years[i]))
      while(run){
        tryCatch({
          fts <- fts_get_flows(year = years[i])
          run <- F
        },
        error = function(e) e
        )
        break
      }
      reportDetails <- rbindlist(lapply(fts$reportDetails, function(x) lapply(x, function(y) paste0(y, collapse = "; "))))
      names(reportDetails) <- paste0("reportDetails_", names(reportDetails))
      fts <- cbind(fts, reportDetails)
      fts[, reportDetails := NULL]
      fts[is.null(fts) | fts == "NULL"] <- NA
      fwrite(fts, paste0(dataset_path, "/fts_", years[i], ".csv"))
    }
    message(paste0("Reading ", years[i]))
    fts_list[[i]] <- fread(paste0(dataset_path, "/fts_", years[i], ".csv"), encoding = "UTF-8")
  }
  
  fts <- rbindlist(fts_list, use.names = T, fill = T)
  rm(fts_list)
  
  #Begin transformation
  message("Curating data...")
  
  #Retain column order
  col_order <- names(fts)
  
  #Remove flows which are outgoing on boundary
  fts <- fts[boundary != "outgoing"]
  #write.csv(fts, "fts_1.csv", row.names = FALSE)
  
  #Remove duplicates which have a shared boundary, and preserve 'incoming' over 'internal' on boundary type
  shared <- rbind(fts[onBoundary == "shared" & boundary == "incoming", .SD[1], by = id], fts[onBoundary == "shared" & boundary == "internal" & !(id %in% fts[onBoundary == "shared" & boundary == "incoming", .SD[1], by = id]$id), .SD[1], by = id])
  fts <- rbind(fts[onBoundary != "shared"], shared)
  #write.csv(fts, "fts_2.csv", row.names = FALSE)
  
  #Split rows into individual years by destination usage year where multiple are recorded 
  fts[, year := destinationObjects_UsageYear.name]
  fts[, multiyear := grepl(";", destinationObjects_UsageYear.name)]
  fts <- fts_split_rows(fts, value.cols = "amountUSD", split.col = "year", split.pattern = "; ", remove.unsplit = T)
  #write.csv(fts, "fts_3.csv", row.names = FALSE)
  
  #Set multi-country flows to 'multi-destination_org_country' in destination_org_country column
  isos <- fread("https://raw.githubusercontent.com/devinit/gha_automation/main/reference_datasets/isos.csv", encoding = "UTF-8", showProgress = F)
  fts <- merge(fts, isos[, .(countryname_fts, destination_org_iso3 = iso3)], by.x = "destinationObjects_Location.name", by.y = "countryname_fts", all.x = T, sort = F)
  fts[, destination_org_country := destinationObjects_Location.name]
  fts[grepl(";", destination_org_country), `:=` (destination_org_country = "Multi-destination_org_country", destination_org_iso3 = "MULTI")]
  #write.csv(fts, "fts_4.csv", row.names = FALSE)
  
  #Deflate by source location and destination year
  fts_orgs <- data.table(fromJSON("https://api.hpc.tools/v1/public/organization")$data)
  fts_locs <- data.table(fromJSON("https://api.hpc.tools/v1/public/location")$data)
  fts_orgs[, `:=` (source_org_type = ifelse(is.null(categories[[1]]$name), NA, categories[[1]]$name), source_org_country = ifelse(is.null(locations[[1]]$name), NA, locations[[1]]$name), source_org_country_id = ifelse(is.null(locations[[1]]$id), NA, locations[[1]]$id)), by = id]
  fts_orgs <- merge(fts_orgs, fts_locs[, .(id, iso3)], by.x = "source_org_country_id", by.y = "id", all.x = T, sort = F)
  fts_orgs <- fts_orgs[, .(sourceObjects_Organization.id = as.character(id), source_org_country, source_org_iso3 = iso3, FTS_source_orgtype = source_org_type)]
  #write.csv(fts_orgs, "fts_orgs.csv", row.names = FALSE)
  
  #Merge DI coded org types
  source_org_dicode <- fread("C:/Users/mike.pearson/Development Initiatives Poverty Research Limited/Humanitarian Team - Documents/GHA Programme/OA1 - Crisis trends, needs, resource flows/GHA Report 2024/Datasets/FTS/dac_and_eu.csv", encoding = "UTF-8", showProgress = F)
  source_org_dicode <- merge(fts_orgs, source_org_dicode[, .(sourceObjects_Organization.id = as.character(sourceObjects_Organization.id), source_orgtype)], by = "sourceObjects_Organization.id", all.x = T)
  
  #Merge source orgs
  fts[, sourceObjects_Organization.id := as.character(sourceObjects_Organization.id)]
  fts <- merge(fts, source_org_dicode, by = "sourceObjects_Organization.id", all.x = T, sort = F)
  #write.csv(fts, "fts_5.csv", row.names = FALSE)
  
  fts[, source_org_iso3 := fifelse(!is.na(source_orgtype) & source_orgtype == "Multilateral: EC", "EUI",
                                   fifelse(is.na(source_org_iso3), NA, source_org_iso3))]
  fts[is.na(source_org_iso3), source_org_iso3 := "DAC"]
  fts[source_org_iso3 == "EUI", source_org_country := "EC"]
  fts[, FTS_source_orgtype := NULL]
  #write.csv(fts, "fts_6.csv", row.names = FALSE)
  
  
  fts[is.na(source_orgtype), source_orgtype := "ERROR"]
  fts[, source_orgtype := fifelse(source_orgtype == "Multilateral: EC", "Multilateral: EC",
                          fifelse(source_orgtype == "Multilateral: Development Bank", "Multilateral: Development Bank",
                          fifelse(sourceObjects_Organization.organizationTypes == "Governments" & source_orgtype == "DAC governments", "Governments: DAC",
                          fifelse(sourceObjects_Organization.organizationTypes == "Governments", "Governments: Non-DAC", 
                          fifelse(sourceObjects_Organization.organizationTypes == "Multilateral Organizations" & sourceObjects_Organization.organizationSubTypes == "UN Agencies", "Multilateral: UN",
                          fifelse(sourceObjects_Organization.organizationTypes == "Multilateral Organizations", "Multilateral: Other",
                          fifelse(sourceObjects_Organization.organizationTypes == "NGOs" & sourceObjects_Organization.organizationSubTypes == "International NGOs", "NGOs: International",
                          fifelse(sourceObjects_Organization.organizationTypes == "NGOs" & sourceObjects_Organization.organizationSubTypes == "National NGOs/CSOs", "NGOs: National",
                          fifelse(sourceObjects_Organization.organizationTypes == "NGOs" & sourceObjects_Organization.organizationSubTypes == "Local NGOs/CSOs", "NGOs: Local",
                          fifelse(sourceObjects_Organization.organizationTypes == "NGOs" & sourceObjects_Organization.organizationSubTypes == "Internationally Affiliated Organizations", "NGOs: Internationally Affiliated",
                          fifelse(sourceObjects_Organization.organizationTypes == "Other", "Other",
                          fifelse(sourceObjects_Organization.organizationTypes == "Pooled Funds" & sourceObjects_Organization.name == "Central Emergency Response Fund", "Pooled Funds: CERF",
                          fifelse(sourceObjects_Organization.organizationTypes == "Pooled Funds" & sourceObjects_Organization.organizationSubTypes == "Global UN Pooled Funds", "Pooled Funds: Other UN",
                          fifelse(sourceObjects_Organization.organizationTypes == "Pooled Funds" & sourceObjects_Organization.organizationSubTypes == "Regional UN Pooled Funds", "Pooled Funds: Regional",
                          fifelse(sourceObjects_Organization.organizationTypes == "Pooled Funds" & sourceObjects_Organization.organizationSubTypes == "Country-based UN Pooled Funds", "Pooled Funds: Country",
                          fifelse(sourceObjects_Organization.organizationTypes == "Private Organizations" & grepl("UNICEF", sourceObjects_Organization.name), "Multilateral: UN",
                          fifelse(sourceObjects_Organization.organizationTypes == "Private Organizations", "Private Organisations: Foundations and Other",
                          fifelse(sourceObjects_Organization.organizationTypes == "Red Cross/Red Crescent Organizations" & sourceObjects_Organization.organizationSubTypes == "Red Cross/Red Crescent National Societies", "Red Cross: National",
                          fifelse(sourceObjects_Organization.organizationTypes == "Red Cross/Red Crescent Organizations" & sourceObjects_Organization.organizationSubTypes == "International Red Cross/Red Crescent Movement", "Red Cross: International", 
                          fifelse(sourceObjects_Organization.organizationTypes == "", "Other: Unknown",
                          fifelse(grepl(";", sourceObjects_Organization.organizationTypes), "Other: Multiple Organisations",
                                  source_orgtype)))))))))))))))))))))]
  #write.csv(fts, "fts_7.csv", row.names = FALSE)
  
  fts[, destination_orgtype := fifelse(destinationObjects_Organization.organizationTypes == "Governments" & destinationObjects_Organization.organizationSubTypes == "National Governments", "Governments: National",
                               fifelse(destinationObjects_Organization.organizationTypes == "Governments" & destinationObjects_Organization.organizationSubTypes == "Local Governments", "Governments: Local",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "UN Agencies", "Multilateral: UN",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "International NGOs", "NGOs: International",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "National NGOs/CSOs", "NGOs: National",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Internationally Affiliated Organizations", "NGOs: Internationally Affiliated",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Local NGOs/CSOs", "NGOs: Local",
                               fifelse(destinationObjects_Organization.organizationTypes == "Other", "Other",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Other Multilateral Organizations", "Multilateral: Other",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Global UN Pooled Funds", "Pooled Funds: Other UN",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Country-based UN Pooled Funds", "Pooled Funds: Country",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Regional UN Pooled Funds", "Pooled Funds: Regional",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "International Private Organizations", "Private Organisations: International",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Local/National Private Organizations", "Private Organisations: Local/National",                                               
                               fifelse(destinationObjects_Organization.organizationSubTypes == "International Red Cross/Red Crescent Movement", "Red Cross: International",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "Red Cross/Red Crescent National Societies", "Red Cross: National",
                               fifelse(destinationObjects_Organization.organizationSubTypes == "", "Other: Unknown",                                              
                               fifelse(grepl(";", destinationObjects_Organization.organizationTypes), "Other: Multiple Organisations", "ERROR"))))))))))))))))))]
  #write.csv(fts, "fts_8.csv", row.names = FALSE)

  #Identify multi-cluster flows
  fts[grepl(";", destinationObjects_GlobalCluster.name), destinationObjects_GlobalCluster.name := "Multiple clusters specified"]
  fts[grepl(";", sourceObjects_GlobalCluster.name), sourceObjects_GlobalCluster.name := "Multiple clusters specified"]
  
  #Identify unspecified cluster flows
  fts[is.na(destinationObjects_GlobalCluster.name) | destinationObjects_GlobalCluster.name == "", destinationObjects_GlobalCluster.name := "Unspecified"]
  fts[is.na(sourceObjects_GlobalCluster.name) | sourceObjects_GlobalCluster.name == "", sourceObjects_GlobalCluster.name := "Unspecified"]
  
  #Domestic response
  fts[, domestic_response := F]
  fts[grepl("Government", sourceObjects_Organization.organizationTypes) & source_org_iso3 == destination_org_iso3, domestic_response := T]
  
  #New to country
  fts[, new_to_country := T]
  fts[sourceObjects_Location.id == destinationObjects_Location.id | destinationObjects_Location.id == "", new_to_country := F]
  
  #New to plan
  fts[, new_to_plan := T]
  if("sourceObjects_Plan.id" %in% names(fts)){
    fts[sourceObjects_Plan.id == destinationObjects_Plan.id | destinationObjects_Plan.id == "", new_to_plan := F]
  }
  
  #New to sector
  fts[, new_to_sector := T]
  fts[sourceObjects_GlobalCluster.id == destinationObjects_GlobalCluster.id | destinationObjects_GlobalCluster.id == "", new_to_sector := F]
  
  #COVID
  fts[, COVID := F]
  if ("destinationObjects_Cluster.name" %in% names(fts)) {
  fts[grepl("COVID", paste0(destinationObjects_GlobalCluster.name, destinationObjects_Cluster.name, destinationObjects_Plan.name, destinationObjects_Emergency.name), ignore.case = T), COVID := T]
  } else {
    fts[grepl("COVID", paste0(destinationObjects_GlobalCluster.name, destinationObjects_Plan.name, destinationObjects_Emergency.name), ignore.case = T), COVID := T]
    }
  
  #Remove partial multi-year flows outside of requested range and pledges 
  fts <- fts[
    year %in% years 
    & status %in% c("paid", "commitment")
  ]
  
  #write.csv(fts, "fts_9.csv", row.names = FALSE)
  
  
  #Add deflators
  deflators <- fread("C:/Users/mike.pearson/Development Initiatives Poverty Research Limited/Humanitarian Team - Documents/GHA Programme/OA1 - Crisis trends, needs, resource flows/GHA Report 2024/Datasets/Deflators/FINAL Merged Dataset/deflators_final.csv", encoding = "UTF-8", showProgress = F)
  deflators <- deflators[, .(source_org_iso3 = ISO, year = as.character(year), deflator = gdp_defl)]
  
  fts <- merge(fts, deflators, by = c("source_org_iso3", "year"), all.x = T, sort = F)
  fts[is.na(deflator)]$deflator <- merge(fts[is.na(deflator)][, -"deflator"], deflators[source_org_iso3 == "DAC"], by = "year", all.x = T, sort = F)$deflator
  fts[, `:=` (amountUSD_defl = amountUSD/deflator, amountUSD_defl_millions = (amountUSD/deflator)/1000000)]
  
  #write.csv(fts, "fts_10.csv", row.names = FALSE)
  
  #Add dummy reverse flows to cancel-out intra-country flows
  fts[, newMoney_dest := newMoney]
  fts[, dummy := F]
  if(dummy_intra_country_flows){
    fts[sourceObjects_Location.id == destinationObjects_Location.id, newMoney_dest := TRUE]
    fts_intracountry <- fts[sourceObjects_Location.id == destinationObjects_Location.id]
    source_cols <- grep("source", names(fts_intracountry))
    destination_cols <- grep("destination", names(fts_intracountry))
    names(fts_intracountry)[source_cols] <- gsub("source", "destination", names(fts_intracountry)[source_cols])
    names(fts_intracountry)[destination_cols] <- gsub("destination", "source", names(fts_intracountry)[destination_cols])
    fts_intracountry[, `:=` (amountUSD = -amountUSD, amountUSD_defl = -amountUSD_defl, amountUSD_defl_millions = -amountUSD_defl_millions, dummy = T)]
    
    fts <- rbind(fts, fts_intracountry, fill = T)
  }
  
  #write.csv(fts, "fts_11.csv", row.names = FALSE)
  
  #Elevate cash to the Global Cluster where it is buried in the field clusteer and no other GC is specified, or is multi-sector
  if ("destinationObjects_Cluster.name" %in% names(fts)) {
  fts[grepl("cash", destinationObjects_Cluster.name, ignore.case = TRUE) &
        !grepl(";", destinationObjects_Cluster.name) &
        (destinationObjects_GlobalCluster.name %in% c("Unspecified", "Multi-sector")),
      destinationObjects_GlobalCluster.name := "Cash"]}
  
  #write.csv(fts, "fts_12.csv", row.names = FALSE)
  
  #Reorder columns nicely
  col_order <- union(col_order, names(fts)[order(names(fts))])
  fts <- fts[, col_order, with = F]
  
  #write.csv(fts, "fts_13.csv", row.names = FALSE)
  
}

    
    
  
  
  
  