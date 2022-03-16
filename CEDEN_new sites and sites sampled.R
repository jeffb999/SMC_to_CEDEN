##########################################
#
# Purpose of the script is to ddentify SMC sites that are new to CEDEN & have had data collected.
# Script sets up the data to be compatable with the "Station" tab in the CEDEN vocabulary request template.
# The "SummaryTable" tab in the vocab request template (which lists all stations that have data to submit, whether the site is new or not)
#    is not filled out using this script.  Therefore the file name is misleading...this script isn't preparing a list of all sites with data,
#    just the new sites that don't already appear in CEDEN's stationcode lookup list.  Maybe later versions will prepare the SummaryTable.
#
# Note: new sites can be added to the "Station" tab in the vocabulary_request_template
# L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\Templates\vocab_request_template_01082019.xlsx
# http://ceden.org/docs/vocab_request_template_01082019.xlsx
# http://ceden.org/vocabulary_request.shtml
#
# Note: Both the 'Station' tab and the 'SummaryTable' are filled out and submitted with the vocab request template.
#  See L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\ForUpload\vocab_request_SCCWRP_101620_jb2_jlsReview.xlsx
#  for a completed vocab request form.
#
#
# Jeff Brown, March 2022
##########################################

library(tidyverse)


#####---  Get SMC Sites  ---#####

### Import dynamically from database
require('dplyr')
require('RPostgreSQL')
library ('reshape')
# connect to db
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = '192.168.1.17', port = 5432, dbname = 'smc', user = 'smcread', password = '1969$Harbor')

lustations_query = "select * from sde.lu_stations"
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)

lab_chem_sql <- paste0("SELECT stationcode, sampledate, sampletypecode, record_origin FROM sde.unified_chemistry
WHERE LOWER(sampletypecode) NOT LIKE '%dup%'
AND LOWER(sampletypecode) NOT LIKE '%qa%'
AND LOWER(sampletypecode) NOT LIKE '%blank%'
AND (LOWER(sampletypecode) LIKE '%grab%'
OR LOWER(sampletypecode) LIKE '%integrated%'
OR LOWER(sampletypecode) LIKE '%not recorded%'
OR LOWER(sampletypecode) LIKE '%split%'
OR LOWER(sampletypecode) LIKE '%fieldsp%')", sep = "")
# running query, pulling data into environment
lab_chem_df <- tbl(con, sql(lab_chem_sql)) %>%
  as_tibble()

phab_sql <- paste0("SELECT stationcode, sampledate, sampleagencycode, record_origin FROM sde.unified_phab", sep = "")
phab_df <- tbl(con, sql(phab_sql)) %>%
  as_tibble()

bugtaxa_sql <- paste0("SELECT stationcode, sampledate, record_origin FROM sde.unified_taxonomy", sep = "")
bugtaxa_df <- tbl(con, sql(bugtaxa_sql)) %>%
  as_tibble()

algaetaxa_sql <- paste0("SELECT stationcode, sampledate, record_origin FROM sde.unified_algae", sep = "")
algaetaxa_df <- tbl(con, sql(algaetaxa_sql)) %>%
  as_tibble()

csci_sql <- paste0("SELECT stationcode, sampledate, record_origin FROM sde.analysis_csci_core", sep = "")
csci_df <- tbl(con, sql(csci_sql)) %>%
  as_tibble()

# No record_origin in analysis_asci table
asci_sql <- paste0("SELECT stationcode, sampledate, record_origin FROM sde.analysis_asci", sep = "")
asci_df <- tbl(con, sql(asci_sql)) %>%
  as_tibble()

toxicity_sql <- paste0("SELECT stationcode, sampledate, record_origin FROM sde.unified_toxicityresults", sep = "")
toxicity_df <- tbl(con, sql(toxicity_sql)) %>%
  as_tibble()



###

# List of Non-probabilistic sites that should be included, per email from Rafi 9/22/2020
AlsoInclude <- c("402SNPMCR", "902ARO100", "903FRC", "905BCC", "907BCT", "911LAP", "802SJN851",
                 "ME-VR2", "REF-FC", "REF-TCAS", "SJC-74", "TC-DO")

# CSULB sites to remove.  CSULB submits data directly to SWAMP.  SiteDates are removed below, not stationcodes,
#  therefore if a site was sampled by CSULB, it won't automatically get removed, unless CSULB is associated with the sample (SiteDate).
#  New sites are compared against the CEDEN stationcode lookup list later in this script, so *sites* already submitted by CSULB
#  will be flagged before they are added to the Station tab of the vocab request form.
PosCSULB <- grep("CSULB", phab_df$sampleagencycode)    # find the index of records that contain "CSULB" in their sampleagencycode
Dat.CSULB <- phab_df[PosCSULB, ]                       # use the index to retrieve these data
Dat.CSULB <- Dat.CSULB %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(SiteDate = paste(stationcode, sampledate, sep="_")) %>%
  filter(!duplicated(SiteDate))

# CEDEN station lookup list csv file must be updated as necessary (in the time frame of the new SMC data)
# http://ceden.org/CEDEN_Checker/Checker/DisplayCEDENLookUp.php?List=StationLookUp
CEDENSites <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/StationLookUp31420229730.csv', stringsAsFactors=F, strip.white=TRUE)

# SWAMP station lookup
# https://swamp.waterboards.ca.gov/swamp_checker/DisplayLookUp.aspx?List=StationLookUp
SWAMPSites <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/SWAMP_StationLookUp_031522.csv', stringsAsFactors=F, strip.white=TRUE)

# Previous vocab request Station tab
PrevVocabReq <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/vocab_request_SCCWRP_101620_jb2_jlsReview_stationtab.csv', stringsAsFactors=F, strip.white=TRUE)

# Previous vocab request SummaryTable tab
PrevVocabReqSummaryTab <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/vocab_request_SCCWRP_101620_jb2_jlsReview_SummaryTableTab.csv', stringsAsFactors=F, strip.white=TRUE)


rm(lab_chem_sql, phab_sql, lustations_query, tbl_lustations, bugtaxa_sql, algaetaxa_sql, csci_sql, asci_sql, toxicity_sql, PosCSULB)






#####---  Conform & screen for probabilistic sites  ---#####

## Chemistry
labchem2 <- lab_chem_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
# Note: 6 stationcodes are changed to conform to the stationcodes already in CEDEN.  Both the before and after stationcodes appear in lu_stations.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

labchem3 <- labchem2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, sampletypecode, probabilistic, SiteDate)) %>%
  arrange(stationcode)

## PHab
phab_df2 <- phab_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

phab_df3 <- phab_df2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, sampleagencycode, probabilistic, SiteDate)) %>%
  arrange(stationcode)

## Bug taxonomy
bugtaxa_df2 <- bugtaxa_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

bugtaxa_df3 <- bugtaxa_df2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, probabilistic, SiteDate)) %>%
  arrange(stationcode)

## Algae taxonomy
algaetaxa_df2 <- algaetaxa_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

algaetaxa_df3 <- algaetaxa_df2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, probabilistic, SiteDate)) %>%
  arrange(stationcode)

## Toxicity results
toxicity_df2 <- toxicity_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

toxicity_df3 <- toxicity_df2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, probabilistic, SiteDate)) %>%
  arrange(stationcode)

## csci
csci_df2 <- csci_df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!duplicated(SiteDate)) %>%
  arrange(SiteDate)

csci_df3 <- csci_df2 %>%
  filter(!duplicated(stationcode)) %>%
  select(-c(sampledate, probabilistic, SiteDate)) %>%
  arrange(stationcode)

# ## asci.  Omit because no record_origin.  Rely on algae taxonomy.
# asci_df2 <- asci_df %>%
#   inner_join(lustations.1[, c("stationid", "masterid", "probabilistic")], by=c("stationcode"="stationid")) %>%
#   filter(!is.na(probabilistic)) %>%
#   filter(tolower(probabilistic) == "true" | stationcode %in% AlsoInclude) %>%
#   mutate(sampledate = as.Date(sampledate)) %>%
#   mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
#                               ifelse(stationcode == 'SMC00036', '408S00036',
#                                      ifelse(stationcode == 'SMC01384', '404S01384',
#                                             ifelse(stationcode == 'SMC00836', '408S00836',
#                                                    ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
#   mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
#   mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
#   filter(!(SiteDate %in% Dat.CSULB$SiteDate)) %>%                         # Remove CSULB samples
#   filter(!duplicated(SiteDate)) %>%
#   arrange(SiteDate)
# 
# asci_df3 <- asci_df2 %>%
#   filter(!duplicated(stationcode)) %>%
#   select(-c(sampledate, probabilistic, SiteDate)) %>%
#   arrange(stationcode)



#####---  Consolidate  ---#####

NewSites <- rbind(rbind(rbind(rbind(phab_df3, bugtaxa_df3), algaetaxa_df3), toxicity_df3), csci_df3) # omit asci because no record_origin
NewSites <- NewSites %>%
  filter(!duplicated(stationcode)) %>%
  arrange(stationcode) %>%
  left_join(lustations.1[, c("stationid", "stationname", "latitude", "longitude")], by=c("stationcode" = "stationid")) %>%
  mutate(StationName2 = ifelse((stationcode == stationname | is.na(stationname) | stationname == ""), stationcode,
                               paste(stationname," (",stationcode,")",sep=""))
         )


#####---  Compare with existing CEDEN sites  ---#####

NewSites2 <- NewSites %>%
  filter(!(stationcode %in% CEDENSites$StationCode))

# Double check against SWAMP station lookup list
NewSites_inSWAMP <- NewSites2 %>%
  filter(stationcode %in% SWAMPSites$StationCode)
# if site exists in SWAMP but not CEDEN, use the associated data from SWAMP (e.g., StationName, Datum, SWRCBWatTypeCode, CoordinateSource, etc)


#####---  Conform with vocab request template  ---#####

StationTab <- NewSites2 %>%
  mutate(StationSource = "SCCWRP",
         StationAgency = "SCCWRP",
         StationComments = "",
         CoordinateNumber = 1,
         Datum = "WGS1984",
         CoordinateSource = "NR",
         SWRCBWatTypeCode = "R_W",
         StationCode.Character.Count = nchar(stationcode),
         Review.Comments = "",
         StationDescr = "",
         EventType1 = "",
         EventType2 = "",
         EventType3 = "",
         GeometryShape = "",
         DirectionsToStation = "",
         AddDate = "",
         Elevation = "",
         UnitElevation = "",
         StationDetailVerBy = "",
         StationDetailVerDate = "",
         StationDetailComments = "",
         LocalWatershed = "",
         LocalWaterbody = "",
         State = "",
         Counties_2004_COUNTY = "",
         CalWater_2004_RB = "",
         CalWater_2004_CALWNUM = "",
         CalWater_2004_HUNAME = "",
         GageStationID = "",
         UpstreamArea = "",
         HBASA2_1995_NHCODE = "",
         NHD_24k_v2_GNIS_Name = "",
         NHD_24k_v2_ReachCode = "",
         NHD_24k_v2_HUC_12 = "",
         NHD_24k_v2_Hu_12_Name = "",
         NHD_100k_GNIS_Name = "",
         NHD_100k_ReachCode = "",
         NHD_Plus_CatchmentComID = "",
         Ecoregion_1987_Level3 = "",
         IBI_NorthCoast_2005_WithinPolygon = "",
         IBI_SoCal_2005_WithinPolygon = "",
         StationGISVerBy = "",
         StationGISVerDate = "",
         StationGISVerComments = "") %>%
  dplyr::rename(TargetLatitude = latitude,
                TargetLongitude = longitude,
                StationCode = stationcode,
                StationName = StationName2) %>%
  select(-c(stationname, masterid))

# Conform to vocab request column order
PrevVocab.List <- colnames(PrevVocabReq)  # list of variables in correct order as they appear in database table
StationTab <- StationTab[PrevVocab.List]


### Check against previous vocab request Station tab submissions
#  CEDEN's stationcode lookup list should have been updated with previous requests from SCCWRP (if the previous request was a year ago),
#    but check the dataframe StationTab against the old list in PrevVocabReq just in case
AlreadySubmitted <- StationTab %>%
  filter(StationCode %in% PrevVocabReq$StationCode)


#write.csv(StationTab, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/vocab_request_StationTab_031522.csv')



#####---   SummaryTable  ---#####

# Compare complete table (NewSites) with previous request to see which are remaining
# In subsequent years when previous data are uploaded, can just refer to most recently collected data (at least that's the intent)

NotInPrevSummary <- NewSites %>%
  filter(!(stationcode %in% PrevVocabReqSummaryTab$StationCode))
#write.csv(NotInPrevSummary, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/vocab_request_ForSummaryTable_031622.csv')


InPrevSummary <- NewSites %>%
  filter(stationcode %in% PrevVocabReqSummaryTab$StationCode)














