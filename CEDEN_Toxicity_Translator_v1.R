##########################
#
# Purpose is to produce toxicity Result, Summary & Batch files ready for upload to CEDEN
#      This script was written to consolidate data from 2009-2019.
#      Subsequent years may not need as extensive data manipulation.
#
# What tables (type of data) are needed for upload?
#      Tox SummaryResults (unified_toxicitysummary)
#      Tox Results (unified_toxicityresults)
#      Tox Batch   (unified_toxicitybatch)
# How are the data consolidated?
#      e.g., batch data used with summary?
# What data are reported separately (in different tabs of Excel file?)
#      3 tabs in CEDEN template: ToxSummaryResults, ToxReplicateResults, ToxBatch
#
# Percent Effect
# Need to calculate a "Percent Effect" for ToxSummaryResults tab in CEDEN.  This is not done by reporting labs.
#     In order to calculate effect, need to identify the control sample (mean in summary table or replicates in Result table).
#     This varies by tox lab & maybe even when the testing was conducted.
# Methods to get Ctrl.  11 different ways had to be used to find control values.
# 1. stationcode = LABQA, sampletypecode = CNEG, matrix = blankwater
# 2. stationcode = LABQA, sampletypecode = CNEG, matrix = referencetoxicant (this is used to find Ctrl for *some* ref tox samples)
# 3. stationcode = LABQA, sampletypecode = CNEG, matrix = blankmatrix (for at least 1 sediment test)
# 4. stationcode = LABQA, sampletypecode = CNEG, matrix = sediment
# 5. sampletypecode = CNEG, matrix=blankwater, stationcode=stationcode.
# 6. stationcode=stationcode, sampletypecode = CNEG, matrix=labwater.  But stationcode=LABQA for ref tox (e.g., toxbatch = 0906-S077/078)
#    Nautilus used a separate control for each sample in a batch, and called it by the stationcode designation.
# 7. sampletypecode = CNDL, matrix = referencetoxicant, concentration = 0 (e.g., toxbatch = 09CerioEMD1)
# 8. stationcode = LABQA, sampletypecode = CNEG, matrix=labwater
# 9. stationcode = LABQA, sampletypecode = CNEG, matrix=samplewater
# 10. For those stragglers that the first 8 methods of finding a control sample didn't work, need to calculate a mean Ctrl
#    from lab replicates in Results table. sampletypecode = CNEG, matrix = labwater  (e.g., toxbatch = 0906-S071)
# 11. Control: sampletypecode = CNDL, matrix = referencetoxicant, concentration = 0 or -88 (Same as Method 7, but now
#   extend the use of the reference control to matrix=samplewater)
# Further notes on Percent Effect:
# A. 43 samples did not have an associated Ctrl in either the Summary or Results table.  These were assigned -88.
#     Will people think this was an enhanced response where mean value was > mean Ctrl value?
# B. For samples with mean value > mean Ctrl value, Percent Effect were assigned 0 (rather than a negative value).
#
# Steps
#   Create list of toxbatches that need to be uploaded ("Good" tox batches)
#   Clean Data. Results, Summary, Batch
#   Conform entries to what is expected in CEDEN
#   Identify or Calculate Percent Effect
#   Conform Variables to CEDEN Templates (name and order)
#   Locations tab prep
#   Get coordinates for sites needing to be added to CEDEN Station tab
#
#
# Use data from this script to populate CEDEN taxonomy data template.  The 2019 version can be found at:
# L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\Templates\ceden_toxicity_template_01082019_blank.xls
#
#
# JBrown November, December 2020, June, July 2021
#########################



library(tidyverse)

#####---  GET DATA  ---#####
require('dplyr')
#require('dbplyr')
require('RPostgreSQL')
library ('reshape')
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host='192.168.1.17', port=5432, dbname='smc', user='smcread', password='1969$Harbor')

lustations_query = "select * from sde.lu_stations"              # Needed for probabilistic designation
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)

ToxSum_query = "select * from sde.unified_toxicitysummary"      # Tox Summary
tbl_ToxSum   = tbl(con, sql(ToxSum_query))
ToxSum.1     = as.data.frame(tbl_ToxSum)

ToxRes_query = "select * from sde.unified_toxicityresults"      # Tox Results
tbl_ToxRes   = tbl(con, sql(ToxRes_query))
ToxRes.1     = as.data.frame(tbl_ToxRes)

ToxBatch_query = "select * from sde.unified_toxicitybatch"      # Tox Batch
tbl_ToxBatch   = tbl(con, sql(ToxBatch_query))
ToxBatch.1     = as.data.frame(tbl_ToxBatch)

## Static files (will need to be revised each year)
CSULB <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/CSULB StationCodes_from PHab_092220.csv', stringsAsFactors=F, strip.white=TRUE)
#CEDEN.Chem <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/Chem N P_Program SCSMC SMCRWM_091820.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Tox <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/CEDEN Toxicity_State_downloaded 070621_NoDups.csv', stringsAsFactors=F, strip.white=TRUE)
# https://ceden.waterboards.ca.gov/AdvancedQueryTool (Here's the link to download toxicity data in 2021.  Who knows what it'll be when you read this.)
#
# #CEDEN Look Up Lists.  Read in tables as needed.
# CEDEN.Agency    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/AgencyLookUp928202095222.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Anlyt     <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/AnalyteLookUp9222020105959.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Fraction  <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/FractionLookUp924202015397.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.LabSubmit  <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/LabSubmissionLookUp115202075519.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Method    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/MethodLookUp9242020152937.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Matrix    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/MatrixLookUp9242020152648.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Orgnmsm   <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/OrganismLookUp1142020141528.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.ResQual   <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/ResQualLookUp9242020165119.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.SmplTyp   <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/SampleTypeLookUp9242020151423.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.SigEff    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/SigEffectLookUp1152020161643.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.TestDur   <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/ToxTestDurLookUp1142020142731.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.TimePt    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/TimePointLookUp1142020142546.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.ToxQA     <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/ToxResultQALookUp1142020142337.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.ToxTrtmnt <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/ToxTreatmentLookUp1142020142822.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.QACode    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/QALookUp9242020165539.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Unit      <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/UnitLookUp9242020154139.csv', stringsAsFactors=F, strip.white=TRUE)
# CEDEN.Variable  <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/VariableCodesLookUp1152020155522.csv', stringsAsFactors=F, strip.white=TRUE)




#####---  Create list of toxbatches that need to be uploaded ("Good" tox batches)  ---#####
## Use this list for Results, Summary, and Batch tables. We want to retain stationcode=LABQA, but
#  because LABQA isn't in lu_stations, we can't rely on SiteDate. Therefore create list of Good ToxBatches:
# 1. Retain record_origin = "SMC"
# 2. Retain probabilistic + handful of others
# 3. Remove CSULB stationcodes (based on PHab)
# 4. Remove toxbatches already in CEDEN

# List of Non-probabilistic sites that should be included, per email from Rafi 9/22/2020
AlsoInclude <- c("402SNPMCR", "902ARO100", "903FRC", "905BCC", "907BCT", "911LAP", "802SJN851",
                 "ME-VR2", "REF-FC", "REF-TCAS", "SJC-74", "TC-DO")

# List of CSULB samples (need to revise each year). Remove all, per email from Rafi 9/22/2020.
CSULB <- CSULB %>%
  mutate(sampledate = as.Date(as.character(sampledate), "%m/%d/%Y")) %>%
  left_join(lustations.1[, c("masterid", "stationid")], by=c("stationcode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, sampledate, sep="_"))

# List of ToxBatches already in CEDEN
CEDEN.Tox.2 <- CEDEN.Tox %>%
  mutate(SampleDate = as.Date(as.character(SampleDate), "%m/%d/%Y")) %>% # No reason to do this, unless SiteDate is created.
  filter(!duplicated(ToxBatch)) %>%
  select(StationCode, SampleDate, ToxBatch, MatrixName, MethodName, ToxTestDurCode, OrganismName, Analyte, SampleAgency) # ToxBatch is the important variable

MoreCEDEN <- c("NAUT_1307-S042_W_TOX", "NAUT_1407-S128_W_TOX") # ToxBatches in CEDEN identified through CEDEN data checker.  Update as needed.

# List of Good ToxBatches
GoodToxBatch <- ToxRes.1 %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic")],
            by = c("stationcode" = "stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(probabilistic == "true" | stationcode %in% AlsoInclude) %>%  # Retain probabilistic sites + bonus sites
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%     # Create SiteDate to merge with CSULB samples
  filter(!(SiteDate %in% CSULB$SiteDate)) %>%                     # Remove CSULB samples
  filter(!(toxbatch %in% CEDEN.Tox.2$ToxBatch)) %>%               # Remove toxbatches already in CEDEN
  filter(!(toxbatch %in% MoreCEDEN))                              # Remove toxbatches in CEDEN identified through CEDEN data checker

GoodToxBatch.2 <- GoodToxBatch %>%
  select(toxbatch, latitude, longitude, probabilistic) %>%
  filter(!duplicated(toxbatch))





#####---  Clean the Data. Results  ---#####

# Retain toxbatches that need to be uploaded
ToxRes.2 <- ToxRes.1 %>%
  inner_join(GoodToxBatch.2, by=("toxbatch"="toxbatch")) %>%
  mutate(result = as.numeric(result)) %>%       # Results are currently character
  mutate(sampledate = as.Date(sampledate)) %>%  # sampledate is different format
  select(-c(created_user, created_date, last_edited_user, last_edited_date, lastchangedate, login_agency,
            login_email, login_owner, login_year, login_project, ceden_publish, globalid, submissionid))

### BEGIN Duplicated Record Purge
### Check for duplicated records & renumber replicate if needed (Change LabReplicate number or remove)

# First correct labreplicate or fieldreplicate = NA
ToxRes.2 <- ToxRes.2 %>%
  mutate(fieldreplicate = ifelse(is.na(fieldreplicate), 1, fieldreplicate)) %>%
  mutate(labreplicate = ifelse(is.na(labreplicate),   1, labreplicate))

# Create variable for unique analysis & another with unique analysis + result.  (Use "originalid" instead?)
ToxRes.2$Unq.Current <- paste(
  ToxRes.2$stationcode,       # LABQA is ok, but need to make entries for many other variables = "Not Applicable". See example template.
  ToxRes.2$sampledate,
  ToxRes.2$sampletypecode,
  ToxRes.2$matrixname,
  ToxRes.2$methodname,
  ToxRes.2$toxbatch,          # Needed to differentiate 15C & 23C experiments
  ToxRes.2$organismname,
  ToxRes.2$testduration,
  ToxRes.2$timepoint,
  ToxRes.2$fractionname,
  ToxRes.2$analytename,
  ToxRes.2$unitanalytename,
  ToxRes.2$dilution,          # 50 & 100 used for same toxbatch
  ToxRes.2$concentration,
  ToxRes.2$fieldreplicate,
  ToxRes.2$labreplicate,
  sep = "_"
)
ToxRes.2$Unq.Current.Result <- paste(
  ToxRes.2$stationcode,
  ToxRes.2$sampledate,
  ToxRes.2$sampletypecode,
  ToxRes.2$matrixname,
  ToxRes.2$methodname,
  ToxRes.2$toxbatch,
  ToxRes.2$organismname,
  ToxRes.2$testduration,
  ToxRes.2$timepoint,
  ToxRes.2$fractionname,
  ToxRes.2$analytename,
  ToxRes.2$unitanalytename,
  ToxRes.2$dilution,
  ToxRes.2$concentration,
  ToxRes.2$fieldreplicate,
  ToxRes.2$labreplicate,
  ToxRes.2$result,
  sep = "_"
)
ToxRes.2 <- ToxRes.2[order(ToxRes.2$Unq.Current.Result), ]

# Step 1: Remove duplicated Unq.Current.Result
#Same.A <- ToxRes.2[duplicated(ToxRes.2$Unq.Current.Result), ]
ToxRes.2 <- ToxRes.2[!duplicated(ToxRes.2$Unq.Current.Result), ]

# Step 2: Revise labreplicate for records with same Unq.Current, but different Unq.Current.Result
#Same.A.Different.Result <- ToxRes.2[duplicated(ToxRes.2$Unq.Current), ]

ToxRes.2$One <- 1
ToxRes.2 <- ToxRes.2 %>%
  group_by(Unq.Current) %>%
  mutate(ExtraReps = cumsum(One)) %>%
  ungroup()
ToxRes.2$One <- NULL


ToxRes.2$Unq.Previous <- lag(ToxRes.2$Unq.Current, 1)
ToxRes.2$labreplicate <- ifelse(is.na(ToxRes.2$Unq.Previous),  ToxRes.2$labreplicate,
                                ifelse(ToxRes.2$Unq.Current == ToxRes.2$Unq.Previous,
                                ToxRes.2$ExtraReps, ToxRes.2$labreplicate))

ToxRes.2$Unq.Current         <- NULL
ToxRes.2$Unq.Current.Result  <- NULL
ToxRes.2$Unq.Previous        <- NULL
ToxRes.2$ExtraReps           <- NULL
#### END Duplicate Record Purge






#####---  Clean the Data. Summary  ---#####
# Retain toxbatches that need to be uploaded
ToxSum.2 <- ToxSum.1 %>%
  filter(toxbatch %in% ToxRes.2$toxbatch) %>%  # Retain good toxbatch
  select(-c(globalid, created_date, created_user, lastchangedate, last_edited_user, last_edited_date, login_agency,
            login_email, login_owner, login_year, login_project, submissionid, origin_lastupdatedate))


#####---  Clean the Data.  ToxBatch  ---#####
# Retain toxbatches that need to be uploaded
ToxBatch.2 <- ToxBatch.1 %>%
  filter(toxbatch %in% ToxRes.2$toxbatch) %>%  # Retain good toxbatch
  mutate(startdate = as.Date(startdate)) %>%
  select(-c(globalid, created_date, created_user, lastchangedate, last_edited_user, last_edited_date, login_agency,
            login_email, login_owner, login_year, login_project, submissionid, origin_lastupdatedate))



#####---  Conform entries to what is expected in CEDEN  ---#####

### Analyte (ToxRes)
# AnalyteToRevise <- ToxRes.2[!(ToxRes.2$analytename %in% CEDEN.Anlyt$AnalyteName), ]
# AnalyteToRevise <- AnalyteToRevise[!duplicated(AnalyteToRevise$analytename), ]
# AnalyteToRevise <- AnalyteToRevise[order(AnalyteToRevise$analytename), ]
#
ToxRes.2$analytename <- ifelse(ToxRes.2$analytename == "young/female" | ToxRes.2$analytename == "Young/Female",
                               "Young/female", ToxRes.2$analytename)
ToxRes.2$analytename <- ifelse(ToxRes.2$analytename == "REPRODUCTION", "Reproduction", ToxRes.2$analytename)
ToxRes.2$analytename <- ifelse(ToxRes.2$analytename == "SURVIVAL", "Survival", ToxRes.2$analytename)


### Analyte (ToxSum)
# AnalyteToRevise.Sum <- ToxSum.2[!(ToxSum.2$analytename %in% CEDEN.Anlyt$AnalyteName), ]
# AnalyteToRevise.Sum <- AnalyteToRevise.Sum[!duplicated(AnalyteToRevise.Sum$analytename), ]
# AnalyteToRevise.Sum <- AnalyteToRevise.Sum[order(AnalyteToRevise.Sum$analytename), ]
#
ToxSum.2$analytename <- ifelse(ToxSum.2$analytename == "young/female" | ToxSum.2$analytename == "Young/Female",
                               "Young/female", ToxSum.2$analytename)


### Dilution (ToxRes)
ToxRes.2 <- ToxRes.2 %>%
  mutate(dilution = ifelse(dilution == -99, 0, dilution))

### Dilution (ToxSum)
ToxSum.2 <- ToxSum.2 %>%
  mutate(dilution = ifelse(dilution == -99, 0, dilution))


### Fraction (ToxRes)
# FractionToRevise <- ToxRes.2[!(ToxRes.2$fractionname %in% CEDEN.Fraction$FractionName), ]
# FractionToRevise <- FractionToRevise[!duplicated(FractionToRevise$fractionname), ]
# FractionToRevise <- FractionToRevise[order(FractionToRevise$fractionname), ]


### Fraction (ToxSum)
# FractionToRevise.Sum <- ToxSum.2[!(ToxSum.2$fractionname %in% CEDEN.Fraction$FractionName), ]
# FractionToRevise.Sum <- FractionToRevise.Sum[!duplicated(FractionToRevise.Sum$fractionname), ]
# FractionToRevise.Sum <- FractionToRevise.Sum[order(FractionToRevise.Sum$fractionname), ]


### LabAgency (ToxRes)
# LabAgencyToRevise <- ToxRes.2[!(ToxRes.2$labagencycode %in% CEDEN.Agency$AgencyCode), ]
# LabAgencyToRevise <- LabAgencyToRevise[!duplicated(LabAgencyToRevise$labagencycode), ]
# LabAgencyToRevise <- LabAgencyToRevise[order(LabAgencyToRevise$labagencycode), ]


### LabAgency (ToxSum)
# LabAgencyToRevise.Sum <- ToxSum.2[!(ToxSum.2$labagencycode %in% CEDEN.Agency$AgencyCode), ]
# LabAgencyToRevise.Sum <- LabAgencyToRevise.Sum[!duplicated(LabAgencyToRevise.Sum$labagencycode), ]
# LabAgencyToRevise.Sum <- LabAgencyToRevise.Sum[order(LabAgencyToRevise.Sum$labagencycode), ]


### LabAgency (ToxBatch)
# LabAgency.Btch.ToRevise <- ToxBatch.2[!(ToxBatch.2$labagencycode %in% CEDEN.Agency$AgencyCode), ]
# LabAgency.Btch.ToRevise <- LabAgency.Btch.ToRevise[!duplicated(LabAgency.Btch.ToRevise$labagencycode), ]
# LabAgency.Btch.ToRevise <- LabAgency.Btch.ToRevise[order(LabAgency.Btch.ToRevise$labagencycode), ]


### LabSubmission (ToxBatch)
# LabSubmit.Btch.ToRevise <- ToxBatch.2[!(ToxBatch.2$labsubmissioncode %in% CEDEN.LabSubmit$LabSubmissionCode), ]
# LabSubmit.Btch.ToRevise <- LabSubmit.Btch.ToRevise[!duplicated(LabSubmit.Btch.ToRevise$labsubmissioncode), ]
# LabSubmit.Btch.ToRevise <- LabSubmit.Btch.ToRevise[order(LabSubmit.Btch.ToRevise$labsubmissioncode), ]


### Matrix (ToxRes)
# MatrixToRevise <- ToxRes.2[!(ToxRes.2$matrixname %in% CEDEN.Matrix$MatrixName), ]
# MatrixToRevise <- MatrixToRevise[!duplicated(MatrixToRevise$matrixname), ]
# MatrixToRevise <- MatrixToRevise[order(MatrixToRevise$matrixname), ]
#
ToxRes.2$matrixname <- ifelse(ToxRes.2$matrixname == "Samplewater" | ToxRes.2$matrixname == "SampleWater",
                                  "samplewater", ToxRes.2$matrixname)
ToxRes.2$matrixname <- ifelse(ToxRes.2$matrixname == "Blankwater", "blankwater", ToxRes.2$matrixname)
ToxRes.2$matrixname <- ifelse(ToxRes.2$matrixname == "Labwater", "labwater", ToxRes.2$matrixname)


### Matrix (ToxSum)
# MatrixToRevise.Sum <- ToxSum.2[!(ToxSum.2$matrixname %in% CEDEN.Matrix$MatrixName), ]
# MatrixToRevise.Sum <- MatrixToRevise.Sum[!duplicated(MatrixToRevise.Sum$matrixname), ]
# MatrixToRevise.Sum <- MatrixToRevise.Sum[order(MatrixToRevise.Sum$matrixname), ]
#
ToxSum.2$matrixname <- ifelse(ToxSum.2$matrixname == "Samplewater" | ToxSum.2$matrixname == "SampleWater",
                              "samplewater", ToxSum.2$matrixname)
ToxSum.2$matrixname <- ifelse(ToxSum.2$matrixname == "Blankwater", "blankwater", ToxSum.2$matrixname)
ToxSum.2$matrixname <- ifelse(ToxSum.2$matrixname == "Labwater", "labwater", ToxSum.2$matrixname)


### Method (ToxRes)
# MethodToRevise <- ToxRes.2[!(ToxRes.2$methodname %in% CEDEN.Method$MethodName), ]
# MethodToRevise <- MethodToRevise[!duplicated(MethodToRevise$methodname), ]
# MethodToRevise <- MethodToRevise[order(MethodToRevise$methodname), ]


### Method (ToxSum)
# MethodToRevise.Sum <- ToxSum.2[!(ToxSum.2$methodname %in% CEDEN.Method$MethodName), ]
# MethodToRevise.Sum <- MethodToRevise.Sum[!duplicated(MethodToRevise.Sum$methodname), ]
# MethodToRevise.Sum <- MethodToRevise.Sum[order(MethodToRevise.Sum$methodname), ]


### Organism (ToxRes)
# OrganismToRevise <- ToxRes.2[!(ToxRes.2$organismname %in% CEDEN.Orgnmsm$FinalID), ]
# OrganismToRevise <- OrganismToRevise[!duplicated(OrganismToRevise$organismname), ]
# OrganismToRevise <- OrganismToRevise[order(OrganismToRevise$organismname), ]
#
ToxRes.2$organismname <- ifelse(ToxRes.2$organismname == "CeDu", "Ceriodaphnia dubia", ToxRes.2$organismname)
ToxRes.2$organismname <- ifelse(ToxRes.2$organismname == "ChTe", "Chironomus tentans", ToxRes.2$organismname)
ToxRes.2$organismname <- ifelse(ToxRes.2$organismname == "HyAz", "Hyalella azteca", ToxRes.2$organismname)


### Organism (ToxSum)
# OrganismToRevise.Sum <- ToxSum.2[!(ToxSum.2$organismname %in% CEDEN.Orgnmsm$FinalID), ]
# OrganismToRevise.Sum <- OrganismToRevise.Sum[!duplicated(OrganismToRevise.Sum$organismname), ]
# OrganismToRevise.Sum <- OrganismToRevise.Sum[order(OrganismToRevise.Sum$organismname), ]
#
ToxSum.2$organismname <- ifelse(ToxSum.2$organismname == "CeDu", "Ceriodaphnia dubia", ToxSum.2$organismname)
ToxSum.2$organismname <- ifelse(ToxSum.2$organismname == "ChTe", "Chironomus tentans", ToxSum.2$organismname)
ToxSum.2$organismname <- ifelse(ToxSum.2$organismname == "HyAz", "Hyalella azteca", ToxSum.2$organismname)


### ResQualCode (ToxRes)
# Remove result -88 and resultqualcode is '=' or NA (actual NA, not 'NA').  Do this for both WQ & tox analytes.
# Unclear if they are supposed to be 'NA', 'ND', or 'NR', so remove.
# CAUTION! NA is changed to '=' later in script, so deal with it here first, before it becomes a CEDEN error.
# TRes88 <- ToxRes.2[ToxRes.2$result == -88, ]
# table(TRes88$analytename, TRes88$resultqualcode) # Caution, table doesn't show NA (but does show 'NA')
# WQAnalyte <- c("Alkalinity as CaCO3", "Ammonia as N", "Ammonia as NH3", "Hardness as CaCO3", "Oxygen, Dissolved", "pH",
#                "SpecificConductivity", "ElectricalConductivity", "Temperature")
ToxRes.2 <- ToxRes.2 %>%
  filter(!(result == -88 & (is.na(resultqualcode) | resultqualcode == '=')))



### SampleDate (ToxRes)
ToxRes.2 <- ToxRes.2 %>%
  mutate(sampledate = as.Date(sampledate))

### SampleDate (ToxSum)
ToxSum.2 <- ToxSum.2 %>%
  mutate(sampledate = as.Date(sampledate))



### SampleTypeCode (ToxRes)
# SampleTypeToRevise <- ToxRes.2[!(ToxRes.2$sampletypecode %in% CEDEN.SmplTyp$SampleTypeCode), ]
# SampleTypeToRevise <- SampleTypeToRevise[!duplicated(SampleTypeToRevise$sampletypecode), ]
# SampleTypeToRevise <- SampleTypeToRevise[order(SampleTypeToRevise$sampletypecode), ]
#
ToxRes.2$sampletypecode <- ifelse(ToxRes.2$sampletypecode == "grab" | ToxRes.2$sampletypecode == "GRAB",
                               "Grab", ToxRes.2$sampletypecode)
ToxRes.2$sampletypecode <- ifelse(ToxRes.2$sampletypecode == "BlankSp", "CNDL", ToxRes.2$sampletypecode)


### SampleTypeCode (ToxSum)
# SmplTypToRevise.Sum <- ToxSum.2[!(ToxSum.2$sampletypecode %in% CEDEN.SmplTyp$SampleTypeCode), ]
# SmplTypToRevise.Sum <- SmplTypToRevise.Sum[!duplicated(SmplTypToRevise.Sum$sampletypecode), ]
# SmplTypToRevise.Sum <- SmplTypToRevise.Sum[order(SmplTypToRevise.Sum$sampletypecode), ]
#
ToxSum.2$sampletypecode <- ifelse(ToxSum.2$sampletypecode == "grab" | ToxSum.2$sampletypecode == "GRAB",
                                  "Grab", ToxSum.2$sampletypecode)
ToxSum.2$sampletypecode <- ifelse(ToxSum.2$sampletypecode == "BlankSp", "CNDL", ToxSum.2$sampletypecode)


### Significant Effect (ToxSum)
# CEDEN.SigEff$SigEffectCode <- ifelse(is.na(CEDEN.SigEff$SigEffectCode), "NA", CEDEN.SigEff$SigEffectCode) # "NA" brought in as NA
# SigEffToRevise.Sum <- ToxSum.2[!(ToxSum.2$sigeffect %in% CEDEN.SigEff$SigEffectCode), ]
# SigEffToRevise.Sum <- SigEffToRevise.Sum[!duplicated(SigEffToRevise.Sum$sigeffect), ]
# SigEffToRevise.Sum <- SigEffToRevise.Sum[order(SigEffToRevise.Sum$sigeffect), ]


### Startdate (ToxBatch)
ToxBatch.2 <- ToxBatch.2 %>%
  mutate(startdate = as.Date(startdate))


### Statistical Method (ToxSum)
# StatMethdToRevise.Sum <- ToxSum.2[!(ToxSum.2$statisticalmethod %in% CEDEN.Variable$ValueCode), ]
# StatMethdToRevise.Sum <- StatMethdToRevise.Sum[!duplicated(StatMethdToRevise.Sum$statisticalmethod), ]
# StatMethdToRevise.Sum <- StatMethdToRevise.Sum[order(StatMethdToRevise.Sum$unitanalytename), ]
#
ToxSum.2$statisticalmethod <- ifelse(ToxSum.2$statisticalmethod == "t-test", "T-test", ToxSum.2$statisticalmethod)


### SubmittingAgency (ToxBatch.2)
# SubmitAgncyToRevise <- ToxBatch.2[!(ToxBatch.2$submittingagencycode %in% CEDEN.Agency$AgencyCode), ]
# SubmitAgncyToRevise <- SubmitAgncyToRevise[!duplicated(SubmitAgncyToRevise$submittingagencycode), ]
# SubmitAgncyToRevise <- SubmitAgncyToRevise[order(SubmitAgncyToRevise$submittingagencycode), ]
#
ToxBatch.2$submittingagencycode <- ifelse(ToxBatch.2$submittingagencycode == "Naut", "NAUT", ToxBatch.2$submittingagencycode)
ToxBatch.2$submittingagencycode <- ifelse(ToxBatch.2$submittingagencycode == "LASGRWC", "CWH", ToxBatch.2$submittingagencycode)


### Time Point (ToxRes)
# TimePtToRevise <- ToxRes.2[!(ToxRes.2$timepoint %in% CEDEN.TimePt$TimePointName), ]
# TimePtToRevise <- TimePtToRevise[!duplicated(TimePtToRevise$timepoint), ]
# TimePtToRevise <- TimePtToRevise[order(TimePtToRevise$timepoint), ]
#
ToxRes.2$timepoint <- ifelse(ToxRes.2$timepoint == "Day 10 ", "Day 10", ToxRes.2$timepoint)
ToxRes.2$timepoint <- ifelse(ToxRes.2$timepoint == "Final" | ToxRes.2$timepoint == "FINAL", "final", ToxRes.2$timepoint)
ToxRes.2$timepoint <- ifelse(ToxRes.2$timepoint == "High", "high", ToxRes.2$timepoint)
ToxRes.2$timepoint <- ifelse(ToxRes.2$timepoint == "Low", "low", ToxRes.2$timepoint)
ToxRes.2$timepoint <- ifelse(ToxRes.2$timepoint == "Initial" | ToxRes.2$timepoint == "INITIAL" |
                               ToxRes.2$timepoint == "initial ", "initial", ToxRes.2$timepoint)


### Time Point (ToxSum)
# TimePtToRevise.Sum <- ToxSum.2[!(ToxSum.2$timepoint %in% CEDEN.TimePt$TimePointName), ]
# TimePtToRevise.Sum <- TimePtToRevise.Sum[!duplicated(TimePtToRevise.Sum$timepoint), ]
# TimePtToRevise.Sum <- TimePtToRevise.Sum[order(TimePtToRevise.Sum$timepoint), ]
#
ToxSum.2$timepoint <- ifelse(ToxSum.2$timepoint == "Day 10 ", "Day 10", ToxSum.2$timepoint)
ToxSum.2$timepoint <- ifelse(ToxSum.2$timepoint == "Final" | ToxSum.2$timepoint == "FINAL", "final", ToxSum.2$timepoint)
ToxSum.2$timepoint <- ifelse(ToxSum.2$timepoint == "High", "high", ToxSum.2$timepoint)
ToxSum.2$timepoint <- ifelse(ToxSum.2$timepoint == "Low", "low", ToxSum.2$timepoint)
ToxSum.2$timepoint <- ifelse(ToxSum.2$timepoint == "Initial" | ToxSum.2$timepoint == "INITIAL" |
                               ToxSum.2$timepoint == "initial ", "initial", ToxSum.2$timepoint)


### Test Duration (ToxRes)
# TestDurationToRevise <- ToxRes.2[!(ToxRes.2$testduration %in% CEDEN.TestDur$ToxTestDurName), ]
# TestDurationToRevise <- TestDurationToRevise[!duplicated(TestDurationToRevise$testduration), ]
# TestDurationToRevise <- TestDurationToRevise[order(TestDurationToRevise$testduration), ]
#
ToxRes.2$testduration <- ifelse(ToxRes.2$testduration == "10 Days", "10 days", ToxRes.2$testduration)
ToxRes.2$testduration <- ifelse(ToxRes.2$testduration == "8 Days", "8 days", ToxRes.2$testduration)
ToxRes.2$testduration <- ifelse(ToxRes.2$testduration == "7 Days", "7 days", ToxRes.2$testduration)


### Test Duration (ToxSum)
# TestDurtnToRevise.Sum <- ToxSum.2[!(ToxSum.2$testduration %in% CEDEN.TestDur$ToxTestDurName), ]
# TestDurtnToRevise.Sum <- TestDurtnToRevise.Sum[!duplicated(TestDurtnToRevise.Sum$testduration), ]
# TestDurtnToRevise.Sum <- TestDurtnToRevise.Sum[order(TestDurtnToRevise.Sum$testduration), ]
#
ToxSum.2$testduration <- ifelse(ToxSum.2$testduration == "10 Days", "10 days", ToxSum.2$testduration)
ToxSum.2$testduration <- ifelse(ToxSum.2$testduration == "8 Days", "8 days", ToxSum.2$testduration)
ToxSum.2$testduration <- ifelse(ToxSum.2$testduration == "7 Days", "7 days", ToxSum.2$testduration)


### Test QACode (ToxSum)
# TQA.ToRevise.Sum <- ToxSum.2[!(ToxSum.2$testqacode %in% CEDEN.QACode$QACode), ]
# TQA.ToRevise.Sum <- TQA.ToRevise.Sum[!duplicated(TQA.ToRevise.Sum$testqacode), ]
# TQA.ToRevise.Sum <- TQA.ToRevise.Sum[order(TQA.ToRevise.Sum$testqacode), ]


### ToxPointMethod (ToxRes)
ToxRes.2 <- ToxRes.2 %>%
  mutate(toxpointmethod = ifelse(toxpointmethod == "Probe" | toxpointmethod == "probe", "ToxWQMeasurement", toxpointmethod))


### Unit (ToxRes)
# UnitToRevise <- ToxRes.2[!(ToxRes.2$unitanalytename %in% CEDEN.Unit$UnitName), ]
# UnitToRevise <- UnitToRevise[!duplicated(UnitToRevise$unitanalytename), ]
# UnitToRevise <- UnitToRevise[order(UnitToRevise$unitanalytename), ]
#
ToxRes.2$unitanalytename <- ifelse(ToxRes.2$unitanalytename == "C", "Deg C", ToxRes.2$unitanalytename)
ToxRes.2$unitanalytename <- ifelse(ToxRes.2$unitanalytename == "MG/L", "mg/L", ToxRes.2$unitanalytename)
ToxRes.2$unitanalytename <- ifelse(ToxRes.2$unitanalytename == "None", "none", ToxRes.2$unitanalytename)
ToxRes.2$unitanalytename <- ifelse(ToxRes.2$unitanalytename == "Num/rep", "Num/Rep", ToxRes.2$unitanalytename)


### Unit (ToxSum)
# UnitToRevise.Sum <- ToxSum.2[!(ToxSum.2$unitanalytename %in% CEDEN.Unit$UnitName), ]
# UnitToRevise.Sum <- UnitToRevise.Sum[!duplicated(UnitToRevise.Sum$unitanalytename), ]
# UnitToRevise.Sum <- UnitToRevise.Sum[order(UnitToRevise.Sum$unitanalytename), ]
#
ToxSum.2$unitanalytename <- ifelse(ToxSum.2$unitanalytename == "C", "Deg C", ToxSum.2$unitanalytename)
ToxSum.2$unitanalytename <- ifelse(ToxSum.2$unitanalytename == "MG/L", "mg/L", ToxSum.2$unitanalytename)
ToxSum.2$unitanalytename <- ifelse(ToxSum.2$unitanalytename == "None", "none", ToxSum.2$unitanalytename)
ToxSum.2$unitanalytename <- ifelse(ToxSum.2$unitanalytename == "Num/rep", "Num/Rep", ToxSum.2$unitanalytename)


### WQSource (ToxRes)
# WQSourceToRevise.Res <- ToxRes.2[!(ToxRes.2$wqsource %in% CEDEN.Matrix$MatrixName), ]
# WQSourceToRevise.Res <- WQSourceToRevise.Res[!duplicated(WQSourceToRevise.Res$wqsource), ]
# WQSourceToRevise.Res <- WQSourceToRevise.Res[order(WQSourceToRevise.Res$wqsource), ]
#
ToxRes.2 <- ToxRes.2 %>%
  mutate(wqsource = ifelse(wqsource == "overlying water" | wqsource == "Overlying water" | wqsource == "Overlying Water",
                           "overlyingwater", wqsource)) %>%
  mutate(wqsource = ifelse(wqsource == "Not applicable" | wqsource == "Not Applicable", "not applicable", wqsource))


### WQSource (ToxSum)
# WQSourceToRevise.Sum <- ToxSum.2[!(ToxSum.2$wqsource %in% CEDEN.Matrix$MatrixName), ]
# WQSourceToRevise.Sum <- WQSourceToRevise.Sum[!duplicated(WQSourceToRevise.Sum$wqsource), ]
# WQSourceToRevise.Sum <- WQSourceToRevise.Sum[order(WQSourceToRevise.Sum$wqsource), ]
#
ToxSum.2 <- ToxSum.2 %>%
  mutate(wqsource = ifelse(wqsource == "overlying water" | wqsource == "Overlying water" | wqsource == "Overlying Water",
                           "overlyingwater", wqsource)) %>%
  mutate(wqsource = ifelse(wqsource == "Not applicable" | wqsource == "Not Applicable", "not applicable", wqsource))



#####---  Calculate Percent Effect ---#####
### BEGIN
## First, resolve some issues found
ToxSum.2b <- ToxSum.2
ToxSum.2b$analytename <- ifelse(ToxSum.2b$toxbatch == "11CerioEMD5" & ToxSum.2b$stationcode == "SMC00684"
                                & ToxSum.2b$statisticalmethod == "Fisher", "Survival", ToxSum.2b$analytename)
ToxSum.2b$unitanalytename <- ifelse(ToxSum.2b$toxbatch == "11CerioEMD5" & ToxSum.2b$stationcode == "SMC00684"
                                & ToxSum.2b$statisticalmethod == "Fisher", "%", ToxSum.2b$unitanalytename)
ToxSum.2b$sampletypecode <- ifelse(ToxSum.2b$toxbatch == "SMC060110" & ToxSum.2b$stationcode == "LABQA",
                                   "CNEG", ToxSum.2b$sampletypecode)

#table(ToxSum.2b$matrixname, ToxSum.2b$sampletypecode)


## Create variable unique for matching controls with samples
PE.Sum <- ToxSum.2b
PE.Sum$ID1 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint, sep="_")
PE.Sum$ID2 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint, PE.Sum$stationcode, sep="_")
PE.Sum$ID3 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint, PE.Sum$stationcode,
                    PE.Sum$sampletypecode, PE.Sum$matrixname, PE.Sum$fieldreplicate, sep="_")  #need to add concentration?
PE.Sum$ID4 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint, PE.Sum$stationcode,
                    PE.Sum$sampletypecode, PE.Sum$matrixname, PE.Sum$fieldreplicate, PE.Sum$concentration, PE.Sum$dilution, sep="_")
PE.Sum <- PE.Sum[order(PE.Sum$ID3, PE.Sum$stationcode), ]
# PE.Sum.Dup3 <- PE.Sum[duplicated(PE.Sum$ID3), ] # dups from concentrations, which are not part of the pasted variable
# S0906S077078 <- PE.Sum[PE.Sum$toxbatch == "0906-S077/078", ] # LABQA, referencetoxicant, CNEG; SM01341, labwater, samplewater, CNEG, Grab; SMC08845, labwater, samplewater, CNEG, Grab
# #write.csv(S0906S077078, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/ToxBatch_0906S077078.csv')
# S0906S081085086 <- PE.Sum[PE.Sum$toxbatch == "0906-S081/085/086", ] # fieldreplicates
# ABCLCOO0516215cerWTOX <- PE.Sum[PE.Sum$toxbatch == "ABCL_COO0516.215cer_W_TOX", ] #dilution in field sample (samplewater), but not in LABQA CNEG
# S09CerioEMD1 <- PE.Sum[PE.Sum$toxbatch == "09CerioEMD1", ] # LABQA, conc=0, sampletypecode CNEG (matrix=blankwater) & CNDL (matrix=referencetoxicant)
# SMC060110 <- PE.Sum[PE.Sum$toxbatch == "SMC060110", ] # sampletypecode=Grab for samples, =CNEG for LABQA, matrix=samplewater for sample & LABQA
# S11CerioEMD5 <- PE.Sum[PE.Sum$toxbatch == "11CerioEMD5", ]

###
# Take away message: LABQA does not always mean it's the sample Ctrl.
# Sometimes the LABQA only applies to the reference toxicant test (see toxbatch 0906-S077/078)
# THEREFORE: First see if there's a non-LABQA stationcode with CNEG and use this as the Ctrl,
# if there's no non-LABQA CNEG then use LABQA CNEG ???
###

### Note: a few methods to match sample with Ctrl were devised.  Two different orderings of the methods are shown below,
#  but both ordering of methods won't be used.


# Methods to get Ctrl.  Need to establish an order.
# 1. stationcode = LABQA, sampletypecode = CNEG, matrix=blankwater (does this ever refer to a reference toxicant Ctrl?)
# 2. stationcode = LABQA, sampletypecode = CNEG, matrix=referencetoxicant
# 3. stationcode = LABQA, sampletypecode = CNEG, matrix = blankmatrix (for at least 1 sediment test)
# 4. stationcode = LABQA, sampletypecode = CNEG, matrix = sediment
# 5. sampletypecode = CNEG, matrix=blankwater, stationcode=stationcode.
# 6. sampletypecode = CNEG, matrix=labwater, stationcode=stationcode.  stationcode=LABQA for ref tox (e.g., toxbatch = 0906-S077/078)
# 7. sampletypecode = CNDL, matrix = referencetoxicant, concentration = 0 (e.g., toxbatch = 09CerioEMD1)
# 8. stationcode = LABQA, sampletypecode = CNEG, matrix=labwater
# 9. stationcode = LABQA, sampletypecode = CNEG, matrix=samplewater
# 10. Results table, sampletypecode = CNEG, matrix = labwater (need to calculate a mean Ctrl from lab replicates) (e.g., toxbatch = 0906-S071)

# # Any reference toxicant QA for LABQA, CNEG, blankwater?
# QA.blankwater <- PE.Sum[PE.Sum$stationcode=="LABQA" & PE.Sum$sampletypecode=="CNEG" & PE.Sum$matrixname == "blankwater", ]
# Batch.QA.blankwater <- PE.Sum[PE.Sum$toxbatch %in% QA.blankwater$toxbatch, ]
# table(Batch.QA.blankwater$toxbatch, Batch.QA.blankwater$sampletypecode)
# ABCL_COO0610365cer_W_TOX <- Batch.QA.blankwater[Batch.QA.blankwater$toxbatch == "ABCL_COO0610365cer_W_TOX", ] # n=4 CNEG (duplicate CNEG records?)
# NAUT_1407S129_W_TOX <- Batch.QA.blankwater[Batch.QA.blankwater$toxbatch == "NAUT_1407-S129_W_TOX", ] # n=3 CNEG (2 timepoints for survival)
# NAUT_1407S133_W_TOX <- Batch.QA.blankwater[Batch.QA.blankwater$toxbatch == "NAUT_1407-S133_W_TOX", ] # n=3 CNEG (2 timepoints for survival)
#
# QA.reftox <- PE.Sum[PE.Sum$stationcode=="LABQA" & PE.Sum$sampletypecode=="CNEG" & PE.Sum$matrixname == "referencetoxicant", ]
# Batch.QA.reftox <- PE.Sum[PE.Sum$toxbatch %in% QA.reftox$toxbatch, ]
# table(Batch.QA.reftox$toxbatch, Batch.QA.reftox$sampletypecode)
#
# # Any instance where CNEG blankwater is used as Ctrl instead of referencetoxicant conc=0?  Is there a way to future proof?
# table(Batch.QA.blankwater$stationcode) # referencetoxicant is only associated with LABQA.  All other stationcodes are actual sites.
# Batch.QA.blankwater2 <- Batch.QA.blankwater[Batch.QA.blankwater$stationcode == "LABQA", ]
# Batch.QA.blankwater2$ID1b <- paste(Batch.QA.blankwater2$ID1, Batch.QA.blankwater2$matrixname, Batch.QA.blankwater2$concentration, sep="_")
# Batch.QA.blankwater2$ID1c <- paste(Batch.QA.blankwater2$ID1, Batch.QA.blankwater2$matrixname, sep="_")
# Batch.QA.blankwater3 <- Batch.QA.blankwater2[, c("stationcode", "sampletypecode", "matrixname", "toxbatch", "concentration", "ID1b", "ID1c")]
# Batch.QA.blankwater3.RefTox <- Batch.QA.blankwater3[Batch.QA.blankwater3$matrixname == "referencetoxicant", ]
# Batch.QA.blankwater3.RefTox <- Batch.QA.blankwater3.RefTox[order(Batch.QA.blankwater3.RefTox$ID1b), ]
# RefToxTests <- Batch.QA.blankwater3.RefTox[!duplicated(Batch.QA.blankwater3.RefTox$ID1c), ]
# # Answer: All referencetoxicant records have an accompanying conc=0.  None use matrix = blankwater as the Control.
#
# # Any CNEG, referencetoxicant, conc != 0 or != -88?
# PE.Sum.CNEG <- PE.Sum[PE.Sum$sampletypecode == "CNEG", ]
# table(PE.Sum.CNEG$concentration)
# Answer: All sampletypecode CNEG have either conc = 0 or -88


# ### Is there a CNEG (or CNDL with conc = 0) for every sample?  Can we use CNEG exclusively as the control?
# ###BEGIN CNEG Investigation
# # PE.Sum ID5 is ID4 without stationcode. PE.Sum ID6 is ID4 without stationcode or sampletypecode
# PE.Sum$ID5 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint,
#                     PE.Sum$sampletypecode, PE.Sum$matrixname, PE.Sum$fieldreplicate, PE.Sum$concentration, PE.Sum$dilution, sep="_")
# PE.Sum$ID6 <- paste(PE.Sum$toxbatch, PE.Sum$organismname, PE.Sum$analytename, PE.Sum$timepoint,
#                     PE.Sum$matrixname, PE.Sum$fieldreplicate, PE.Sum$concentration, PE.Sum$dilution, sep="_")
# 
# CNEG.All <- PE.Sum %>%
#   filter(sampletypecode == "CNEG")
# NoCNEG <- setdiff(PE.Sum$ID5, CNEG.All$ID5) #Which PE.Sum$ID5 are not found in CNEG.All$ID5?  Answer = 
# NoCNEG <- PE.Sum[PE.Sum$ID5 %in% NoCNEG, ]
# # Note: sampletype 'CNDL' = Negative Control Dilution
# 
# CNEG.CNDL <- PE.Sum %>%
#   filter(sampletypecode == "CNEG" | (sampletypecode == 'CNDL' & concentration == 0))
# 
# NoCNEG.CNDL <- setdiff(PE.Sum$ID5, CNEG.CNDL$ID5) #Which PE.Sum$ID5 are not found in CNEG.All$ID5?  Answer = 
# NoCNEG.CNDL <- PE.Sum[PE.Sum$ID5 %in% NoCNEG.CNDL, ]
# 
# # Any CNDL conc = 0 without a CNEG? Yes, toxbatch 09CerioEMD4 (Survival & Young/female)
# CNEG.CNDL2 <- CNEG.CNDL %>%
#   arrange(-xtfrm(sampletypecode)) %>%
#   filter(!duplicated(ID6))
# NoCNEG.CNDL2 <- setdiff(PE.Sum$ID6, CNEG.CNDL2$ID6) #Which PE.Sum$ID5 are not found in CNEG.All$ID5?  Answer = 1,010
# NoCNEG.CNDL2 <- PE.Sum[PE.Sum$ID6 %in% NoCNEG.CNDL2, ]
# 
# Test1 <- PE.Sum[PE.Sum$toxbatch == '0906-S077/078', ] # Has CNEG, but it's matrix=labwater, while Grab is matrix=samplewater,
# #    THEREFORE need more than just 'CNEG' to match up control and samples. (CNEG + ID1?)
#
# Also, 2 different controls can exist per tox batch.  For example, tox batch 1006-S004/S005 uses both stationcode SMC26909 & SMC27709 as CNEG.
#
# ###END CNEG/CNDL investigation



## Method 1. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=blankwater
Controls1 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "blankwater", ]
Controls1 <- Controls1 %>%
  dplyr::rename(MeanCtrl1 = mean) %>%
  mutate(Mthd1 = paste(Controls1$ID1, "Mthd1", sep="")) %>%
  filter(!duplicated(Mthd1)) # removes 2 duplicate CNEG records for toxbatch = ABCL_COO0610365cer_W_TOX

PE.Sum$Mthd1 <- ifelse((PE.Sum$sampletypecode == "Grab" & PE.Sum$toxbatch %in% Controls1$toxbatch) |
                         (PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                            PE.Sum$matrixname == "blankwater"),
                       paste(PE.Sum$ID1, "Mthd1", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls1[, c("Mthd1", "MeanCtrl1")], by = ("Mthd1"="Mthd1"))

# Stragglers
PE.NotMthd1 <- PE.Sum[is.na(PE.Sum$MeanCtrl1), ] # this identified 929 records
# table(PE.NotMthd1$matrixname)
# table(PE.NotMthd1$sampletypecode)
# Intgrtd <- PE.NotMthd1[PE.NotMthd1$sampletypecode == "Integrated", ]
# Int.Batch <- PE.NotMthd1[PE.NotMthd1$toxbatch == "NAUT_1905-S092_S_TOX", ]


## Method 2. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=referencetoxicant
Controls2 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "referencetoxicant", ]
Controls2 <- Controls2 %>%
  dplyr::rename(MeanCtrl2 = mean)
Controls2$Mthd2 <- paste(Controls2$ID1, "Mthd2", sep="")
Controls2 <- Controls2[!duplicated(Controls2$Mthd2), ]

PE.Sum$Mthd2 <- ifelse(PE.Sum$matrixname == "referencetoxicant", paste(PE.Sum$ID1, "Mthd2", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls2[, c("Mthd2", "MeanCtrl2")], by = ("Mthd2"="Mthd2"))

# Stragglers
PE.NotMthd2 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2), ]



## Method 3. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=blankmatrix
Controls3 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "blankmatrix", ]
Controls3 <- Controls3 %>%
  dplyr::rename(MeanCtrl3 = mean)
Controls3$Mthd3 <- paste(Controls3$ID1, "Mthd3", sep="")
Controls3 <- Controls3[!duplicated(Controls3$Mthd3), ]
#Batch.Ctrl3 <- PE.Sum[PE.Sum$toxbatch %in% Controls3$toxbatch, ]
#table(Batch.Ctrl3$sampletypecode, Batch.Ctrl3$matrixname)

PE.Sum$Mthd3 <- ifelse(PE.Sum$toxbatch %in% Controls3$toxbatch, paste(PE.Sum$ID1, "Mthd3", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls3[, c("Mthd3", "MeanCtrl3")], by = ("Mthd3"="Mthd3"))

# Stragglers
PE.NotMthd3 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3), ]



## Method 4. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=sediment
Controls4 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "sediment", ]
Controls4 <- Controls4 %>%
  dplyr::rename(MeanCtrl4 = mean)
Controls4$Mthd4 <- paste(Controls4$ID1, "Mthd4", sep="")
Controls4 <- Controls4[!duplicated(Controls4$Mthd4), ]
#Batch.Ctrl4 <- PE.Sum[PE.Sum$toxbatch %in% Controls4$toxbatch, ]
#table(Batch.Ctrl4$sampletypecode, Batch.Ctrl4$matrixname)

PE.Sum$Mthd4 <- ifelse(PE.Sum$toxbatch %in% Controls4$toxbatch & PE.Sum$matrixname == "sediment",
                       paste(PE.Sum$ID1, "Mthd4", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls4[, c("Mthd4", "MeanCtrl4")], by = ("Mthd4"="Mthd4"))

# Stragglers
PE.NotMthd4 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3)
                      & is.na(PE.Sum$MeanCtrl4), ]


## Method 5. Control: sampletypecode = CNEG, matrix=blankwater, stationcode=stationcode.
Controls5 <- PE.Sum[PE.Sum$stationcode != "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "blankwater", ]
Controls5 <- Controls5 %>%
  dplyr::rename(MeanCtrl5 = mean)
Controls5$Mthd5 <- paste(Controls5$ID2, Controls5$fieldreplicate, "Mthd5", sep="") # need stationcode & fieldreplicate
#Controls5.Dup <- Controls5[duplicated(Controls5$Mthd5), ]
Controls5 <- Controls5[!duplicated(Controls5$Mthd5), ] # removes 2 duplicate CNEG records for toxbatch = ABCL_COO0610365cer_W_TOX

PE.Sum$Mthd5 <- ifelse(PE.Sum$stationcode != "LABQA" & PE.Sum$toxbatch %in% Controls5$toxbatch,
                       paste(PE.Sum$ID2, PE.Sum$fieldreplicate, "Mthd5", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls5[, c("Mthd5", "MeanCtrl5")], by = ("Mthd5"="Mthd5"))

# Stragglers
PE.NotMthd5 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3)
                      & is.na(PE.Sum$MeanCtrl4) & is.na(PE.Sum$MeanCtrl5), ]



## Method 6. Control: sampletypecode = CNEG, matrix=labwater, stationcode=stationcode.  But stationcode=LABQA for ref tox
Controls6 <- PE.Sum[PE.Sum$stationcode != "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "labwater", ]
Controls6 <- Controls6 %>%
  dplyr::rename(MeanCtrl6 = mean)
Controls6$Mthd6 <- paste(Controls6$ID2, Controls6$fieldreplicate, "Mthd6", sep="") # need stationcode & fieldreplicate
#Controls6.Dup <- Controls6[duplicated(Controls6$Mthd6), ]
Controls6 <- Controls6[!duplicated(Controls6$Mthd6), ] # removes 2 duplicate CNEG records for toxbatch = ABCL_COO0610365cer_W_TOX

PE.Sum$Mthd6 <- ifelse(PE.Sum$stationcode != "LABQA" & PE.Sum$toxbatch %in% Controls6$toxbatch,
                       paste(PE.Sum$ID2, PE.Sum$fieldreplicate, "Mthd6", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls6[, c("Mthd6", "MeanCtrl6")], by = ("Mthd6"="Mthd6"))

# Stragglers
PE.NotMthd6 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3)
                      & is.na(PE.Sum$MeanCtrl4) & is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6), ]
#C09CerioEMD1 <- PE.Sum[PE.Sum$toxbatch == "09CerioEMD1", ]



## Method 7. Control: sampletypecode = CNDL, matrix = referencetoxicant, concentration = 0 or -88
Controls7 <- PE.Sum[!is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNDL" & PE.Sum$matrixname == "referencetoxicant" &
                      (PE.Sum$concentration == 0 | PE.Sum$concentration == -88), ]
Controls7 <- Controls7 %>%
  dplyr::rename(MeanCtrl7 = mean) %>%
  mutate(Mthd7 = paste(ID1, "Mthd7", sep="_")) %>%
  filter(!duplicated(Mthd7))

PE.Sum$Mthd7 <- ifelse(PE.Sum$sampletypecode == "CNDL" & PE.Sum$matrixname == "referencetoxicant",
                       paste(PE.Sum$ID1, "Mthd7", sep="_"), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls7[, c("Mthd7", "MeanCtrl7")], by = ("Mthd7"="Mthd7"))

# Stragglers
PE.NotMthd7 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3)
                      & is.na(PE.Sum$MeanCtrl4) & is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6)
                      & is.na(PE.Sum$MeanCtrl7), ]
#table(PE.NotMthd7$sampletypecode, PE.NotMthd7$matrixname)



## Method 8. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=labwater
Controls8 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "labwater", ]
Controls8 <- Controls8 %>%
  dplyr::rename(MeanCtrl8 = mean)
Controls8$Mthd8 <- paste(Controls8$ID1, "Mthd8", sep="")
Controls8 <- Controls8[!duplicated(Controls8$Mthd8), ]

PE.Sum$Mthd8 <- ifelse((PE.Sum$sampletypecode == "Grab" & PE.Sum$toxbatch %in% Controls8$toxbatch) |
                         (PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                            PE.Sum$matrixname == "labwater"),
                       paste(PE.Sum$ID1, "Mthd8", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls8[, c("Mthd8", "MeanCtrl8")], by = ("Mthd8"="Mthd8"))

# Stragglers
PE.NotMthd8 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3) & is.na(PE.Sum$MeanCtrl4) &
                        is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6) & is.na(PE.Sum$MeanCtrl7) &
                        is.na(PE.Sum$MeanCtrl8), ]
#table(PE.NotMthd8$sampletypecode, PE.NotMthd8$matrixname)
#Mthd8.CNEG <- PE.NotMthd8[PE.NotMthd8$sampletypecode == "CNEG", ]



## Method 9. Control: stationcode=LABQA, sampletypecode=CNEG & matrix=labwater
Controls9 <- PE.Sum[PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                      PE.Sum$matrixname == "samplewater", ]
Controls9 <- Controls9 %>%
  dplyr::rename(MeanCtrl9 = mean)
Controls9$Mthd9 <- paste(Controls9$ID1, "Mthd9", sep="")
Controls9 <- Controls9[!duplicated(Controls9$Mthd9), ]

PE.Sum$Mthd9 <- ifelse((PE.Sum$sampletypecode == "Grab" & PE.Sum$toxbatch %in% Controls9$toxbatch) |
                         (PE.Sum$stationcode == "LABQA" & !is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNEG" &
                            PE.Sum$matrixname == "samplewater"),
                       paste(PE.Sum$ID1, "Mthd9", sep=""), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls9[, c("Mthd9", "MeanCtrl9")], by = ("Mthd9"="Mthd9"))

# Stragglers
PE.NotMthd9 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3) & is.na(PE.Sum$MeanCtrl4) &
                        is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6) & is.na(PE.Sum$MeanCtrl7) & is.na(PE.Sum$MeanCtrl8) &
                        is.na(PE.Sum$MeanCtrl9), ]
#table(PE.NotMthd9$sampletypecode, PE.NotMthd9$matrixname)

#T09CerioEMD4 <- PE.Sum[PE.Sum$ID1 == "09CerioEMD4_Ceriodaphnia dubia_Survival_Day 7", ] # closer look at ID1 for one of the last stragglers



## Method 10. Use Results table to calculate a mean from control replicates
Res2Ctrl     <- ToxRes.2 %>%
  mutate(ID1 = paste(toxbatch, organismname, analytename, timepoint, sep="_")) %>%
  arrange(ID1)

Res2Ctrl.2   <- Res2Ctrl[Res2Ctrl$ID1 %in% PE.NotMthd9$ID1, ]        # Use this to identify the samples of interest (stragglers from method 9)
Res2Ctrl.3   <- Res2Ctrl.2[Res2Ctrl.2$sampletypecode == "CNEG", ]    # Use this to identify the control replicates
#table(Res2Ctrl.3$analytename)
# Create Control mean by ID1
ResCtrlMean <- aggregate(Res2Ctrl.3$result, by=list(Res2Ctrl.3$ID1), FUN=mean)  # Calculate a mean from the replicates
ResCtrlMean <- ResCtrlMean %>%
  dplyr::rename(ID1=Group.1, MeanCtrl10 = x) %>%
  left_join(Res2Ctrl.3[, c("ID1", "toxresultcomments")], by=c("ID1" = "ID1")) %>%
  filter(!duplicated(ID1))


PE.Sum <- PE.Sum %>%
  mutate(Mthd10 = ifelse(PE.Sum$ID1 %in% PE.NotMthd9$ID1, paste(ID1, "Mthd10", sep="_"), "NotYet")) %>%  # Create Mthd10
  left_join(ResCtrlMean[, c("ID1", "MeanCtrl10", "toxresultcomments")], by = ("ID1"="ID1")) %>%          # Add MeanCtrl10
  mutate(Mthd10 = ifelse((analytename == "Survival" & MeanCtrl10 == 0) |          # Mthd10 for poor control performance
                           (analytename == "Young/female" & MeanCtrl10 == 0) |
                           (!is.na(toxresultcomments) & toxresultcomments == "Control failed, Results compared to ref tox control"),
                         "NotYet", Mthd10)) %>%
  mutate(MeanCtrl10 = ifelse((analytename == "Survival" & MeanCtrl10 == 0) |          # Mthd10 for poor control performance
                               (analytename == "Young/female" & MeanCtrl10 == 0) |
                               (!is.na(toxresultcomments) & toxresultcomments == "Control failed, Results compared to ref tox control"),
                             NA, MeanCtrl10))

# #Bad Control
# R09CerioEMD4 <- Res2Ctrl.3[Res2Ctrl.3$ID1 == "09CerioEMD4_Ceriodaphnia dubia_Survival_Day 7", ] # Survival result replicates
# R09CerioEMD4_batch <- Res2Ctrl.3[Res2Ctrl.3$toxbatch == "09CerioEMD4", ] # Result batch


# Stragglers
PE.NotMthd10 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3) & is.na(PE.Sum$MeanCtrl4) &
                        is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6) & is.na(PE.Sum$MeanCtrl7) & is.na(PE.Sum$MeanCtrl8) &
                        is.na(PE.Sum$MeanCtrl9 & is.na(PE.Sum$MeanCtrl10)), ]


# # Any variable that can identify control replicates for these stragglers?
# T11CerioEMD3 <- Res2Ctrl[Res2Ctrl$ID1 == "11CerioEMD3_Ceriodaphnia dubia_Survival_Day 7", ] # No Ctrl associated with this straggler in the Results table
# S11CerioEMD3 <- PE.Sum[PE.Sum$ID1 == "11CerioEMD3_Ceriodaphnia dubia_Survival_Day 7", ] # No Ctrl associated with this straggler in the Results table
#
# Straggler9ResReps <- Res2Ctrl.2[Res2Ctrl.2$ID1 %in% PE.NotMthd10$ID1, ] # Get Result records for method 9 stragglers (from method 8 straggler pool)
# table(Straggler9ResReps$sampletypecode, Straggler9ResReps$matrixname)
# Straggler9ResReps2 <- Res2Ctrl[Res2Ctrl$ID1 %in% PE.NotMthd10$ID1, ] # Get Result records for method 9 stragglers (from All results table)
# table(Straggler9ResReps2$sampletypecode, Straggler9ResReps2$matrixname)
# # None of the remaining stragglers have a Control in the Results table.  All are sampletypecode=Grab & matrix=samplewater.



## Method 11. Control: sampletypecode = CNDL, matrix = referencetoxicant, concentration = 0 or -88 (Same as Method 7, but now
#   extend the use of the reference control to matrix=samplewater)
#T09CerioEMD4 <- PE.Sum[PE.Sum$ID1 == "09CerioEMD4_Ceriodaphnia dubia_Survival_Day 7", ] # closer look at ID1 for one of the last stragglers
####
Controls11 <- PE.Sum[!is.na(PE.Sum$sampletypecode) & PE.Sum$sampletypecode == "CNDL" & PE.Sum$matrixname == "referencetoxicant" &
                      (PE.Sum$concentration == 0 | PE.Sum$concentration == -88), ]
Controls11 <- Controls11 %>%
  dplyr::rename(MeanCtrl11 = mean) %>%
  mutate(Mthd11 = paste(ID1, "Mthd11", sep="_")) %>%
  filter(!duplicated(Mthd11))

PE.Sum$Mthd11 <- ifelse(PE.Sum$ID1 %in% Controls11$ID1 & PE.Sum$Mthd7 == "NotYet",
                       paste(PE.Sum$ID1, "Mthd11", sep="_"), "NotYet")

PE.Sum <- PE.Sum %>%
  left_join(Controls11[, c("Mthd11", "MeanCtrl11")], by = ("Mthd11"="Mthd11"))
#M11 <- PE.Sum[!is.na(PE.Sum$MeanCtrl11), ] # See who got picked up.  Remember, MeanCtrl1 is also represented.

# Stragglers
PE.NotMthd11 <- PE.Sum[is.na(PE.Sum$MeanCtrl1) & is.na(PE.Sum$MeanCtrl2) & is.na(PE.Sum$MeanCtrl3)
                      & is.na(PE.Sum$MeanCtrl4) & is.na(PE.Sum$MeanCtrl5) & is.na(PE.Sum$MeanCtrl6)
                      & is.na(PE.Sum$MeanCtrl7) & is.na(PE.Sum$MeanCtrl8) & is.na(PE.Sum$MeanCtrl9)
                      & is.na(PE.Sum$MeanCtrl10) & is.na(PE.Sum$MeanCtrl11), ]






### Consolidate MeanControl into 1 column.  Then calculate a percent effect.
PE.Sum$MeanCtrl <- ifelse(!is.na(PE.Sum$MeanCtrl1), PE.Sum$MeanCtrl1,
                          ifelse(!is.na(PE.Sum$MeanCtrl2), PE.Sum$MeanCtrl2,
                                 ifelse(!is.na(PE.Sum$MeanCtrl3), PE.Sum$MeanCtrl3,
                                        ifelse(!is.na(PE.Sum$MeanCtrl4), PE.Sum$MeanCtrl4,
                                               ifelse(!is.na(PE.Sum$MeanCtrl5), PE.Sum$MeanCtrl5,
                                               ifelse(!is.na(PE.Sum$MeanCtrl6), PE.Sum$MeanCtrl6,
                                                      ifelse(!is.na(PE.Sum$MeanCtrl7), PE.Sum$MeanCtrl7,
                                                             ifelse(!is.na(PE.Sum$MeanCtrl8), PE.Sum$MeanCtrl8,
                                                                    ifelse(!is.na(PE.Sum$MeanCtrl9), PE.Sum$MeanCtrl9,
                                                                           ifelse(!is.na(PE.Sum$MeanCtrl10), PE.Sum$MeanCtrl10,
                                                                                  ifelse(!is.na(PE.Sum$MeanCtrl11), PE.Sum$MeanCtrl11,NA)))))))))))
PE.NoCtrl <- PE.Sum[is.na(PE.Sum$MeanCtrl), ] # n=39
#table(PE.Sum$MeanCtrl) # 0-100, good!  This table misses the NAs though.
PE.Sum$PercentEffect <- ifelse(is.na(PE.Sum$MeanCtrl), NA,
                               (PE.Sum$MeanCtrl - PE.Sum$mean)/PE.Sum$MeanCtrl * 100) # Calculate percent effect
#table(PE.Sum$PercentEffect)
PE.Sum$PercentEffect <- ifelse(is.na(PE.Sum$PercentEffect), NA,
                               ifelse(PE.Sum$PercentEffect < 0, 0, PE.Sum$PercentEffect)) # Samples that had better response than control get 0 effect, rather than negative effect 

## Add Percent Effect to ToxSum.2b (which is basically PE.Sum; could just use PE.Sum and knock out unwanted variables later)
# PE.Sum.ID4Dup <- PE.Sum[duplicated(PE.Sum$ID4), ] # No dups found.  Therefore ID4 is useful for identifying unique summary records.
ToxSum.2b$ID4 <- paste(ToxSum.2b$toxbatch, ToxSum.2b$organismname, ToxSum.2b$analytename, ToxSum.2b$timepoint,
                       ToxSum.2b$stationcode, ToxSum.2b$sampletypecode, ToxSum.2b$matrixname, ToxSum.2b$fieldreplicate,
                       ToxSum.2b$concentration, ToxSum.2b$dilution, sep="_")

ToxSum.2b <- merge(ToxSum.2b, PE.Sum[, c("ID4", "PercentEffect")], by="ID4", all.x=TRUE) # Add percent effect.  Again, could just use PE.Sum.

#PE.NA  <- ToxSum.2b[is.na(ToxSum.2b$PercentEffect), ]
#Strags <- ToxSum.2b[ToxSum.2b$ID4 %in% PE.NotMthd10$ID4, ]

ToxSum.2b$PercentEffect <- ifelse(is.na(ToxSum.2b$PercentEffect), -88, ToxSum.2b$PercentEffect)  # Percent Effect = -88 for samples lacking a Control
ToxSum.2b$ID4 <- NULL

#write.csv(ToxSum.2b, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Tox Summary 8June21.csv')

### END CALCULATE PERCENT EFFECT






#####---  Conform Variables to CEDEN Templates  ---#####

### ToxSummaryResults
ToxSum.3 <- ToxSum.2b %>%
  inner_join(ToxBatch.1 %>%
               select(toxbatch)) %>%
  transmute(StationCode = stationcode,
            SampleDate = sampledate,
            ProjectCode = ifelse(StationCode == "LABQA", "Not Applicable", "SMC_SCCWRP"),  # also CNEG, CNDL, reference toxicant or ??
            EventCode = "WQ",
            ProtocolCode = ifelse(StationCode == "LABQA", "Not Applicable", "SWAMP_2016_WS"),
            AgencyCode = labagencycode,
            SampleComments = "",
            LocationCode = ifelse(StationCode == "LABQA", "Not Applicable", "Bank"),
            GeometryShape = ifelse(StationCode == "LABQA", "Not Applicable", "Point"),
            CollectionTime = "00:00",
            CollectionMethodCode=case_when(StationCode %in% c("LABQA")~"Not Applicable",
                                           matrixname %in% c("samplewater","samplewater, <1.2 um", "overlyingwater")~"Water_Grab",
                                           matrixname %in% c("sediment")~"Sed_Grab",
                                           matrixname %in% c("labwater","blankwater","blankmatrix")~"Not Applicable",
                                           StationCode %in% c("LABQA")~"Not Applicable",
                                           T~"FLAG"), #Probably "Not Applicable"
            SampleTypeCode = sampletypecode,
            Replicate = fieldreplicate,
            CollectionDeviceName="Not Recorded",
            CollectionDepth = -88,
            UnitCollectionDepth = "m",
            PositionWaterColumn=case_when(SampleTypeCode %in% c("Grab")~"Subsurface",
                                          SampleTypeCode %in% c("Integrated", "CNDL", "CNEG")~"Not Applicable",
                                          T~"FLAG"),
            LabCollectionComments="",
            ToxBatch = toxbatch,
            MatrixName = matrixname,
            MethodName = methodname,
            TestDuration = testduration,
            OrganismName = organismname,
            TestExposureType = ifelse(analytename == "Survival", "Acute", "Chronic"),
            QAControlID="SWAMP_2016_WS",
            SampleID="",
            LabSampleID = labsampleid,
            ToxTestComments=toxtestcomments,
            Treatment=treatment,
            Concentration=concentration,
            UnitTreatment=unittreatment,
            Dilution=dilution,
            WQSource=wqsource,
            ToxPointMethod=toxpointmethod,
            AnalyteName=analytename,
            FractionName=fractionname,
            UnitAnalyte=unitanalytename,
            TimePoint=timepoint,
            RepCount=repcount,
            Mean=mean,
            StdDev=stddev,
            StatisticalMethod=statisticalmethod,
            AlphaValue=alphavalue,
            bValue="",
            CalcValueType="Not Recorded",
            CalculatedValue=probability,
            CriticalValue=alphavalue,
            PercentEffect=PercentEffect,
            MSD=msd,
            EvalThreshold=evalthreshold,
            SigEffect=sigeffect,
            TestQACode=testqacode,
            ComplianceCode="NR",
            ToxPointSummaryComments=toxsummarycomments,
            TIENarrative="") %>%
  arrange(ToxBatch, SampleDate, StationCode)
#write.csv(ToxSum.3, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/ToxSummary_070221.csv')





### ToxReplicateResults
ToxRes.3 <- ToxRes.2 %>%
  inner_join(ToxBatch.1 %>%
               select(toxbatch)) %>%
  transmute(StationCode = stationcode,
            SampleDate = sampledate,
            ProjectCode = ifelse(StationCode == "LABQA", "Not Applicable", "SMC_SCCWRP"),  # also CNEG, CNDL, reference toxicant or ??
            EventCode = "WQ",
            ProtocolCode = ifelse(StationCode == "LABQA", "Not Applicable", "SWAMP_2016_WS"),
            AgencyCode = labagencycode,
            SampleComments = "",
            LocationCode = ifelse(StationCode == "LABQA", "Not Applicable", "Bank"),
            GeometryShape = ifelse(StationCode == "LABQA", "Not Applicable", "Point"),
            CollectionTime = "00:00",
            CollectionMethodCode=case_when(StationCode %in% c("LABQA")~"Not Applicable",
                                           matrixname %in% c("samplewater","samplewater, <1.2 um", "overlyingwater")~"Water_Grab",
                                           matrixname %in% c("sediment")~"Sed_Grab",
                                           matrixname %in% c("labwater","blankwater","blankmatrix")~"Not Applicable",
                                           StationCode %in% c("LABQA")~"Not Applicable",
                                           T~"FLAG"), #Probably "Not Applicable"
            SampleTypeCode = sampletypecode,
            Replicate = fieldreplicate,
            CollectionDeviceName="Not Recorded",
            CollectionDepth = -88,
            UnitCollectionDepth = "m",
            PositionWaterColumn=case_when(SampleTypeCode %in% c("Grab")~"Subsurface",
                                          SampleTypeCode %in% c("Integrated", "CNDL", "CNEG")~"Not Applicable",
                                          T~"FLAG"),
            LabCollectionComments="",
            ToxBatch = toxbatch,
            MatrixName = matrixname,
            MethodName = methodname,
            TestDuration = testduration,
            OrganismName = organismname,
            TestExposureType = ifelse(analytename == "Survival", "Acute", "Chronic"),
            QAControlID="SWAMP_2016_WS",
            SampleID="",
            LabSampleID = labsampleid,
            ToxTestComments=toxtestcomments,
            Treatment=treatment,
            Concentration=concentration,
            UnitTreatment=unittreatment,
            Dilution=dilution,
            WQSource=wqsource,
            ToxPointMethod=toxpointmethod,
            AnalyteName=analytename,
            FractionName=fractionname,
            UnitAnalyte=unitanalytename,
            TimePoint=timepoint,
            LabReplicate=labreplicate,
            OrganismPerRep = -88,
            Result=result,
            ResQualCode=ifelse(is.na(resultqualcode), "=", resultqualcode),
            ToxResultQACode="none",
            ComplianceCode="Com",
            ToxResultComments="") %>%
  arrange(ToxBatch, SampleDate, StationCode)
#write.csv(ToxRes.3, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/ToxResults_070221.csv')



### ToxBatch
ToxBatch.3 <- ToxBatch.2 %>%
  transmute(ToxBatch = toxbatch,
            StartDate=startdate,
            LabAgencyCode=labagencycode,
            LabSubmissionCode=labsubmissioncode,
            BatchVerificationCode="NR",
            RefToxBatch=reftoxbatch,
            OrganismAgeAtTestStart=organismageatteststart,
            SubmittingAgencyCode=submittingagencycode,
            OrganismSupplier=organismsupplier,
            ToxBatchComments=toxbatchcomments) %>%
  arrange(ToxBatch)
#write.csv(ToxBatch.3, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/ToxBatch_070221.csv')




#####--- Locations tab prep  ---#####
#Get actual latitude, longitude and AgencyCode) from PHab
PHab_query = "select * from sde.unified_phab"                   # Took over half an hour 5/28/2021 8:02-8:38
tbl_PHab   = tbl(con, sql(PHab_query))
PHab.1     = as.data.frame(tbl_PHab)
rm(PHab_query)

require(lubridate)

PHab.2 <- PHab.1 %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  arrange(stationcode, sampledate, actual_latitude, targetlatitude) %>%
  left_join(lustations.1[, c("stationid", "masterid")], by=c("stationcode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  mutate(SiteYear = paste(masterid, year(sampledate), sep="_"))

ToxSamples <- ToxSum.3 %>%
  left_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude")], by=c("StationCode" = "stationid")) %>%
  mutate(SampleDate = as.Date(SampleDate)) %>%
  mutate(SiteDate = paste(masterid, SampleDate, sep="_")) %>%
  mutate(SiteYear = paste(masterid, year(SampleDate), sep="_")) %>%
  filter(!duplicated(SiteDate),
         StationCode != "LABQA") %>%
  arrange(StationCode, SampleDate)

PHab.3 <- PHab.2 %>%
  filter(!duplicated(SiteDate)) %>%
  select(stationcode, sampledate, SiteDate, sampleagencycode, sampleagencyname,
         actual_latitude, actual_longitude, targetlatitude, targetlongitude, datum) %>%
  mutate(actual_latitude = ifelse(is.na(actual_latitude), targetlatitude, actual_latitude)) %>%
  mutate(actual_longitude = ifelse(is.na(actual_longitude), targetlongitude, actual_longitude)) %>%
  arrange(stationcode, sampledate) %>%
  filter(SiteDate %in% ToxSamples$SiteDate)
#rm(PHab.1, PHab.2)

## Add actual field sampling agency, or keep 'SCCWRP'
# table(PHab.3$sampleagencycode)
# table(PHab.3$sampleagencyname)
# table(AgencyLU$AgencyCode)     # See if PHab.3$sampleagencycode needs to be conformed to the CEDEN LUList
ToxSamples2 <- ToxSamples %>%
  left_join(PHab.3[, c("SiteDate", "sampleagencycode", "sampleagencyname",
                       "actual_latitude", "actual_longitude", "datum")], by=c("SiteDate" = "SiteDate")) %>%
  mutate(AgencyCode = ifelse(!is.na(sampleagencycode), sampleagencycode, AgencyCode)) %>%
  select(-c(sampleagencycode, sampleagencyname)) # Disable or disregard this line if investigating AgencyCode below
# table(ToxSamples2$AgencyCode)                        # Who's represented?
# AgSC <- ToxSamples2[ToxSamples2$AgencyCode == "SCCWRP", ]  # See if the sampleagencyname can help fill in gaps when sampleagencycode is blank
# table(AgSC$sampleagencyname)                   # Answer: nope


## Create a dataframe that can be used for the "Locations" tab in the CEDEN field template
LcnsTox <- ToxSamples2 %>%
  transmute(StationCode = StationCode,
            SampleDate = SampleDate,
            ProjectCode = ProjectCode,
            EventCode = EventCode,
            ProtocolCode = ProtocolCode,
            AgencyCode = AgencyCode,
            SampleComments = SampleComments,
            LocationCode = LocationCode,
            GeometryShape = GeometryShape,
            CoordinateNumber = 1,
            ActualLatitude = ifelse(is.na(actual_latitude) | actual_latitude == -88, latitude, actual_latitude),
            ActualLongitude = ifelse(is.na(actual_longitude) | actual_longitude == -88, longitude, actual_longitude),
            Datum = datum,
            CoordinateSource = "NR",
            Elevation = "",
            UnitElevation = "",
            StationDetailVerBy = "",
            StationDetailVerDate = "",
            StationDetailComments = "",
            SiteDate = SiteDate) %>%  # not uploaded to CEDEN, but used to remove dup SiteDate
  filter(!duplicated(SiteDate)) %>%
  mutate(Datum = ifelse(is.na(Datum) | Datum == "NR" | Datum == "", "WGS84", Datum)) %>%  # Just a guess.  Needs to be populated, and WGS84 is a safe bet.
  select(-c(SiteDate))
#write.csv(LcnsTox, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Toxicity_Locations ready for CEDEN checker_061121.csv')






#####--- Get coordinates for sites needing to be added to CEDEN Station tab ---#####
# Work that needs to be done first:
# 1. Find out which sites need coordinates by comparing list of sites to those in Chem & Bug files to be uploaded to CEDEN
#    (the information for these sites may have been gathered already).  The Chem/Bug file may be in:
#    L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\Sites\
# 2. If coordinates haven't already been gathered for the toxicity sites, see if the sites already exist in CEDEN by comparing with
#    a CEDEN station lookup list.  The LUlist may be in:
#    L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\LULists\
# Now:
# 3. For those sites still needing coordinates, follow the code below for the Sites Of Interest (SOI),
#    grabbing coordinates from lustations (or phab metrics table for "actual" coordinates and datum)

require('dplyr')
require('RPostgreSQL')
library ('reshape')
# connect to db
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = '192.168.1.17', port = 5432, dbname = 'smc', user = 'smcread', password = '1969$Harbor')

lustations_query = "select * from sde.lu_stations"                  # Cross-walk
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)


SOI <- c("SMC01559", "SMC00791", "SMC02464", "SMC03234", "SMC10146", "SMC02123", "SMC01523", "LALT503", "SMC05147", "801M12673")
SOI.lustat <- lustations.2[lustations.2$stationid %in% SOI, ]
SOI.lustat <- merge(SOI.lustat, phab.2[,c("masterid", "targetlatitude", "targetlongitude", "actual_latitude", "actual_longitude")],
                    by="masterid", all.x=TRUE)
#
SOI.phab <- phab.2[phab.2$masterid %in% SOI.lustat$masterid, ]  # for "target" and "actual" coordinates, and datum
SOI.phab <- SOI.phab[!duplicated(SOI.phab$stationcode), ]





