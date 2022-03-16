##########################
#
# Purpose is to produce chemistry results file ready for upload to CEDEN
#
# Steps
# 1. Get chemistry data, either: Option#1 from static files from October 2020, or Option#2 dynamically.
#    In either case, the data are filtered for:
#    i) records not already in CEDEN
#    ii) mostly probabilistic, with a handful of relevant non-probabilistic sites
#    iii) record_origin = "SMC"
# 2. Conform variable names to match CEDEN templates
# 3. Add lab batch data
# 4. Conform entry values to match CEDEN lookup lists
# 5. Resolve remaining universal errors (those applicable to any dataset) or those errors unique to this 2009-2019 dataset,
#    saving necessary updates to the CEDEN vocab_request template (revisions intended for a LookUp List)
#
# JBrown September, October 2020
#########################


library(tidyverse)





#####---  GET DATA  ---#####

## Static Files (download these whether Option#1 or Option#2 below is followed)
# CEDEN LookUp lists downloaded 9/22/2020 or 9/24/2020
CEDEN.Anlyt <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/AnalyteLookUp9222020105959.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.SmplTyp <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/SampleTypeLookUp9242020151423.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Method <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/MethodLookUp9242020152937.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Unit <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/UnitLookUp9242020154139.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.QACode <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/QALookUp9242020165539.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.PresName <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/PrepPreservationLookUp9242020171931.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Digest <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/DigestExtractLookUp9242020172244.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Matrix <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/MatrixLookUp9242020152648.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Fraction <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/FractionLookUp924202015397.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Constit <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/ConstituentLookUp1052020103237.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Agency <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/AgencyLookUp928202095222.csv', stringsAsFactors=F, strip.white=TRUE)


##### GET DATA OPTION #1.  Use this option only if "chem_results.df" has already been created.
# If this option is used, download 'chem_results.df' & 'Lchem_batch4' then jump to "Conform With CEDEN" section
# Data downloaded from unified_chemistry October 2020.  Data already filtered for:
# 1. Sites:
#    Probabilistic
#    Relevant non-probabilistic (see "AlsoInclude" below)
#    No CSULB sites
# 2. Samples not already in CEDEN (Referenced ProgramCode = "SCSMC" and SMCRWM" in CEDEN chemistry download 9/18/2020. Nitrogenous compounds)
chem_results.df <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem_Measurements For Upload_092220.csv', stringsAsFactors=F, strip.white=TRUE)
chem_results.df$X <- NULL # Added from exporting as csv
# Chemistry Lab Batch.  Combined tbl_chemistrybatch & legacy_tbl_chemistrybatch
Lchem_batch4    <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/Chem_chembatch_092420.csv', stringsAsFactors=F, strip.white=TRUE)
Lchem_batch4$X <- NULL


##### GET DATA OPTION #2 Recreate chem_results.df & Lab Batch file (Lchem_batch4).  If this option is used, start here & follow to the end.

## CONNECTION INFORMATION - Paul Smith 2May19
require('dplyr')
#require('dbplyr')
require('RPostgreSQL')
library ('reshape')
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = '192.168.1.17', port = 5432, dbname = 'smc', user = 'smcread', password = '1969$Harbor')


lustations_query = "select * from sde.lu_stations"          # Needed for probabilistic designation
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)

Chem_query = "select * from sde.unified_chemistry"          # Chemistry results (took 55 minutes to download, with all the dups & triplicates)
tbl_Chem   = tbl(con, sql(Chem_query))
Chem.1     = as.data.frame(tbl_Chem)

ChemBatch_query = "select * from sde.tbl_chemistrybatch"            # Lab batch info
tbl_ChemBatch   = tbl(con, sql(ChemBatch_query))
chem_batch.df     = as.data.frame(tbl_ChemBatch)

LChemBatch_query = "select * from sde.legacy_tbl_chemistrybatch"    # Lab batch info
tbl_LChemBatch   = tbl(con, sql(LChemBatch_query))
Lchem_batch.df     = as.data.frame(tbl_LChemBatch)

TChem_query = "select * from sde.tbl_chemistryresults"              # tbl_chemistryresults needed for labbatch, analysisdate, expectedvalue
tbl_TChem   = tbl(con, sql(TChem_query))
TChem       = as.data.frame(tbl_TChem)

LTChem_query = "select * from sde.legacy_tbl_chemistryresults"      # legacy_tbl_chemistryresults needed for labbatch, analysisdate, expectedvalue
tbl_LTChem   = tbl(con, sql(LTChem_query))
LTChem       = as.data.frame(tbl_LTChem)

PHab_query = "select * from sde.unified_phab"                           # PHab needed for actual coordinates
tbl_PHab   = tbl(con, sql(PHab_query))
PHab.1       = as.data.frame(tbl_PHab)


## Static Files
# Existing CEDEN chem. Chemistry records from Program "SCSMC" and SMCRWM" with analytename with nitrogenous compounds or total P.
CEDEN.Chem <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/Chem N P_Program SCSMC SMCRWM_091820.csv', stringsAsFactors=F, strip.white=TRUE)
# Need to remove CSULB sites.  List from unified_phab using sampleagencycode = "CSULB" or "CSULB-SEAL".
CSULB <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/CSULB StationCodes_from PHab_092220.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.Stns <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/StationLookUp910202091912.csv', stringsAsFactors=F, strip.white=TRUE)
#Chem.2 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem2_probabilistic_101420.csv', stringsAsFactors=F, strip.white=TRUE)
#Chem.2$X <- NULL








#####--- Identify Measurements (site, date, analyte, replicate, matrix, etc) Needing to be Uploaded ---#####


### List of Non-probabilistic sites that should be included, per email from Rafi 9/22/2020
AlsoInclude <- c("402SNPMCR", "902ARO100", "903FRC", "905BCC", "907BCT", "911LAP", "802SJN851",
                 "ME-VR2", "REF-FC", "REF-TCAS", "SJC-74", "TC-DO")

### List of CSULB samples. Remove all, per email from Rafi 9/22/2020.
CSULB$sampledate <- as.Date(as.character(CSULB$sampledate), "%m/%d/%Y")
CSULB$SiteDate <- paste(CSULB$stationcode, CSULB$sampledate, sep="_")

####  Conform Data
### SMC Data
## Retain only record_origin = "SMC".  Create SiteDate.
Chem.2 <- Chem.1
Chem.2 <- Chem.2[Chem.2$record_origin == "SMC", ]
Chem.2 <- merge(Chem.2, lustations.1[, c("stationid", "latitude", "longitude", "probabilistic")],
                by.x="stationcode", by.y="stationid", all=FALSE)
Chem.2 <- Chem.2[!is.na(Chem.2$probabilistic), ]
Chem.2 <- Chem.2[Chem.2$probabilistic == "true" | Chem.2$stationcode %in% AlsoInclude, ]
Chem.2$result <- as.numeric(Chem.2$result)
Chem.2$mdl    <- as.numeric(Chem.2$mdl)
Chem.2$rl     <- as.numeric(Chem.2$rl)

Chem.2$sampledate <- as.Date(Chem.2$sampledate)
Chem.2$SiteDate <- paste(Chem.2$stationcode, Chem.2$sampledate, sep="_") # Create SiteDate

Chem.2 <- Chem.2[!(Chem.2$SiteDate %in% CSULB$SiteDate), ] # Remove CSULB samples

library(reshape)
Chem.2 <- rename(Chem.2, c(stationcode="StationCode", sampledate="SampleDate"))

Chem.2$labreplicate <- ifelse(is.na(Chem.2$labreplicate), 1, Chem.2$labreplicate)

#### BEGIN Duplicated Record Purge
### Check for duplicated records & renumber replicate if needed (Change LabReplicate number or remove)
#  Hopefully chemistry lab was consistent with analyte naming within each event!

# First correct labreplicate or fieldreplicate = NA
Chem.2$fieldreplicate <- ifelse(is.na(Chem.2$fieldreplicate), 1, Chem.2$fieldreplicate)
Chem.2$labreplicate   <- ifelse(is.na(Chem.2$labreplicate),   1, Chem.2$labreplicate)

# Create variable for unique analysis & another with unique analysis + result
Chem.2$Unq.Current <- paste(
  Chem.2$StationCode,
  Chem.2$SampleDate,
  Chem.2$sampletypecode,
  Chem.2$matrixname,
  Chem.2$methodname,
  Chem.2$fractionname,
  Chem.2$analytename,
  Chem.2$unit,
  Chem.2$fieldreplicate,
  Chem.2$labreplicate,
  sep = "_"
)
Chem.2$Unq.Current.Result <- paste(
  Chem.2$StationCode,
  Chem.2$SampleDate,
  Chem.2$sampletypecode,
  Chem.2$matrixname,
  Chem.2$methodname,
  Chem.2$fractionname,
  Chem.2$analytename,
  Chem.2$unit,
  Chem.2$fieldreplicate,
  Chem.2$labreplicate,
  Chem.2$result,
  sep = "_"
)
Chem.2 <- Chem.2[order(Chem.2$Unq.Current.Result), ]

# Step 1: Remove duplicated Unq.Current.Result
#Same.A <- Chem.2[duplicated(Chem.2$Unq.Current.Result), ]
Chem.2 <- Chem.2[!duplicated(Chem.2$Unq.Current.Result), ]

# Step 2: Revise labreplicate for records with same Unq.Current, but different Unq.Current.Result
#Same.A.Different.Result <- Chem.2[duplicated(Chem.2$Unq.Current), ]
#Same.906M21774.SS <- Chem.2[Chem.2$StationCode == "906M21774" & Chem.2$analytename == "Suspended Solids", ]
#Cr.SMC05165.Chem2 <- Chem.2[Chem.2$StationCode == "SMC05165" & Chem.2$analytename == "Chromium", ]
#Cr.402M00094.Chem2 <- Chem.2[Chem.2$StationCode == "402M00094" & Chem.2$analytename == "Chromium", ]
#SpC.801M12684.Chem2 <- Chem.2[Chem.2$StationCode == "801M12684" & Chem.2$analytename == "SpecificConductivity", ]
#SpC.801M12684.Chem1 <- Chem.1[Chem.1$stationcode == "801M12684" & Chem.1$analytename == "SpecificConductivity", ]
# SMC02563.Chem2 <- Chem.2[Chem.2$StationCode == "SMC02563" & Chem.2$SampleDate == "2010-06-24", ]
# SMC02563.Chem2 <- SMC02563.Chem2[order(SMC02563.Chem2$analytename, SMC02563.Chem2$fieldreplicate, SMC02563.Chem2$labreplicate), ]
# "Phosphate as P" & "Phosphorus as PO4" are both present, and become "Phosphate as P" in the analyte conform step later in script.
#SMC34888.Chem2 <- Chem.2[Chem.2$StationCode == "SMC34888" & Chem.2$SampleDate == "2010-06-24", ]
#SMC34888.Chem2 <- SMC34888.Chem2[order(SMC34888.Chem2$analytename, SMC34888.Chem2$fieldreplicate, SMC34888.Chem2$labreplicate), ]
#SMC00480.Hg <- Chem.2[Chem.2$StationCode == "SMC00480" & Chem.2$SampleDate == "2009-05-13" & Chem.2$analytename == "Mercury", ]

Chem.2$One <- 1
Chem.2 <- Chem.2 %>%
  group_by(Unq.Current) %>%
  mutate(ExtraReps = cumsum(One))
Chem.2$One <- NULL


Chem.2$Unq.Previous <- lag(Chem.2$Unq.Current, 1)
Chem.2$labreplicate <- ifelse(Chem.2$Unq.Current == Chem.2$Unq.Previous, Chem.2$ExtraReps, Chem.2$labreplicate)

Chem.2$Unq.Current         <- NULL
Chem.2$Unq.Current.Result  <- NULL
Chem.2$Unq.Previous        <- NULL
Chem.2$ExtraReps           <- NULL
#### END Duplicate Record Purge



### Format samples that already have chemistry data in CEDEN.  Use this dataframe in next step to filter out SMC samples already in CEDEN.
# Re-Create SiteDate for CEDEN, to conform the date with R not Excel
CEDEN.Chem$SampleDate <- as.Date(as.character(CEDEN.Chem$SampleDate), "%m/%d/%Y")
CEDEN.Chem$SiteDate <- paste(CEDEN.Chem$StationCode, CEDEN.Chem$SampleDate, sep="_")
CEDEN.Chem.2 <- CEDEN.Chem[!duplicated(CEDEN.Chem$SiteDate), ]


###  Identify SMC samples (Site Date) needing to be added (samples not already in CEDEN)
chem_results.df <- Chem.2[!(Chem.2$SiteDate %in% CEDEN.Chem.2$SiteDate), ]
#write.csv(Chem.2, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem2_probabilistic_101420.csv')
#chem_results.df.402.Cr <- chem_results.df[chem_results.df$StationCode == "402M00094" & chem_results.df$analytename == "Chromium", ]

### Add labbatch
TChem2 <- TChem
TChem2 <- TChem2[, c("stationcode", "sampledate", "fieldreplicate", "labreplicate", "sampletypecode",
                     "matrixname", "methodname", "fractionname", "analytename", "labbatch", "labsampleid",
                     "analysisdate", "expectedvalue")]
LTChem2 <- LTChem
LTChem2 <- LTChem2[, c("stationcode", "sampledate", "fieldreplicate", "labreplicate", "sampletypecode",
                       "matrixname", "methodname", "fractionname", "analytename", "labbatch", "labsampleid",
                       "analysisdate", "expectedvalue")]
LTChem3 <- rbind(TChem2, LTChem2)

LTChem3$sampledate   <- as.Date(LTChem3$sampledate)
LTChem3$analysisdate <- as.Date(LTChem3$analysisdate)
LTChem3$Measure <- paste(LTChem3$stationcode, LTChem3$sampledate, LTChem3$sampletypecode,
                       LTChem3$matrixname, LTChem3$methodname, LTChem3$fractionname, LTChem3$analytename, sep="_")
LTChem3 <- LTChem3[!duplicated(LTChem3$Measure), ]
chem_results.df <- chem_results.df[order(chem_results.df$StationCode, chem_results.df$SampleDate, chem_results.df$analytename,
                                         chem_results.df$fieldreplicate, chem_results.df$fractionname, chem_results.df$labreplicate), ]
chem_results.df$Measure <- paste(chem_results.df$StationCode, chem_results.df$SampleDate,
                                 chem_results.df$sampletypecode, chem_results.df$matrixname,
                                 chem_results.df$methodname, chem_results.df$fractionname,
                                 chem_results.df$analytename, sep="_")

chem_results.df <- merge(chem_results.df, LTChem3[, c("Measure", "labbatch", "labsampleid", "analysisdate", "expectedvalue")],
                         by="Measure", all.x=TRUE)
chem_results.df$Measure <- NULL

##PHab for actual lat/long
PHab.2 <- PHab.1
PHab.2$sampledate <- as.Date(PHab.2$sampledate)
PHab.2$SiteDate <- paste(PHab.2$stationcode, PHab.2$sampledate, sep="_")
PHab.3 <- PHab.2[!duplicated(PHab.2$SiteDate), ]
chem_results.df <- merge(chem_results.df, PHab.3[, c("SiteDate", "stationname", "sampleagencycode", 
                                                     "datum", "actual_latitude", "actual_longitude")],
                         by="SiteDate", all.x=TRUE)

chem_results.df$ActualLatitude <- ifelse(!is.na(chem_results.df$actual_latitude) & chem_results.df$actual_latitude != -88,
                                         chem_results.df$actual_latitude, chem_results.df$latitude)
chem_results.df$ActualLongitude <- ifelse(!is.na(chem_results.df$actual_longitude) & chem_results.df$actual_longitude != -88,
                                         chem_results.df$actual_longitude, chem_results.df$longitude)
chem_results.df$actual_latitude  <- NULL
chem_results.df$actual_longitude <- NULL


#write.csv(chem_results.df, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem_Measurements For Upload_092220.csv')

#NoLabBatch       <- chem_results.df[is.na(chem_results.df$labbatch), ]  # Check to see if any records are missing labbatch.  Result: 0
#chem_res_Samples <- chem_results.df[!duplicated(chem_results.df$SiteDate), ]
#chem_res_Sites   <- chem_results.df[!duplicated(chem_results.df$StationCode), ] # For CEDEN "Summary" tab, which has list of all sites assigning to a ProjectCode.
NEWChemSites <- chem_results.df[!(chem_results.df$StationCode %in% CEDEN.Stns$StationCode), ]
NEWChemSites <- NEWChemSites[!duplicated(NEWChemSites$StationCode), ]
NEWChemSites <- NEWChemSites[, c("StationCode", "stationname", "latitude", "longitude", "datum", "ActualLatitude", "ActualLongitude")]
#write.csv(NEWChemSites, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Sites/NewChemSites_100920.csv') # For CEDEN "Station" tab, which is list of sites not already in CEDEN.
NewChemSamples <- chem_results.df[!(chem_results.df$SiteDate %in% CEDEN.Chem$SiteDate), ]
NewChemSamples <- NewChemSamples[!duplicated(NewChemSamples$SiteDate), ]
NewChemSamples <- NewChemSamples[, c("StationCode", "SampleDate", "stationname", "probabilistic",
                                     "sampleagencycode", "datum", "ActualLatitude", "ActualLongitude")]
#write.csv(NewChemSamples, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/NewChemSamples_100920.csv')


######---  Work with Combining lab chemistry batch data  ---#####
# Combine the 2 labbatch files and remove dups
Lchem_batch2 <- Lchem_batch.df
Lchem_batch2$lastchangedate         <- NULL
Lchem_batch2$legacy                 <- NULL
Lchem_batch2$chemistrybatchrecordid <- NULL
Lchem_batch3 <- rbind(Lchem_batch2, chem_batch.df)
Lchem_batch4 <- Lchem_batch3[!duplicated(Lchem_batch3$labbatch), ]
#write.csv(Lchem_batch4, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/Chem_chembatch_100920.csv')

# Why are there labbatch values in tbl_chemistryresults & legacy_tbl_chemistryresults that are not 
#    in tbl_chemistrybatch & legacy_tbl_chemistrybatch?
ResultsNotBatch1 <- setdiff(chem_results.df$labbatch, Lchem_batch4$labbatch) # n=123
ResultsNotBatch2 <- setdiff(LTChem$labbatch, Lchem_batch4$labbatch)          # n=231         
ResultsNotBatch2b <- setdiff(Lchem_batch4$labbatch, LTChem$labbatch)         # n=2,053
#write.csv(ResultsNotBatch1, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/Labbatch in chemresult but not_chembatch_092420.csv')



############################################
############################################ 
#####---  Conform With CEDEN  ---#####

# Whether GET DATA Option#1 or Option#2 was used, continue here


# Format chem_results.df & Lchem_batch4 (especially important if data were imported as csv)
chem_results.df$SampleDate <- as.Date(chem_results.df$SampleDate, "%m/%d/%Y")
chem_results.df$labbatch <- as.character(chem_results.df$labbatch)
Lchem_batch4$labbatch <- as.character(Lchem_batch4$labbatch)



#### Conform Variables

CEDEN_chem_labbatch<-Lchem_batch4 %>%
  filter(labbatch %in% chem_results.df$labbatch) %>%
  transmute(LabBatch=labbatch,
            LabAgencyCode=labagencycode,
            LabSubmissionCode=labsubmissioncode,
            BatchVerificationCode="NR",
            SubmittingAgencyCode=submittingagencycode,
            LabBatchComments=labbatchcomments
            )

# Check SubmittingAgencyCode
SubAgencyToRevise <- CEDEN_chem_labbatch[!(CEDEN_chem_labbatch$SubmittingAgencyCode %in% CEDEN.Agency$AgencyCode), ]
SubAgencyToRevise <- SubAgencyToRevise[!duplicated(SubAgencyToRevise$SubmittingAgencyCode), ]
SubAgencyToRevise <- SubAgencyToRevise[order(SubAgencyToRevise$SubmittingAgencyCode), ]
CEDEN_chem_labbatch$SubmittingAgencyCode <- ifelse(CEDEN_chem_labbatch$SubmittingAgencyCode == "LASGRWC", "CWH",
                                                   CEDEN_chem_labbatch$SubmittingAgencyCode)
CEDEN_chem_labbatch$SubmittingAgencyCode <- ifelse(CEDEN_chem_labbatch$SubmittingAgencyCode == "OCWMPU", "OCPW",
                                                   CEDEN_chem_labbatch$SubmittingAgencyCode)
CEDEN_chem_labbatch$SubmittingAgencyCode <- ifelse(CEDEN_chem_labbatch$SubmittingAgencyCode == "SBCFCD", "SBCFC",
                                                   CEDEN_chem_labbatch$SubmittingAgencyCode)
# Check LabAgencyCode
LabAgencyToRevise <- CEDEN_chem_labbatch[!(CEDEN_chem_labbatch$LabAgencyCode %in% CEDEN.Agency$AgencyCode), ]
LabAgencyToRevise <- LabAgencyToRevise[!duplicated(LabAgencyToRevise$LabAgencyCode), ]
LabAgencyToRevise <- LabAgencyToRevise[order(LabAgencyToRevise$LabAgencyCode), ]
CEDEN_chem_labbatch$LabAgencyCode <- ifelse(CEDEN_chem_labbatch$LabAgencyCode == "CalScience", "CALSCI",
                                            CEDEN_chem_labbatch$LabAgencyCode)
CEDEN_chem_labbatch$LabAgencyCode <- ifelse(CEDEN_chem_labbatch$LabAgencyCode == "ASL", "AS", CEDEN_chem_labbatch$LabAgencyCode)
CEDEN_chem_labbatch$LabAgencyCode <- ifelse(CEDEN_chem_labbatch$LabAgencyCode == "ES", "Babcock", CEDEN_chem_labbatch$LabAgencyCode)

# Additional revisions found through CEDEN checker
CEDEN_chem_labbatch$LabBatch    <- as.character(CEDEN_chem_labbatch$LabBatch)




CEDEN_chem_chemresults<-chem_results.df %>%
  inner_join(Lchem_batch4 %>%
              select(labbatch, preparationpreservationname, preparationpreservationdate, digestextractmethod)) %>%
  transmute(StationCode=StationCode,
            SampleDate=SampleDate,
            ProjectCode="SMC_SCCWRP", # One project for all
            EventCode="WQ",
            ProtocolCode="SWAMP_2016_WS",
            AgencyCode=sampleagencycode,
            SampleComments=labresultcomments,
            LocationCode="Bank",
            GeometryShape="Point",
            CollectionTime="00:00", #Unfortunately, Excel formats this as time, and 00:00 is required as text.  Must manually revise in xsl file.
            CollectionMethodCode=case_when(matrixname %in% c("samplewater","samplewater, <1.2 um")~"Water_Grab",
                                           matrixname %in% c("sediment")~"Sed_Grab",
                                           matrixname %in% c("labwater","blankwater","blankmatrix")~"Not Applicable",
                                           matrixname %in% c("benthic")~"Algae_SWAMP",
                                           T~"FLAG"), #Probably "Not Applicable"
            SampleTypeCode=sampletypecode,
            Replicate=fieldreplicate,
            CollectionDeviceName="Not Recorded",
            CollectionDepth= -88,
            UnitCollectionDepth="m",
            PositionWaterColumn=case_when(SampleTypeCode %in% c("Grab")~"Subsurface",
                                          SampleTypeCode %in% c("Integrated", "integrated")~"Not Applicable",
                                          T~"FLAG"),
            LabCollectionComments="",
            LabBatch=labbatch,
            AnalysisDate=analysisdate,
            MatrixName=matrixname,
            MethodName=methodname,
            AnalyteName=analytename,
            FractionName=fractionname,
            UnitName=unit,
            LabReplicate=labreplicate,
            Result=result,
            ResQualCode=resqualcode,
            MDL=mdl,	
            RL=rl,
            QACode=qacode,
            ComplianceCode="NR",
            DilutionFactor=dilutionfactor,
            ExpectedValue=expectedvalue,
            PrepPreservationName=preparationpreservationname,
            PrepPreservationDate=preparationpreservationdate,
            DigestExtractMethod=digestextractmethod,
            DigestExtractDate="01/Jan/1950 00:00",
            SampleID="",
            LabSampleID=labsampleid,
            LabResultComments=labresultcomments
          ) %>%
  mutate(AgencyCode = ifelse(is.na(AgencyCode), "SCCWRP", AgencyCode))

CEDEN_chem_chemresults <- CEDEN_chem_chemresults[!is.na(CEDEN_chem_chemresults$StationCode), ]
CEDEN_chem_chemresults$PrepPreservationDate <- as.Date(as.character(CEDEN_chem_chemresults$PrepPreservationDate), "%m/%d/%Y")


#### Conform Analytes

# Identify AnalyteNames in SMC that need to be revised to conform with CEDEN LUList downloaded 9/22/2020
# http://ceden.org/CEDEN_Checker/Checker/DisplayCEDENLookUp.php?List=AnalyteLookUp
#
# AnalyteToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$AnalyteName %in% CEDEN.Anlyt$AnalyteName), ]
# AnalyteToRevise <- AnalyteToRevise[!duplicated(AnalyteToRevise$AnalyteName), ]
# AnalyteToRevise <- AnalyteToRevise[order(AnalyteToRevise$AnalyteName), ]
#
# table(AnalyteToRevise$AnalyteName)
# table(CEDEN.Anlyt$AnalyteName)
#
# P.as.PO4 <- chem_results.df[grepl("TPHOS", chem_results.df$labbatch), ]
#  table(P.as.PO4$analytename)
# PO4.as.P <- chem_results.df[grepl("OPO4", chem_results.df$labbatch), ]
#  table(PO4.as.P$analytename)

## Errors found through checker.
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Phosphorus as PO4" &
                                               grepl("W_TPHOS", CEDEN_chem_chemresults$LabBatch),
                                             "Phosphorus as P", CEDEN_chem_chemresults$AnalyteName)
#
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "% Solids", "Total Solids", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Azinphos ethyl", "Azinphos Ethyl", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Azinphos methyl", "Azinphos Methyl", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Bromophenyl phenyl ether, 4-", "Bromophenyl Phenyl Ether, 4-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Butyl benzyl phthalate", "Butyl Benzyl Phthalate", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Carbon tetrachloride", "Carbon Tetrachloride", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chloroethyl vinyl ether, 2-", "Chloroethyl Vinyl Ether, 2-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophenyl phenyl ether, 4-", "Chlorophenyl Phenyl Ether, 4-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorpyrifos methyl", "Chlorpyrifos Methyl", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Clay <0.0039 mm", "Clay", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Coliform", "Coliform, Total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Crotoxyphos", "Ciodrin", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Cyfluthrin, Total", "Cyfluthrin, total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Cyhalothrin, lambda, total", "Cyhalothrin, Total lambda-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Cyhalothrin, Lambda, total", "Cyhalothrin, Total lambda-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Cypermethrin, total", "Cypermethrin, Total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Danitol", "Fenpropathrin", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Decachlorobiphenyl(Surrogate)", "PCB 209(Surrogate)", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Dementon-O", "Demeton-O", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Demeton-o", "Demeton-O", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Demeton-S", "Demeton-s", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Desulfinyl fipronil", "Fipronil Desulfinyl", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Di-n-butyl phthalate", "Di-n-butyl Phthalate", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Di-n-octyl phthalate", "Di-n-octyl Phthalate", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Dichloropropene, 1,3- (Total)", "Dichloropropene, Total 1,3-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Dimethyl-2-Nitrobenzene, 1,3-(surrogate)", "Dimethyl-2-nitrobenzene, 1,3-(Surrogate)", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Dissolved Solids", "Total Dissolved Solids", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Endosulfan sulfate", "Endosulfan Sulfate", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Esfenvalerate/Fenvalerate, total", "Esfenvalerate/Fenvalerate, Total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Fecal Coliform", "Coliform, Fecal", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Granule + Pebble 2.0 to <64 mm", "Granule + Pebble", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Granule 2.0 to <4.0 mm", "Granule", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Hardness as CaCo3", "Hardness as CaCO3", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "HCH, alpha", "HCH, alpha-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "HCH, beta", "HCH, beta-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "HCH, delta", "HCH, delta-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "HCH, gamma", "HCH, gamma-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Heptachlor epoxide", "Heptachlor Epoxide", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "N-Nitrosodimethylamine", "Nitrosodimethylamine, N-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "N-Nitrosodiphenylamine", "Nitrosodiphenylamine, N-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Nitrate as N03", "Nitrate as NO3", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Nitrogen,Total", "Nitrogen, Total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "OilandGrease", "OilandGrease; HEM", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "orthoPhosphate as P", "OrthoPhosphate as P", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Orthophosphate as P", "OrthoPhosphate as P", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Parathion, methyl", "Parathion, Methyl", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Permethrin-1", "Permethrin, cis-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Permethrin-13C6", "Cypermethrin-13C6(Surrogate)", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Permethrin-2", "Permethrin, trans-", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Permethrin, total", "Permethrin, Total", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Phosphorus as PO4", "Phosphate as P", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Sand 0.0625 to <2.0 mm", "Sand", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Silica", "Silica as SiO2", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Silt  0.0039 to <0.0625 mm", "Silt", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Silt 0.0039 to <0.0625 mm", "Silt", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Sumithrin", "Phenothrin", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "suspended solids", "Total Suspended Solids", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Suspended solids", "Total Suspended Solids", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Suspended Solids", "Total Suspended Solids", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "TCMX(Surrogate)", "Tetrachloro-m-xylene(Surrogate)", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "TEPP", "Tetraethyl pyrophosphate, TEPP", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Total Alkalinity", "Alkalinity as CaCO3", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Total Alkalinity (calc)", "Alkalinity as CaCO3", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Total Hardness", "Hardness as CaCO3", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults$AnalyteName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Triphenyl phosphate(Surrogate)", "Triphenyl Phosphate(Surrogate)", CEDEN_chem_chemresults$AnalyteName)
CEDEN_chem_chemresults             <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$AnalyteName == "Volatile Solids"),] # Nothing similar in CEDEN.  Option#1 drop.  Option#2 request CEDEN addition.

# Remove volatile solids records from LabBatch dataframe
VS <- chem_results.df[chem_results.df$analytename == "Volatile Solids", ]
CEDEN_chem_labbatch <- CEDEN_chem_labbatch[!(CEDEN_chem_labbatch$LabBatch %in% VS$labbatch), ]

# ## AnalyteNames that don't need to be revised:
# AnalyteOk <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$analytename %in% CEDEN.Anlyt$AnalyteName, ]
# AnalyteOk <- AnalyteOk[!duplicated(AnalyteOk$analytename), ]
# AnalyteOk <- AnalyteOk[order(AnalyteOk$analytename), ]


### Conform SampleTypeCode
#table(CEDEN_chem_chemresults$SampleTypeCode)
#table(CEDEN.SmplTyp$SampleTypeCode)
CEDEN_chem_chemresults$SampleTypeCode <- ifelse(CEDEN_chem_chemresults$SampleTypeCode == "integrated", "Integrated",
                                                CEDEN_chem_chemresults$SampleTypeCode)

### Conform Matrix
# MatrixToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$MatrixName %in% CEDEN.Matrix$MatrixName), ]
# MatrixToRevise <- MatrixToRevise[!duplicated(MatrixToRevise$MatrixName), ]
# MatrixToRevise <- MatrixToRevise[order(MatrixToRevise$MatrixName), ]
#table(CEDEN_chem_chemresults$Matrix)
#table(CEDEN.Matrix$MatrixName)


### Conform MethodName
# Identify MethodNames in SMC that need to be revised to conform with CEDEN LUList downloaded 9/24/2020
#
# MethodToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$MethodName %in% CEDEN.Method$MethodName), ]
# MethodToRevise <- MethodToRevise[!duplicated(MethodToRevise$MethodName), ]
# MethodToRevise <- MethodToRevise[order(MethodToRevise$MethodName), ]
#
# table(MethodToRevise$MethodName)
# table(CEDEN.Method$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "Calculated Value", "Calculated", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 1631Em", "EPA 1631EM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 3112B", "SM 3112 B", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 625mNCI", "EPA 625M-NCI", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 8270C-SIM", "EPA 8270C SIM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 8270C NCI", "EPA 8270C_NCI", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "GC/MS NCI-SIM", "GCMS-NCI-SIM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "PCB Congener Screen Extended List_W", "EPA 8082", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "PCB Congener Screen_W", "EPA 8082", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "Pyrethroid Pesticides by GC/MS/MS", "WPCL SOP 67", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "pyrethroids_Water GCMS-SIM", "EPA 8270 SIM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "Pyrethroids_Water GCMS-SIM", "EPA 8270 SIM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 10300 c (m)", "SM 10300 CM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 10300 C (m)", "SM 10300 CM", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 2320B", "SM 2320 B", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 4500-N", "SM 4500-N A", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 4500-P B.5 E", "SM 4500-P BE", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 4500-SO4", "SM 4500-SO4 E", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "SM 4500 NH3 D", "SM 4500-NH3 D", CEDEN_chem_chemresults$MethodName)


### Conform Fraction
# FractionToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$FractionName %in% CEDEN.Fraction$FractionName), ]
# FractionToRevise <- FractionToRevise[!duplicated(FractionToRevise$FractionName), ]
# FractionToRevise <- FractionToRevise[order(FractionToRevise$FractionName), ]
#table(CEDEN_chem_chemresults$Fraction)
#table(CEDEN.Fraction$FractionName)


### Conform Unit
# Identify UnitNames in SMC that need to be revised to conform with CEDEN LUList downloaded 9/24/2020
#
# UnitToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$UnitName %in% CEDEN.Unit$UnitName), ]
# UnitToRevise <- UnitToRevise[!duplicated(UnitToRevise$UnitName), ]
# UnitToRevise <- UnitToRevise[order(UnitToRevise$UnitName), ]
#
# table(UnitToRevise$UnitName)
# table(CEDEN.Unit$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "% Recovery", "% recovery", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "mg/cmÂ²",  "mg/cm2", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "mg/cm²",  "mg/cm2", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "mg/l", "mg/L", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "mg/L ", "mg/L", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "None", "none", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "ug/cmÂ²", "ug/cm2", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "ug/cm²", "ug/cm2", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "ug/kg dw", "ug/Kg dw", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "ug/kg ww", "ug/Kg ww", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$UnitName == "ug/l", "ug/L", CEDEN_chem_chemresults$UnitName)


### Conform ResQualCode
CEDEN_chem_chemresults$ResQualCode <- ifelse(CEDEN_chem_chemresults$ResQualCode == "PR", "P", CEDEN_chem_chemresults$ResQualCode)


### Conform QACode
#Identify QACode in SMC that need to be revised to conform with CEDEN LUList downloaded 9/24/2020
#
QACodeToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$QACode %in% CEDEN.QACode$QACode), ]
QACodeToRevise <- QACodeToRevise[!duplicated(QACodeToRevise$QACode), ]
QACodeToRevise <- QACodeToRevise[order(QACodeToRevise$QACode), ]
#
# table(QACodeToRevise$QACode)
# table(CEDEN.QACode$QACode)
CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "AN-IP,J", "J", CEDEN_chem_chemresults$QACode) #No AN
CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "BRK", "LST", CEDEN_chem_chemresults$QACode)
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "BZ,D", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "CE,IL", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "CS,IL", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "CS,J", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "CT,GB", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DF", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DF,DG", "", CEDEN_chem_chemresults$QACode) #all exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DF,DG,GN", "", CEDEN_chem_chemresults$QACode) #all exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DF,J", "", CEDEN_chem_chemresults$QACode) #all exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DG", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,DG,J", "", CEDEN_chem_chemresults$QACode) #all exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,H", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,HT", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "D,J", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "DF,GN", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "DF,J", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "GN,DB", "DB,GN", CEDEN_chem_chemresults$QACode) #Need to reorder???
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "GN,M", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "H,J", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "IP,J", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "J,SLM", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
#CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "J,VIP", "", CEDEN_chem_chemresults$QACode) #both exist in LUList, but not together
CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "none", "None", CEDEN_chem_chemresults$QACode)
CEDEN_chem_chemresults$QACode <- ifelse(CEDEN_chem_chemresults$QACode == "NONE", "None", CEDEN_chem_chemresults$QACode)


### Conform PrepPreservationName
# NOTE: Insufficient info in chemistry data spreadsheet to correctly revise "Acidified".
# It doesn't appear in LUList, but "LabAcidified" and "FieldAcidified" do.
# Unfortunately the field and lab comments aren't helpful in this instance.
# Analytes are nutrients, metals, & general.  What can be assumed?  Defaulted to FieldAcidified for all.
#
# PresNameToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$PrepPreservationName %in% CEDEN.PresName$PrepPreservationName), ]
# PresNameToRevise <- PresNameToRevise[!duplicated(PresNameToRevise$PrepPreservationName), ]
# PresNameToRevise <- PresNameToRevise[order(PresNameToRevise$PrepPreservationName), ]
#
# table(PresNameToRevise$PrepPreservationName)
# table(CEDEN.PresName$PrepPreservationName)
CEDEN_chem_chemresults$PrepPreservationName <- ifelse(CEDEN_chem_chemresults$PrepPreservationName == "none", "None", CEDEN_chem_chemresults$PrepPreservationName)
CEDEN_chem_chemresults$PrepPreservationName <- ifelse(CEDEN_chem_chemresults$PrepPreservationName == "Acidified", "FieldAcidified", CEDEN_chem_chemresults$PrepPreservationName)


### Conform DigestExtractMethod
# DigestNameToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$DigestExtractMethod %in% CEDEN.Digest$DigestExtractMethod), ]
# DigestNameToRevise <- DigestNameToRevise[!duplicated(DigestNameToRevise$DigestExtractMethod), ]
# DigestNameToRevise <- DigestNameToRevise[order(DigestNameToRevise$DigestExtractMethod), ]
# 
# table(DigestNameToRevise$DigestExtractMethod)
# table(CEDEN.Digest$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "EPA 300.0 ", "EPA 300.0", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "epa 3510C", "EPA 3510C", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "EPA 3545A/ASE", "EPA 3545A", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "KCEL SOP 469", "Not Recorded", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "none", "None", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "not recorded", "Not Recorded", CEDEN_chem_chemresults$DigestExtractMethod)
CEDEN_chem_chemresults$DigestExtractMethod <- ifelse(CEDEN_chem_chemresults$DigestExtractMethod == "not Recorded", "Not Recorded", CEDEN_chem_chemresults$DigestExtractMethod)



#### Constituent
# In CEDEN, 'constituent' = MethodName, MatrixName, AnalyteName, FractionName, UnitName
CEDEN.Constit$Constituent <- paste(CEDEN.Constit$MethodName, CEDEN.Constit$MatrixName, CEDEN.Constit$AnalyteName,
                                   CEDEN.Constit$FractionName, CEDEN.Constit$UnitName, sep="_")
CEDEN_chem_chemresults$Constituent <- paste(CEDEN_chem_chemresults$MethodName, CEDEN_chem_chemresults$MatrixName,
                                            CEDEN_chem_chemresults$AnalyteName, CEDEN_chem_chemresults$FractionName,
                                            CEDEN_chem_chemresults$UnitName, sep="_")
ConstituentToRevise <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$Constituent %in% CEDEN.Constit$Constituent), ]
ConstituentToRevise <- ConstituentToRevise[!duplicated(ConstituentToRevise$Constituent), ]
ConstituentToRevise <- ConstituentToRevise[order(ConstituentToRevise$Constituent), ]
#write.csv(ConstituentToRevise, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/ConstituentToRevise_092420.csv')
# Need to use Constituent LookUp List to determine which part of the combination needs to be changed, or request as a new Constituent
CEDEN_chem_chemresults$FractionName <- ifelse((CEDEN_chem_chemresults$MethodName == "EPA 8270C" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270C_NCI") &
                                                CEDEN_chem_chemresults$FractionName == "Not Recorded",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse((CEDEN_chem_chemresults$MethodName == "SM 2340 B" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 200.7" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 200.8" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 300.0" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 350.1" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 353.2" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 365.1" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 365.3" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 525.2" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 608" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 624" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8082" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8141B" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270 SIM" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270C SIM" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270C" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270D_NCI" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 9060" |
                                                 CEDEN_chem_chemresults$MethodName == "GCMS-NCI-SIM" |
                                                 CEDEN_chem_chemresults$MethodName == "KCEL SOP 440" |
                                                 CEDEN_chem_chemresults$MethodName == "KCEL SOP 469" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 10200 H-2b" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-NH3 F" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-NH3 H" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-NO2 B" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-NO3 E" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-P BE" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-P E" |
                                                 CEDEN_chem_chemresults$MethodName == "SM 4500-Si D") &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse((CEDEN_chem_chemresults$MethodName == "EPA 8270C_NCI" |
                                                 CEDEN_chem_chemresults$MethodName == "EPA 8270C") &
                                                CEDEN_chem_chemresults$FractionName == "Not Recorded",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$MethodName == "EPA 353.2" &
                                                CEDEN_chem_chemresults$MatrixName == "sediment" &
                                                CEDEN_chem_chemresults$FractionName == "Dissolved",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$UnitName <- ifelse((CEDEN_chem_chemresults$MethodName == "EPA 625" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 525.2" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 625" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8082" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8270 SIM" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8270" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8270C SIM" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8270C" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 8270D_NCI" |
                                             CEDEN_chem_chemresults$MethodName == "EPA 9060" |
                                             CEDEN_chem_chemresults$MethodName == "GCMS-NCI-SIM")  &
                                            grepl("urrogate", CEDEN_chem_chemresults$AnalyteName) &
                                            CEDEN_chem_chemresults$UnitName == "%",
                                              "% recovery", CEDEN_chem_chemresults$UnitName)
# begin Chlorophyll a revisions
CEDEN_chem_chemresults$Result <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                          CEDEN_chem_chemresults$UnitName == "ug/cm2" &
                                          CEDEN_chem_chemresults$Result != -88,
                                        CEDEN_chem_chemresults$Result * 10, CEDEN_chem_chemresults$Result)
CEDEN_chem_chemresults$MDL <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                       CEDEN_chem_chemresults$UnitName == "ug/cm2" &
                                       CEDEN_chem_chemresults$MD != -88,
                                        CEDEN_chem_chemresults$MDL * 10, CEDEN_chem_chemresults$MDL)
CEDEN_chem_chemresults$RL <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                      CEDEN_chem_chemresults$UnitName == "ug/cm2" &
                                      CEDEN_chem_chemresults$RL != -88,
                                     CEDEN_chem_chemresults$RL * 10, CEDEN_chem_chemresults$RL)
CEDEN_chem_chemresults$MatrixName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                              CEDEN_chem_chemresults$UnitName == "ug/cm2" &
                                              (CEDEN_chem_chemresults$MatrixName == "samplewater" |
                                                 CEDEN_chem_chemresults$MatrixName == "samplewater, <1.2 um"),
                                            "benthic", CEDEN_chem_chemresults$MatrixName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                                (CEDEN_chem_chemresults$UnitName == "ug/cm2" |
                                                   CEDEN_chem_chemresults$UnitName == "ug/L" |
                                                   CEDEN_chem_chemresults$UnitName == "mg/cm3") &
                                                (CEDEN_chem_chemresults$FractionName == "Total" |
                                                   CEDEN_chem_chemresults$FractionName == "None"),
                                              "Particulate", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                                CEDEN_chem_chemresults$UnitName == "ug/cm2",
                                              "mg/m2", CEDEN_chem_chemresults$UnitName)
# Chl SM 10200 H-2b
CEDEN_chem_chemresults$Result <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                          CEDEN_chem_chemresults$UnitName == "mg/cm3" &
                                          CEDEN_chem_chemresults$Result != -88,
                                        CEDEN_chem_chemresults$Result * 1000, CEDEN_chem_chemresults$Result)
CEDEN_chem_chemresults$MDL <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                       CEDEN_chem_chemresults$UnitName == "mg/cm3" &
                                       CEDEN_chem_chemresults$MD != -88,
                                     CEDEN_chem_chemresults$MDL * 1000, CEDEN_chem_chemresults$MDL)
CEDEN_chem_chemresults$RL <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                      CEDEN_chem_chemresults$UnitName == "mg/cm3" &
                                      CEDEN_chem_chemresults$RL != -88,
                                    CEDEN_chem_chemresults$RL * 1000, CEDEN_chem_chemresults$RL)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                                (CEDEN_chem_chemresults$UnitName == "ug/cm2" |
                                                   CEDEN_chem_chemresults$UnitName == "ug/L" |
                                                   CEDEN_chem_chemresults$UnitName == "mg/cm3") &
                                                (CEDEN_chem_chemresults$FractionName == "Total" |
                                                   CEDEN_chem_chemresults$FractionName == "None"),
                                              "Particulate", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Chlorophyll a" &
                                            CEDEN_chem_chemresults$UnitName == "mg/cm3",
                                          "mg/L", CEDEN_chem_chemresults$UnitName)
# end of Chlorophyll a revisions
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Ash Free Dry Mass" &
                                                CEDEN_chem_chemresults$UnitName == "mg/cm2" &
                                                (CEDEN_chem_chemresults$MatrixName =="samplewater, <1.2 um" |
                                                   CEDEN_chem_chemresults$MatrixName == "benthic") &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                CEDEN_chem_chemresults$FractionName == "Total"),
                                              "Particulate", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Ash Free Dry Mass" &
                                                CEDEN_chem_chemresults$UnitName == "mg/L" &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                              "Particulate", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "Laser" &
                                              CEDEN_chem_chemresults$UnitName == "%",
                                            "SM 2560 D", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$MethodName <- ifelse(CEDEN_chem_chemresults$MethodName == "None" &
                                              (CEDEN_chem_chemresults$AnalyteName == "Nitrogen, Total" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Nitrate + Nitrite as N"),
                                            "EPA 300.0", CEDEN_chem_chemresults$MethodName)
CEDEN_chem_chemresults$FractionName <- ifelse((CEDEN_chem_chemresults$AnalyteName == "Hardness as CaCO3" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Alkalinity as CaCO3" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Bicarbonate" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Carbonate" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Hydroxide" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Total Suspended Solids" |
                                                 CEDEN_chem_chemresults$AnalyteName == "Total Organic Carbon" |
                                                 CEDEN_chem_chemresults$AnalyteName == "MBAS") &
                                                CEDEN_chem_chemresults$UnitName == "mg/L" &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                            "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Total Solids" &
                                                CEDEN_chem_chemresults$UnitName == "%" &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Total Organic Carbon" &
                                                CEDEN_chem_chemresults$UnitName == "% dw" &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                              "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse((CEDEN_chem_chemresults$AnalyteName == "Total Dissolved Solids" |
                                                CEDEN_chem_chemresults$AnalyteName == "Dissolved Organic Carbon") &
                                                CEDEN_chem_chemresults$UnitName == "mg/L" &
                                                CEDEN_chem_chemresults$FractionName == "None",
                                              "Dissolved", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$MethodName == "Not Recorded" &
                                                CEDEN_chem_chemresults$FractionName == "None" &
                                                CEDEN_chem_chemresults$AnalyteName == "OilandGrease; HEM",
                                            "Total", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Clay" &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                   CEDEN_chem_chemresults$FractionName == "Total"),
                                              "<0.0039 mm", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Granule" &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                   CEDEN_chem_chemresults$FractionName == "Total"),
                                              "2.0 to <4.0 mm", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Sand" &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                   CEDEN_chem_chemresults$FractionName == "Total"),
                                              "0.0625 to <2.0 mm", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Silt" &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                   CEDEN_chem_chemresults$FractionName == "Total"),
                                              "0.0039 to <0.0625 mm", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$UnitName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "pH" &
                                            CEDEN_chem_chemresults$UnitName == "NR",
                                          "none", CEDEN_chem_chemresults$UnitName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "pH" &
                                                (CEDEN_chem_chemresults$FractionName == "Total" |
                                                   CEDEN_chem_chemresults$FractionName == "Not Recorded") &
                                                   CEDEN_chem_chemresults$MethodName == "SM 4500-H+ B",
                                              "None", CEDEN_chem_chemresults$FractionName)
CEDEN_chem_chemresults$FractionName <- ifelse(CEDEN_chem_chemresults$AnalyteName == "Nitrogen, Total" &
                                                (CEDEN_chem_chemresults$FractionName == "None" |
                                                   CEDEN_chem_chemresults$FractionName == "Not Recorded") &
                                                CEDEN_chem_chemresults$MethodName == "SM 4500-N A",
                                              "Total", CEDEN_chem_chemresults$FractionName)

CEDEN_chem_chemresults$Constituent <- NULL #No longer needed



#### Additional revisions found through CEDEN checker

DefaultDate <- as.Date("1950-01-01", "%Y-%m-%d")
CEDEN_chem_chemresults$PrepPreservationDate <- ifelse(CEDEN_chem_chemresults$PrepPreservationName == "Not Recorded" |
                                                        CEDEN_chem_chemresults$PrepPreservationName == "None", DefaultDate,
                                                      CEDEN_chem_chemresults$PrepPreservationDate)
CEDEN_chem_chemresults$PrepPreservationDate <- as.Date(CEDEN_chem_chemresults$PrepPreservationDate)
CEDEN_chem_chemresults$AgencyCode <- ifelse(CEDEN_chem_chemresults$AgencyCode == "CalScience", "CALSCI",
                                                      CEDEN_chem_chemresults$AgencyCode)
CEDEN_chem_labbatch$LabAgencyCode <- ifelse(CEDEN_chem_labbatch$LabAgencyCode == "CalScience", "CALSCI",
                                         CEDEN_chem_labbatch$LabAgencyCode)
CEDEN_chem_chemresults$LabBatch <- as.character(CEDEN_chem_chemresults$LabBatch)
CEDEN_chem_labbatch$LabBatch    <- as.character(CEDEN_chem_labbatch$LabBatch)
CEDEN_chem_chemresults$CollectionTime <- as.character(CEDEN_chem_chemresults$CollectionTime) # Unfortunately this gets formatted as time in Excel.  Must make a manual change to text in xls file.
CEDEN_chem_chemresults <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$AnalyteName == "None"), ]

#Use SampleDate if PreservationDate is recorded as earlier
#CEDEN_chem_chemresults$SmpDat99 <- as.Date(as.character(CEDEN_chem_chemresults$SampleDate), "%m/%d/%Y")
#CEDEN_chem_chemresults$PresDat99 <- as.Date(as.character(CEDEN_chem_chemresults$PrepPreservationDate), "%m/%d/%Y")
CEDEN_chem_chemresults$SmpDat99 <- CEDEN_chem_chemresults$SampleDate
CEDEN_chem_chemresults$PresDat99 <- CEDEN_chem_chemresults$PrepPreservationDate
CEDEN_chem_chemresults$PresDat99 <- ifelse(is.na(CEDEN_chem_chemresults$PresDat99), DefaultDate, CEDEN_chem_chemresults$PresDat99)
library("zoo")
CEDEN_chem_chemresults$PresDat99 <- as.Date(CEDEN_chem_chemresults$PresDat99)
#NPD <- CEDEN_chem_chemresults[is.na(CEDEN_chem_chemresults$PresDat99), ]
# TimeTravel <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$PresDat99 < CEDEN_chem_chemresults$SmpDat99 &
#                                       CEDEN_chem_chemresults$PresDat99 != DefaultDate, ]
CEDEN_chem_chemresults$PrepPreservationDate <- ifelse(CEDEN_chem_chemresults$PresDat99 < CEDEN_chem_chemresults$SmpDat99 &
                                                        CEDEN_chem_chemresults$PresDat99 != DefaultDate,
                                                      CEDEN_chem_chemresults$SampleDate, CEDEN_chem_chemresults$PrepPreservationDate)
#library("zoo")
CEDEN_chem_chemresults$PrepPreservationDate <- as.Date(CEDEN_chem_chemresults$PrepPreservationDate)
CEDEN_chem_chemresults$SmpDat99 <- NULL
CEDEN_chem_chemresults$PresDat99 <- NULL



# ### Check for dups & renumber replicate if  needed.  Should have been resolved earlier in script.
# # Unique for dups in CEDEN: Columns A-C, F, H, J-O and U-Z
# CEDEN_chem_chemresults$Unq.Current <- paste(CEDEN_chem_chemresults$StationCode,
#                                      CEDEN_chem_chemresults$SampleDate,
#                                      CEDEN_chem_chemresults$ProjectCode,
#                                      CEDEN_chem_chemresults$AgencyCode,
#                                      CEDEN_chem_chemresults$LocationCode,
#                                      CEDEN_chem_chemresults$CollectionTime,
#                                      CEDEN_chem_chemresults$CollectionMethodCode,
#                                      CEDEN_chem_chemresults$SampleTypeCode,
#                                      CEDEN_chem_chemresults$Replicate,
#                                      CEDEN_chem_chemresults$CollectionDeviceName,
#                                      CEDEN_chem_chemresults$CollectionDepth,
#                                      CEDEN_chem_chemresults$MatrixName,
#                                      CEDEN_chem_chemresults$MethodName,
#                                      CEDEN_chem_chemresults$AnalyteName,
#                                      CEDEN_chem_chemresults$FractionName,
#                                      CEDEN_chem_chemresults$UnitName,
#                                      CEDEN_chem_chemresults$LabReplicate, sep="_")
# CEDEN_chem_chemresults <- CEDEN_chem_chemresults[order(CEDEN_chem_chemresults$Unq.Current), ]
# CEDEN_chem_chemresults$Unq.Previous <- lag(CEDEN_chem_chemresults$Unq.Current, 1)
# CEDEN_chem_chemresults$Unq.Next <- lead(CEDEN_chem_chemresults$Unq.Current, 1)
# 
# CEDEN_chem_chemresults$One <- 1
# CEDEN_chem_chemresults <- CEDEN_chem_chemresults %>%
#   group_by(Unq.Current) %>%
#   mutate(ExtraReps = cumsum(One))
# CEDEN_chem_chemresults$One <- NULL
# 
# CEDEN_chem_chemresults$DropMe <- ifelse((CEDEN_chem_chemresults$Unq.Current == CEDEN_chem_chemresults$Unq.Previous &
#                                           CEDEN_chem_chemresults$Unq.Current == CEDEN_chem_chemresults$Unq.Next) |
#                                           (CEDEN_chem_chemresults$Unq.Current == CEDEN_chem_chemresults$Unq.Previous &
#                                              CEDEN_chem_chemresults$ExtraReps > 2),
#                                         "DROP", "keeper")
# 
# #DropReps <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$DropMe == "DROP", ]
# #write.csv(DropReps, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem_Duplicates_100920.csv')
# #table(CEDEN_chem_chemresults$DropMe)
# #CrSMC05165 <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$StationCode == "SMC05165" & CEDEN_chem_chemresults$AnalyteName == "Chromium", ]
# #Cr402M00094 <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$StationCode == "402M00094" & CEDEN_chem_chemresults$AnalyteName == "Chromium", ]
# #Cr402M00094_2 <- chem_results.df[chem_results.df$StationCode == "402M00094" & chem_results.df$analytename == "Chromium", ]
# #SpC.801M12684.Final <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$StationCode == "801M12684" & CEDEN_chem_chemresults$AnalyteName == "SpecificConductivity", ]
# #SpC.801M12684.chemresults <- chem_results.df[chem_results.df$StationCode == "801M12684" & chem_results.df$analytename == "SpecificConductivity", ]
# 
# CEDEN_chem_chemresults$LabReplicate <- CEDEN_chem_chemresults$ExtraReps
# CEDEN_chem_chemresults <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$DropMe != "DROP", ]
# 
# CEDEN_chem_chemresults$Unq.Current     <- NULL
# CEDEN_chem_chemresults$Unq.Previous    <- NULL
# CEDEN_chem_chemresults$Unq.Next        <- NULL
# CEDEN_chem_chemresults$ExtraReps       <- NULL
# CEDEN_chem_chemresults$DropMe          <- NULL
# #END Duplicate check



## Remove LabQA or FieldQA
Blanks.To.Ditch <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$MatrixName == "blankmatrix" &
                                                     CEDEN_chem_chemresults$CollectionMethodCode == "Not Applicable", ]
CEDEN_chem_labbatch <- CEDEN_chem_labbatch[!(CEDEN_chem_labbatch$LabBatch %in% Blanks.To.Ditch$LabBatch), ]
CEDEN_chem_chemresults <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$MatrixName == "blankmatrix" &
                                              CEDEN_chem_chemresults$CollectionMethodCode == "Not Applicable"), ]

## MDL or RL = NA
#MDL.Check <- CEDEN_chem_chemresults[is.na(CEDEN_chem_chemresults$MDL), ]
# SMC00288.MDL <- CEDEN_chem_chemresults[CEDEN_chem_chemresults$StationCode == "SMC00288" &
#                                         CEDEN_chem_chemresults$SampleDate == "2009-06-18" &
#                                         CEDEN_chem_chemresults$AnalyteName == "Mercury", ]
CEDEN_chem_chemresults$MDL <- ifelse(is.na(CEDEN_chem_chemresults$MDL), -88, CEDEN_chem_chemresults$MDL) #could've gone with = RL?
CEDEN_chem_chemresults$RL <- ifelse(is.na(CEDEN_chem_chemresults$RL), -88, CEDEN_chem_chemresults$RL) #could've gone with = MDL?

## MDL > RL
CEDEN_chem_chemresults$MDL <- ifelse(CEDEN_chem_chemresults$Result > CEDEN_chem_chemresults$MDL &
                                       !is.na(CEDEN_chem_chemresults$ResQualCode) &
                                       CEDEN_chem_chemresults$ResQualCode == "DNQ", -88,
                                     ifelse(CEDEN_chem_chemresults$MDL > CEDEN_chem_chemresults$RL,
                                     CEDEN_chem_chemresults$RL, CEDEN_chem_chemresults$MDL))
## DNQ when Result=MDL
CEDEN_chem_chemresults$ResQualCode <- ifelse(!is.na(CEDEN_chem_chemresults$ResQualCode) &
                                               CEDEN_chem_chemresults$ResQualCode == "DNQ" &
                                               CEDEN_chem_chemresults$Result >= CEDEN_chem_chemresults$MDL,
                                             "=", CEDEN_chem_chemresults$ResQualCode)
## DNQ when Result < MDL
CEDEN_chem_chemresults$MDL <- ifelse(!is.na(CEDEN_chem_chemresults$ResQualCode) &
                                       CEDEN_chem_chemresults$ResQualCode == "DNQ" &
                                       CEDEN_chem_chemresults$Result < CEDEN_chem_chemresults$MDL,
                                     CEDEN_chem_chemresults$Result, CEDEN_chem_chemresults$MDL)
## MDL negative, but not -88
CEDEN_chem_chemresults$MDL <- ifelse(CEDEN_chem_chemresults$MDL < 0 , -88, CEDEN_chem_chemresults$MDL)



## LabBatch too long (longer than 35 characters)
#First, solve issue unique to 2009-2020 dataset.  Can't automatically truncate when the unique character is last in string > 35.
CEDEN_chem_chemresults$LabBatch <- ifelse(CEDEN_chem_chemresults$LabBatch == "BABCOCK_[CALC_A1F2253-01]_W_Not Recorded_2",
                                          "BABCOCK_[CALC_A1F2253-01]_W_NR_2", CEDEN_chem_chemresults$LabBatch)
CEDEN_chem_chemresults$LabBatch <- ifelse(CEDEN_chem_chemresults$LabBatch == "BABCOCK_[CALC_A1F2253-01]_W_Not Recorded",
                                          "BABCOCK_[CALC_A1F2253-01]_W_NR", CEDEN_chem_chemresults$LabBatch)
CEDEN_chem_labbatch$LabBatch <- ifelse(CEDEN_chem_labbatch$LabBatch == "BABCOCK_[CALC_A1F2253-01]_W_Not Recorded_2",
                                          "BABCOCK_[CALC_A1F2253-01]_W_NR_2", CEDEN_chem_labbatch$LabBatch)
CEDEN_chem_labbatch$LabBatch <- ifelse(CEDEN_chem_labbatch$LabBatch == "BABCOCK_[CALC_A1F2253-01]_W_Not Recorded",
                                          "BABCOCK_[CALC_A1F2253-01]_W_NR", CEDEN_chem_labbatch$LabBatch)

CEDEN_chem_chemresults$LabBatch <- as.character(CEDEN_chem_chemresults$LabBatch)
CEDEN_chem_chemresults$LabBatch <- str_trunc(CEDEN_chem_chemresults$LabBatch, 35, "right", ellipsis = "")
#
CEDEN_chem_labbatch$LabBatch <- as.character(CEDEN_chem_labbatch$LabBatch)
CEDEN_chem_labbatch$LabBatch <- str_trunc(CEDEN_chem_labbatch$LabBatch, 35, "right", ellipsis = "")






#####--- Remove LabBatch already in CEDEN.  Identify LabBatch from CEDEN error log ---#####

### Remove LabBatch for those already in CEDEN (with other variables not the same as in CEDEN). From the error log:
# "Generates when the LabBatch, within the LabBatch worksheet, is found within the database and the LabAgencyCode,
#  LabSubmissionCode, BatchVerificationCode, SubmittingAgencyCode, and LabBatchComments, within the LabBatch worksheet,
#  are not equal to the database."

# Note: these changes will be unique to the dataset being submitted, while the changes in the rest of the script apply more universally

# Use error log from CEDEN checker to identify rows in Excel that need to be revised.
# Combine this info with the LabBatch tab to find the LabBatches.

## SET 1 BEGIN (LabBatch 1 to 1500.  Note the chemistry data had to be split into 3 files in order for CEDEN checker to work)
LabBatch_to_revise_1to1500 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_1to1500_101520c.csv', stringsAsFactors=F, strip.white=TRUE)
LabBatch_Errors_1to1500 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_1to1500_101520c_Errors.csv', stringsAsFactors=F, strip.white=TRUE)
#
LabBatch_Errors_1to1500$ExcelRow <- as.integer(LabBatch_Errors_1to1500$ExcelRow)
LabBatch_Errors_1to1500 <- LabBatch_Errors_1to1500[order(LabBatch_Errors_1to1500$ExcelRow), ]
#
LabBatch_to_revise_1to1500$RowNum <- 1:nrow(LabBatch_to_revise_1to1500)+1 # +1 because it's the Excel spreadsheet, not the R dataframe
#
# Get list of LabBatches needing to change, then change directly in CEDEN_chem_chemresults
LB_To_Remove <- LabBatch_to_revise_1to1500[LabBatch_to_revise_1to1500$RowNum %in% LabBatch_Errors_1to1500$ExcelRow, ]
LB_To_Remove_List1 <- LB_To_Remove$LabBatch

## SET 2 BEGIN (LabBatch 1501 to 3000)
LabBatch_to_revise_1501to3000 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_1501to3000_101520c.csv', stringsAsFactors=F, strip.white=TRUE)
LabBatch_Errors_1501to3000 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_1501to3000_101520c_Errors.csv', stringsAsFactors=F, strip.white=TRUE)
#
LabBatch_Errors_1501to3000$ExcelRow <- as.integer(LabBatch_Errors_1501to3000$ExcelRow)
LabBatch_Errors_1501to3000 <- LabBatch_Errors_1501to3000[order(LabBatch_Errors_1501to3000$ExcelRow), ]
#
LabBatch_to_revise_1501to3000$RowNum <- 1:nrow(LabBatch_to_revise_1501to3000)+1
#
# Get list of LabBatches needing to change, then change directly in CEDEN_chem_chemresults
LB_To_Remove <- LabBatch_to_revise_1501to3000[LabBatch_to_revise_1501to3000$RowNum %in% LabBatch_Errors_1501to3000$ExcelRow, ]
LB_To_Remove_List2 <- LB_To_Remove$LabBatch

## SET 3 BEGIN (LabBatch 3000 to End)
LabBatch_to_revise_3000toEnd <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_3000toEnd_101520c.csv', stringsAsFactors=F, strip.white=TRUE)
LabBatch_Errors_3000toEnd <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/ForUpload/Errors/LabBatch_3000toEnd_101520c_Errors.csv', stringsAsFactors=F, strip.white=TRUE)
#
LabBatch_Errors_3000toEnd$ExcelRow <- as.integer(LabBatch_Errors_3000toEnd$ExcelRow)
LabBatch_Errors_3000toEnd <- LabBatch_Errors_3000toEnd[order(LabBatch_Errors_3000toEnd$ExcelRow), ]
#
LabBatch_to_revise_3000toEnd$RowNum <- 1:nrow(LabBatch_to_revise_3000toEnd)+1
#
# Get list of LabBatches needing to change, then change directly in CEDEN_chem_chemresults
LB_To_Remove <- LabBatch_to_revise_3000toEnd[LabBatch_to_revise_3000toEnd$RowNum %in% LabBatch_Errors_3000toEnd$ExcelRow, ]
LB_To_Remove_List3 <- LB_To_Remove$LabBatch


## Combine list of LabBatches to remove
LB_To_Remove_List_All <- c(LB_To_Remove_List1, LB_To_Remove_List2, LB_To_Remove_List3)


## Revise chemresults
CEDEN_chem_chemresults <- CEDEN_chem_chemresults[!(CEDEN_chem_chemresults$LabBatch %in% LB_To_Remove_List_All), ]
# Revise LabBatch file too
CEDEN_chem_labbatch <- CEDEN_chem_labbatch[!(CEDEN_chem_labbatch$LabBatch %in% LB_To_Remove_List_All), ]

#### END Remove LabBatch already in CEDEN



#####---  Save files ready to assemble for CEDEN checker  ---#####
#write.csv(CEDEN_chem_chemresults, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Chem_data ready for CEDEN checker_110421f.csv')
#write.csv(CEDEN_chem_labbatch, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Samples/LabBatch_110421f.csv')




#####--- STEPS TO SUBMIT DATA TO CHECKER  ---#####
# 1.  Find the blank chemistry data template at:
#     L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\Templates\ceden_chem_template_01082019_blank.xls
#     Save a copy of the template as new name, at L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\ForUpload\
# 2.  Copy records from LabBatch file (e.g., LabBatch_101320.csv saved above) to the LabBatch tab in the data template
# 3.  Copy records from Chemistry file (e.g., Chem_data ready for CEDEN checker_101320.csv saved above) to the ChemResults tab in template
# 4.  In LabBatch tab within template, format LabBatch column as text (yes, even though it was saved as a character format in R).
# 5.  In ChemResults tab:
# 5a.   format CollectionTime column as text, then change all to "00:00"
# 5b.   format LabBatch column as text
# 5c.   format DigestExtractDate column as text, then change all to "01/Jan/1950 00:00"
# 6.  If filled out template dataset is large (~7Mb or larger, or has too many errors), the file must be broken up & submitted in separate files.
#     Base the separation on LabBatch.  Sort the Results tab & LabBatch tab by LabBatch the column.
#     Then create a new file using the same subset of LabBatches in the Result & LabBatch tabs.
#     Use the LabBatch tab to determine the number of LabBatches to split up the file (divide by half, thirds, fourths, whatever)
# 7.  Submit file to CEDEN checker (http://ceden.org/CEDEN_checker/Checker/CEDENUpload.php)
#     Crying won't help you, praying won't do you no good.
# 8.  A list of Errors and Warnings will appear in a webpage, and an Excel file emailed to you.
#     Errors are items that must be revised before data are accepted by CEDEN. Warnings are items that would be nice to revise.
#     If a page pops up with "500 - Internal server error", then the file may be too large (or surpassed the maximum
#     number of errors checked by CEDEN at a time).  Split up the file to be checked using step 6 above.
# 9.  Update this R script with universal error corrections, please



