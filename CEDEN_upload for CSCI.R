#########################################
#
# Purpose: Format CSCI data in SMC database for upload to CEDEN.
# This refers to csci index scores & other data found in the analysis_csci_core table.
#
#
# Guidance information from Toni Marshall via Shuka Rastegarpour of SWAMP (yes, even though uploading to CEDEN) in email from Shuka 4/29/2021.
# L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\CSCI support\Guidance for entering CSCI scores to SWAMP habitat template_030321.docx
# L:\SMC Regional Monitoring_ES\SMC_RM\Data\Working\CEDEN Upload\CSCI support\SWAMP_Field_CollectionResults_Template_v2.5_081420.xlsx
# According to the document, 3 tabs need to be completed: Sample, Locations, Habitat Results
# Sample tab, 6 required fields:  StationCode, SampleDate ProjectCode EventCode ProtocolCode AgencyCode
#   Columns A-F should be completed.  (Note: the CEDEN field template from 2019 does not have a 'Sample' tab)
# Locations tab, 8 required fields: StationCode, SampleDate, ProjectCode, EventCode, ProtocolCode, AgencyCode,
#   LocationCode, GeometryShape
#   Columns A-I with the exception of comments.  Comments can be left blank. 
# HabitatResults, 22 required fields: StationCode, SampleDate, ProjectCode, EventCode, ProtocolCode, AgencyCode,
#   LocationCode, GeometryShape, CollectionTime, CollectionMethodCode, Replicate, MatrixName, MethodName, AnalyteName,
#   FractionName, UnitName, Result, ResQualCode, QACode, ComplianceCode, BatchVerificationCode, CollectionDeviceName
#   Columns A-Y - Comments can be left blank and VariableResult should be left blank. 
#
# JBrown
# May 2021
#########################################


library(tidyverse)

## CONNECTION INFORMATION - Paul Smith 2May19
require('dplyr')
#require('dbplyr')
require('RPostgreSQL')
library ('reshape')
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = '192.168.1.17', port = 5432,
                      dbname = 'smc', user = 'smcread', password = '1969$Harbor')


lustations_query = "select * from sde.lu_stations"                  # Needed for probabilistic designation
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)
rm(lustations_query)

csci_query = "select * from analysis_csci_core"                     # CSCI
tbl_csci   = tbl(con, sql(csci_query))
csci.df     = as.data.frame(tbl_csci)
rm(csci_query)

# S1G_query = "select * from analysis_csci_suppl1_grps"               # suppl1_grps
# tbl_S1G   = tbl(con, sql(S1G_query))
# suppl1_grps.df     = as.data.frame(tbl_S1G)
# rm(S1G_query)
# 
# S2M_query = "select * from analysis_csci_suppl2_mmi"                # suppl2_mmi
# tbl_S2M   = tbl(con, sql(S2M_query))
# suppl2_mmi.df     = as.data.frame(tbl_S2M)
# rm(S2M_query)


# Static files
CSULB <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/CSULB StationCodes_from PHab_092220.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.CSCI <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/CSCI_ceden_data_20210505102644.csv', stringsAsFactors=F, strip.white=TRUE)
Sigala <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/Index_Collections_20210506_MarcoSigala.csv', stringsAsFactors=F, strip.white=TRUE)
AgencyLU <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/LULists/AgencyLookUp528202193450.csv')

#####--  Filter SMC sites (Index Scores table)  --#####

# List of Non-probabilistic sites that should be included, per email from Rafi 9/22/2020
AlsoInclude <- c("402SNPMCR", "902ARO100", "903FRC", "905BCC", "907BCT", "911LAP", "802SJN851",
                 "ME-VR2", "REF-FC", "REF-TCAS", "SJC-74", "TC-DO")

# List of CSULB samples to remove (9/22/2020)
CSULB <- CSULB %>%
  mutate(sampledate = as.Date(as.character(sampledate), "%m/%d/%Y")) %>%
  left_join(lustations.1[, c("masterid", "stationid")], by=c("stationcode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, sampledate, sep="_"))

# List of CSCI records already in CEDEN (5/5/2021)
CEDEN.CSCI <- CEDEN.CSCI %>%
  mutate(SampleDate = as.Date(as.character(SampleDate), "%m/%d/%Y")) %>%
  left_join(lustations.1[, c("masterid", "stationid")], by=c("StationCode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, SampleDate, sep="_")) %>%
  filter(!duplicated(SiteDate))

# List of files already in SWAMP (according to Marco Sigala, 5/6/2021). (Extras not identified as record_origin=CEDEN in csci.df?)
Sigala2 <- Sigala %>%
  mutate(SampleDate = as.Date(as.character(SampleDate), "%m/%d/%Y")) %>%
  left_join(lustations.1[, c("masterid", "stationid")], by=c("StationCode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, SampleDate, sep="_")) %>%
  drop_na(CSCI_Exists) %>%         # Drop the NA values.
  filter(!(CSCI_Exists == "")) %>% # Drop blanks.
  arrange(SiteDate, -xtfrm(CSCI_Exists)) %>% # Put "Yes" first in sort.
  filter(!duplicated(SiteDate))
#unique(Sigala2$CSCI_Exists)
#table(Sigala2$CSCI_Exists)
# Note: CSCI_Exists = "No data" or "Not scored" are still retained.
# Per Marco's email (via Rafi via Chad 5/7/2021): "Any record where CSCI_Exists is null indicates I have not calculated or reported CSCI scores."




#### CSCI Index Score table
# Retain only record_origin = "SMC".  Rename sites already in CEDEN under different stationcodes.  Create SiteDate.
#table(csci.df$record_origin)
mydf <- csci.df %>%
  filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic")],
             by = c("stationcode" = "stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(probabilistic == "true" | stationcode %in% AlsoInclude) %>%  # Retain probabilistic sites + bonus sites
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_")) %>%
  filter(!(SiteDate %in% CSULB$SiteDate)) %>%                         # Remove CSULB samples
  filter(!(SiteDate %in% CEDEN.CSCI$SiteDate)) %>%                    # Remove samples already in CEDEN
  filter(!(SiteDate %in% Sigala2$SiteDate)) %>%                       # Remove samples in SWAMP
  dplyr::rename(StationCode=stationcode, SampleDate=sampledate) %>%
  select(-c(objectid, created_user, created_date, last_edited_user, last_edited_date, login_email,
            login_agency, login_owner, login_year, login_project, globalid))
#str(mydf)



# Rename and transpose wide-to-long the CSCI Index Scores & additional analytes in Skuka's Excel file.
mydf2  <- mydf %>%
  dplyr::rename(CSCI_Percentile=csci_percentile, CSCI=csci, CSCI_Mean_O=mean_o, CSCI_Pcnt_Ambiguous_Taxa=pcnt_ambiguous_taxa,
                CSCI_Count=count, CSCI_Number_of_MMI_Iterations=number_of_mmi_iterations,
                CSCI_Number_of_OE_Iterations=number_of_oe_iterations, CSCI_Pcnt_Ambiguous_Individuals=pcnt_ambiguous_individuals,
                CSCI_MMI_Percentile=mmi_percentile, CSCI_E=e, CSCI_MMI=mmi, CSCI_OoverE_Percentile=oovere_percentile,
                CSCI_OoverE=oovere) %>%
  gather(AnalyteName, Result, CSCI_Count:CSCI_Percentile, factor_key=TRUE) #transpose



# #### suppl1_grps table
# # Note, this dataset has no sampledate and no sampleid (site,date,method)
# S1G <- suppl1_grps.df %>%
#   filter(record_origin == "SMC") %>%
#   filter(stationcode %in% mydf$StationCode) %>%
#   select(c(stationcode, pgroup1, pgroup2, pgroup3, pgroup4, pgroup5, pgroup6, pgroup7, pgroup8, pgroup9, pgroup10, pgroup11)) %>%
#   dplyr::rename(StationCode=stationcode, CSCI_ProbGroup1=pgroup1, CSCI_ProbGroup2=pgroup2, CSCI_ProbGroup3=pgroup3,
#                 CSCI_ProbGroup4=pgroup4, CSCI_ProbGroup5=pgroup5, CSCI_ProbGroup6=pgroup6, CSCI_ProbGroup7=pgroup7,
#                 CSCI_ProbGroup8=pgroup8, CSCI_ProbGroup9=pgroup9, CSCI_ProbGroup10=pgroup10, CSCI_ProbGroup11=pgroup11)
# 
# S1G2 <- S1G %>%
#   gather(AnalyteName, Result, CSCI_ProbGroup1:CSCI_ProbGroup11, factor_key=TRUE) # wide-to-long
# # What now?  What data template to use?  How to add it to mydf3?
# 
# 
# 
# #### suppl2_mmi table
# S2M <- suppl2_mmi.df %>%
#   filter(record_origin == "SMC") %>%
#   filter(stationcode %in% mydf$StationCode) %>%
#   select(c(stationcode, sampleid, metric, iteration, value, predicted_value, score))
# 
# S2M2 <- S2M %>%
#   gather(Type, Result, value:score, factor_key=TRUE) %>% # wide-to-long
#   mutate(metric = ifelse(Type == "value", paste("CSCI_",metric,sep=""),
#                          ifelse(Type == "predicted_value", paste("CSCI_",metric,"_predicted",sep=""),
#                                 ifelse(Type == "score", paste("CSCI_",metric,"_score",sep=""), "Oops"))))
# # Need Type?  Need iteration?
# # What now?  Get date from sampleid (merge with csci.df to get date)?  Append to mydf3?  Use another data template?






#####--- Identify stationcode and samples that need to be added to CEDEN through vocab request form process  ---#####
#TBD.  See SMC_BMI_Translator_v2.R for previous strategy.



  

#####---  Conform variables With CEDEN (Index Scores table) ---#####

# table(CEDEN.CSCI$MethodName)
# table(CEDEN.CSCI$CollectionDeviceDescription)
# table(CEDEN.CSCI$CollectionMethodName)


# Create list of analytes associated with different unit designation in CEDEN
PercentAnalyte <- c("CSCI_Pcnt_Ambiguous_Taxa", "CSCI_Pcnt_Ambiguous_Individuals") 
CountAnalyte <- c("CSCI_Count", "CSCI_Number_of_MMI_Iterations", "CSCI_Number_of_OE_Iterations")
NoneAnalyte <- c("CSCI_Percentile", "CSCI", "CSCI_Mean_O", "CSCI_MMI_Percentile", "CSCI_E", "CSCI_MMI",
                 "CSCI_OoverE_Percentile", "CSCI_OoverE")

# Variable name and order according to CEDEN field template 01082019 (http://ceden.org/ceden_datatemplates.shtml)
mydf3 <- mydf2 %>%
  transmute(StationCode=StationCode,
            SampleDate = SampleDate,
            ProjectCode = "SMC_SCCWRP",     # One project for all
            EventCode = "BA",
            ProtocolCode = "SWAMP_2016_WS",
            AgencyCode = "SCCWRP",
            SampleComments = "",
            LocationCode = "X",
            GeometryShape="Point",
            CollectionTime = "00:00", #Unfortunately, Excel formats this as time, and 00:00 is required as text.  Must manually revise in xsl file.
            CollectionMethodCode = "BMI_RWB_MCM",
            Replicate = fieldreplicate,
            CollectionDeviceName = "D-Frame Kick Net",  # The order of this variable is different in SWAMP field results template
            HabitatCollectionComments = "",
            MatrixName = "benthic",  #'benthic' in CEDEN, 'Benthic' in SWAMP.
            MethodName = "CSCI_software_v1.x",
            AnalyteName = AnalyteName,
            FractionName="None",
            UnitName = ifelse(AnalyteName %in% PercentAnalyte, "%",
                              ifelse(AnalyteName %in% CountAnalyte, "count",
                                     ifelse(AnalyteName %in% NoneAnalyte, "none", "Oops"))),
            VariableResult = NA, # NA appears in CEDEN CSCI spreadsheet.
            Result = Result,
            ResQualCode = "=",
            QACode = "None",
            ComplianceCode = "Pend",
            BatchVerificationCode = "NR",
            HabitatResultComments = "",
            SiteDate = SiteDate,   # keep to merge with PHab dataset, but this variable is not uploaded to CEDEN
            latitude = latitude,   # keep for use in Locations tab in template (in case it's missing in PHab file), but variable isn't used in HabitatResults tab
            longitude = longitude) # keep for use in Locations tab in template (in case it's missing in PHab file), but variable isn't used in HabitatResults tab
#table(mydf3$UnitName) # Any "Oops"?
mydf3 <- mydf3 %>%
  mutate(AnalyteName = as.character(AnalyteName)) %>%   # from factor to character
  arrange(StationCode, SampleDate, Replicate, AnalyteName)



#AndHuc <- merge(mydf3, lustations.1[, c("stationid", "huc")], by.x="StationCode", by.y="stationid", all.x=TRUE)
#RB8 <- AndHuc[AndHuc$huc <900 & AndHuc$huc >= 800, ]
#write.csv(RB8, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Misc/RB8_csci.csv')

### Inventory of analytes that might be included in upload, other than CSCI Index Score. (The source of the data is in parentheses)

## Analytes in file from Skuka (Not in CEDEN. Need to submit vocab request to CEDEN for these analytes?)
# email from Rafi on 5/7/2021 says "So, I think we don't put any Suppl2-level results in CEDEN. Just Suppl1."
#
#                   What template is used to submit data?
#                   No sampledate or sampleid (site+date) for suppl1_grps.
#                   sampleid (site+date) for suppl2_mmi, use CSCI template to submit?)
# CSCI_Clinger_PercentTaxa              (suppl2_mmi.df, value)
# CSCI_Clinger_PercentTaxa_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_Clinger_PercentTaxa_score              (suppl2_mmi.df, score)
# CSCI_Coleoptera_PercentTaxa              (suppl2_mmi.df, value)
# CSCI_Coleoptera_PercentTaxa_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_Coleoptera_PercentTaxa_score              (suppl2_mmi.df, score)
# CSCI_Taxonomic_Richness              (suppl2_mmi.df, value)
# CSCI_Taxonomic_Richness_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_Taxonomic_Richness_score              (suppl2_mmi.df, score)
# CSCI_EPT_PercentTaxa              (suppl2_mmi.df, value)
# CSCI_EPT_PercentTaxa_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_EPT_PercentTaxa_score              (suppl2_mmi.df, score)
# CSCI_Shredder_Taxa              (suppl2_mmi.df, value)
# CSCI_Shredder_Taxa_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_Shredder_Taxa_score              (suppl2_mmi.df, score)
# CSCI_Intolerant_Percent              (suppl2_mmi.df, value)
# CSCI_Intolerant_Percent_predicted              (suppl2_mmi.df, predicted_value)
# CSCI_Intolerant_Percent_score              (suppl2_mmi.df, score)
#
# CSCI_ProbGroup1                   (suppl1_grps.df)
# CSCI_ProbGroup2                   (suppl1_grps.df)
# CSCI_ProbGroup3                   (suppl1_grps.df)
# CSCI_ProbGroup4                   (suppl1_grps.df)
# CSCI_ProbGroup5                   (suppl1_grps.df)
# CSCI_ProbGroup6                   (suppl1_grps.df)
# CSCI_ProbGroup7                   (suppl1_grps.df)
# CSCI_ProbGroup8                   (suppl1_grps.df)
# CSCI_ProbGroup9                   (suppl1_grps.df)
# CSCI_ProbGroup10                  (suppl1_grps.df)
# CSCI_ProbGroup11                  (suppl1_grps.df)

## Analytes already in CEDEN (listed is the analyte, unit, and source)
# CSCI_Percentile	                 none   (csci.df)
# CSCI	                           none   (csci.df)
# CSCI_Mean_O	                     none   (csci.df)
# CSCI_Pcnt_Ambiguous_Taxa	       %      (csci.df)
# CSCI_Count	                     count   (csci.df)
# CSCI_Number_of_MMI_Iterations	   count   (csci.df)
# CSCI_Number_of_OE_Iterations	   count   (csci.df)
# CSCI_Pcnt_Ambiguous_Individuals	 %      (csci.df)
# CSCI_MMI_Percentile	             none   (csci.df)
# CSCI_E	                         none   (csci.df)
# CSCI_MMI	                       none   (csci.df)
# CSCI_OoverE_Percentile	         none   (csci.df)
# CSCI_OoverE	                     none   (csci.df)



#####--- Get actual latitude, longitude and AgencyCode) from PHab  ---#####
PHab_query = "select * from sde.unified_phab"                   # Took over half an hour 5/28/2021 8:02-8:38
tbl_PHab   = tbl(con, sql(PHab_query))
PHab.1     = as.data.frame(tbl_PHab)
rm(PHab_query)

PHab.2 <- PHab.1 %>%
  mutate(sampledate = as.Date(sampledate)) %>%
  arrange(stationcode, sampledate, actual_latitude, targetlatitude) %>%
  left_join(lustations.1[, c("stationid", "masterid")], by=c("stationcode" = "stationid")) %>%
  mutate(SiteDate = paste(masterid, sampledate, sep="_"))                                          # Need masterid in mydf2 too
PHab.3 <- PHab.2 %>%
  filter(!duplicated(SiteDate)) %>%
  select(stationcode, sampledate, SiteDate, sampleagencycode, sampleagencyname,
         actual_latitude, actual_longitude, targetlatitude, targetlongitude, datum) %>%
  mutate(actual_latitude = ifelse(is.na(actual_latitude), targetlatitude, actual_latitude)) %>%
  mutate(actual_longitude = ifelse(is.na(actual_longitude), targetlongitude, actual_longitude)) %>%
  filter(SiteDate %in% mydf2$SiteDate)
#rm(PHab.1, PHab.2)

## Add actual field sampling agency, or keep 'SCCWRP'
# table(PHab.3$sampleagencycode)
# table(PHab.3$sampleagencyname)
# table(AgencyLU$AgencyCode)     # See if PHab.3$sampleagencycode needs to be conformed to the CEDEN LUList
mydf4 <- mydf3 %>%
  left_join(PHab.3[, c("SiteDate", "sampleagencycode", "sampleagencyname",
                       "actual_latitude", "actual_longitude", "datum")], by=c("SiteDate" = "SiteDate")) %>%
  mutate(AgencyCode = ifelse(!is.na(sampleagencycode), sampleagencycode, AgencyCode)) %>%
  select(-c(sampleagencycode, sampleagencyname)) # Disable or disregard this line if investigating AgencyCode below
# table(mydf4$AgencyCode)                        # Who's represented?
# AgSC <- mydf4[mydf4$AgencyCode == "SCCWRP", ]  # See if the sampleagencyname can help fill in gaps when sampleagencycode is blank
# table(AgSC$sampleagencyname)                   # Answer: nope


## Create a dataframe that can be used for the "Locations" tab in the CEDEN field template
Lcns1 <- mydf4 %>%
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
  select(-SiteDate)
#write.csv(Lcns1, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/CSCI_Locations ready for CEDEN checker_052821.csv')

## Finalize dataset for HabitatResults tab
mydf5 <- mydf4 %>%
  select(-c(SiteDate, latitude, longitude, actual_latitude, actual_longitude, datum))
#write.csv(mydf5, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/CSCI_data ready for CEDEN checker_052821.csv')





#####--- Summary info.  Not for upload ---#####
require(lubridate)
Samples1 <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/CSCI Samples_072921.csv', stringsAsFactors=F, strip.white=TRUE)
Samples2 <- Samples1 %>%
  mutate(SampleDate = as.Date(SampleDate, format = '%m/%d/%Y')) %>%
  mutate(Year = year(SampleDate)) %>%
  mutate(SiteDate=paste(StationCode, SampleDate, sep="_")) %>%
  left_join(lustations.1, by=c("StationCode"="stationid")) %>%
  filter(!duplicated(SiteDate)) %>%
  mutate(RB = ifelse(huc > 399 & huc < 500, 4,
                     ifelse(huc > 799 & huc < 900, 8,
                            ifelse(huc > 899 & huc < 1000, 9, -88)))) %>%
  mutate(RBName = ifelse(huc > 399 & huc < 500, "Los Angeles",
                     ifelse(huc > 799 & huc < 900, "Santa Ana",
                            ifelse(huc > 899 & huc < 1000, "San Diego", "Outside"))))


Sites1 <- Samples2 %>%
  filter(!duplicated(StationCode))

table(Samples2$smcshed)
table(Samples2$RBName)
table(Sites1$RBName)


library(tidyverse)
library(sf)

smc_sheds_sf<-st_read("C:/Data/Data/SMC/SMCWatersheds/SMCSheds2009.shp") %>%      # SMC watershed outlines
  st_transform(crs=4326) # crs=4326 indicates WGS1984 coordinate system

SamplePoints<-Samples2 %>%
  st_as_sf(coords = c("longitude", "latitude"), # can use numbers here too                      # Indicate which columns have the latitude & longitude
           remove = F,                                                                          # Don't remove these lat/lon cols from df
           crs = 4326)                                                                          # crs=4326 indicates WGS1984 coordinate system
Site1Points<-Sites1 %>%
  st_as_sf(coords = c("longitude", "latitude"), # can use numbers here too                      # Indicate which columns have the latitude & longitude
           remove = F,                                                                          # Don't remove these lat/lon cols from df
           crs = 4326)                                                                          # crs=4326 indicates WGS1984 coordinate system



### Map Site distribution by Regional Board
SamplePoints2 <- SamplePoints
SamplePoints2$RBName <- factor(SamplePoints2$RBName, levels=c("Los Angeles", "Santa Ana", "San Diego"))

NCat1 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "Los Angeles", ])
NCat2 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "Santa Ana", ])
NCat3 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "San Diego", ])
ggplot() +
  theme_bw()+ # white background
  geom_sf(data=smc_sheds_sf)+ # Catchment outlines
  geom_sf(data=SamplePoints2, aes(color=RBName))+
  ggtitle("Distribution by Regional Board") +
  scale_color_manual(drop=FALSE, name="Regional Board",
                    values=c("Los Angeles"="blue", "Santa Ana"="red", "San Diego"="green"),    # manual colors
                    labels=c(paste("Los Angeles (n=", NCat1,")", sep=""),
                             paste("Santa Ana (n=", NCat2,")", sep=""),
                             paste("San Diego (n=", NCat3,")",sep="")),
                    guide=guide_legend(override.aes = list(bg=c("blue", "red", "green"))))
ggsave("L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Site distribution by Regional Board_August2021.png")


### Map Sample Distribution bu Regional Board
SamplePoints2 <- SamplePoints
SamplePoints2$RBName <- factor(SamplePoints2$RBName, levels=c("Los Angeles", "Santa Ana", "San Diego"))

NCat1 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "Los Angeles", ])
NCat2 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "Santa Ana", ])
NCat3 <- nrow(SamplePoints2[!is.na(SamplePoints2$RBName) & SamplePoints2$RBName == "San Diego", ])
ggplot() +
  theme_bw()+ # white background
  geom_sf(data=smc_sheds_sf)+ # Catchment outlines
  geom_sf(data=SamplePoints2, aes(color=RBName))+
  ggtitle("Distribution by Regional Board") +
  scale_color_manual(drop=FALSE, name="Regional Board",
                     values=c("Los Angeles"="blue", "Santa Ana"="red", "San Diego"="green"),    # manual colors
                     labels=c(paste("Los Angeles (n=", NCat1,")", sep=""),
                              paste("Santa Ana (n=", NCat2,")", sep=""),
                              paste("San Diego (n=", NCat3,")",sep="")),
                     guide=guide_legend(override.aes = list(bg=c("blue", "red", "green"))))
ggsave("L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Sample distribution by Regional Board_August2021.png")


#### Histogram of samples per year
ggplot(Samples2, aes(x=Year)) +
  #theme_bw()+ # white background
  geom_histogram(binwidth=1, color="black", fill="cyan")+
  scale_x_continuous(breaks = seq(2005, 2020, by = 1))+  #set minimum, maximum, and interval
  #scale_y_continuous(breaks = seq(0, 90, by = 10))+  #set minimum, maximum, and interval
  scale_y_continuous(expand = c(0,0), breaks = seq(0, 90, by = 10))+
  ylab("Number of Samples")+
  theme(axis.text.x=element_text(size=10, color="black"))+  #this gets the tick labels to rotate 45?, and justifies text.
  theme(axis.text.y=element_text(size=10, color="black"))+  #this adjusts the y-axis text.  Works with Version 1.0.136
  theme(axis.line = element_line(colour = "black"))         # Add axis lines
ggsave("L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Sample frequency_August2021.png")


#### Map of sites excluded
# Need to read in the CSULB, CEDEN.CSCI and Sigala static data files & csci.df from the database.

# Add coordinates
CSULB2 <- CSULB %>%
  left_join(lustations.1[, c("stationid", "latitude", "longitude")],
            by=c("stationcode"="stationid"))
CEDEN.CSCI2 <- CEDEN.CSCI %>%
  left_join(lustations.1[, c("stationid", "latitude", "longitude")],
            by=c("StationCode"="stationid")) %>%
  dplyr::rename(stationcode=StationCode)
Sigala3 <- Sigala2 %>%
  left_join(lustations.1[, c("stationid", "latitude", "longitude")],
            by=c("StationCode"="stationid")) %>%
  dplyr::rename(stationcode=StationCode)

# Data frame of SMC csci, before purging CSULB and data already in CEDEN
smc.csci <- csci.df %>%
  #filter(record_origin == "SMC") %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic")],
             by = c("stationcode" = "stationid")) %>%
  filter(!is.na(probabilistic)) %>%
  filter(probabilistic == "true" | stationcode %in% AlsoInclude) %>%  # Retain probabilistic sites + bonus sites
  mutate(sampledate = as.Date(sampledate)) %>%
  mutate(stationcode = ifelse(stationcode == 'SMC00027', '403S00027', # Conform stationcodes to those that already exist in CEDEN (according to Jarma Bennett's email 5/19/2021)
                              ifelse(stationcode == 'SMC00036', '408S00036',
                                     ifelse(stationcode == 'SMC01384', '404S01384',
                                            ifelse(stationcode == 'SMC00836', '408S00836',
                                                   ifelse(stationcode == 'SMC01640', '404S01640', stationcode)))))) %>%
  mutate(stationcode = ifelse(stationcode == '405PS0011', '845PS0011', stationcode)) %>% # Per email from Rafi 5/10/2021.  Use RB8 stationcode.
  mutate(SiteDate = paste(masterid, sampledate, sep="_"))

# Retain CSULB csci and csci data already in CEDEN that are in SMC (just because CSULB PHab data exist, doesn't mean there's csci data too)
CSULB3 <- CSULB2 %>%
  filter(SiteDate %in% smc.csci$SiteDate) %>%
  mutate(Sourced = "CSULB")
CEDEN.CSCI3 <- CEDEN.CSCI2 %>%
  filter(SiteDate %in% smc.csci$SiteDate) %>%
  mutate(Sourced = "CEDEN")
Sigala4 <- Sigala3 %>%
  filter(SiteDate %in% smc.csci$SiteDate) %>%
  mutate(Sourced = "Sigala")
KickedOut <- rbind(rbind(CSULB3[, c("stationcode", "latitude", "longitude", "SiteDate", "Sourced")],
                         CEDEN.CSCI3[, c("stationcode", "latitude", "longitude", "SiteDate", "Sourced")]),
                   Sigala4[, c("stationcode", "latitude", "longitude", "SiteDate", "Sourced")])
KickedOut2 <- KickedOut %>%
  filter(!duplicated(SiteDate))

# Shape file of all, Statewide
KickedOut2_Pts<-KickedOut2 %>%
  st_as_sf(coords = c("longitude", "latitude"),
           crs = 4326)    # WGS1984 coordinate system

# Shape file of sites within SMC (southern California)
KickedOut3_Pts <- st_intersection(KickedOut2_Pts, smc_sheds_sf)

KickedOut3_Sites <- KickedOut3_Pts %>%
  filter(!duplicated(stationcode))

# Map of SoCal
ggplot()+
  geom_sf(data=smc_sheds_sf)+ # watershed outlines
  geom_sf(data=KickedOut3_Pts, color="blue")+
  #theme(legend.title=element_blank())+   # Option to remove legend title
  #guides(color=guide_legend("State"))+   # Option to change legend title
  ggtitle("SMC CSCI samples removed\nCSULB or samples already in CEDEN")
ggsave("L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Map_CSULB or samples already in CEDEN.png")


# Map of all California
library("maps")
states <- st_as_sf(map("state", plot = FALSE, fill = TRUE)) #built-in map of US that comes with library(maps)
states = st_transform(states, crs = 4326)    # WGS84

ggplot()+
  geom_sf(data=states)+       # USA
  geom_sf(data=smc_sheds_sf)+ # watershed outlines
  geom_sf(data=KickedOut2_Pts, color="blue")+
  #theme(legend.title=element_blank())+   # Option to remove legend title
  #guides(color=guide_legend("State"))+   # Option to change legend title
  coord_sf(xlim=c(-125, -114), ylim=c(32.2, 42.5), expand = FALSE)+        # California coordinate boundaries
  ggtitle("SMC CSCI samples removed\nCSULB or samples already in CEDEN")
ggsave("L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/CEDEN Upload/Measurements/Map_CSULB or samples already in CEDEN_CA.png")



