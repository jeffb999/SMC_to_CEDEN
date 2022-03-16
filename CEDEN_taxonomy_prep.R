# script to prep taxonomy data for CEDEN submission
# Anne Holt, 10/13/20

# import data to be submitted, creates new clean dataset for export
# Adding Stations portion of the scriptchecks if new stations need to be added using CEDEN lookup lists
# http://ceden.org/CEDEN_Checker/Checker/LookUpLists.php



# cleaning based on CEDEN checker errors

# REQUIRED FIELDS, to prioritize data cleaning: 
# StationCode, SampleDate, ProjectCode, CollectionTime, CollectionMethodCode, SampleTypeCode,Replicate, CollectionDevideName, FinalID,
# BAResult, Result, UnitName, ResQualCode, QACode

# for now, this script generally does not clean up fields if they are not required for submission 





#### DATA PREP ####

library(tidyverse)

# import sccwrp taxonomy data to be submitted
tax_og <- read.csv("ceden_taxonomy_sccwrp_092920_AH.csv")


# cleaning based on CEDEN checker errors


tax_clean <- tax_og %>% 
  # making sure no extra rows 
  filter(!is.na(StationCode)) %>% 
  
  # note: project code should be "SMC_SCCWRP", if StationCode error may have to request addition of that station
  
  # using Organism Look up list...
  mutate(FinalID = recode(FinalID,"Aeshna/Rhionaeshna" = "Aeshna", #spelled Rhionaeschna, but can just leave Aeshna
                                    "Anisoptera" = "Odonata", #move from infraorder to order
                                    "Axonopsinae" = "Aturidae", #subfamily to family
                                    # "Cyphomella" = "Chironomini", #genus to tribe...requested Cyphomella addition so commented out for now
                                    "Eogammarus" = "Anisogammaridae", #genus to family 
                                    "Eukiefferiella tirolensis" = "Eukiefferiella", #changed to genus
                                    "Graptocorixa uhleroidae" = "Graptocorixa uhleroidea", #misspelled
                                    "Helius sp" = "Limoniinae", #changed to subfamily
                                    "Truncatelloidea" = "Gastropoda", #changed to class
                                    "Ylodes" = "Triaenodes" #changed form genus to family
                                   )) %>% 
  
  mutate(ResQualCode = case_when(ResQualCode == "'=" ~ "=", #not accepted formatting, this is some excel formatting thing 
                                 TRUE ~ ResQualCode))  %>%
  
  
  # taxonomic qualifier: accepts "None", but error with NA (this field is not required but quick fix)
  mutate(TaxonomicQualifier = case_when(is.na(TaxonomicQualifier) ~ "None",
                                        TRUE ~ TaxonomicQualifier)) %>% 

  # PersonnelCode_Result is a desired field (though still not technically required), but all updates are made for this personnel field
  # name formatting should be "Last, *First Initial*."
  mutate(PersonnelCode_Result = recode(PersonnelCode_Result,
                                       "G. Wallace" = "Wallace, G.",
                                       "J. Pfeiffer" = "Pfeiffer, J.",
                                       "K. King" = "King, K.",
                                       "Kurt King" = "King, Kt.",
                                       "W. Willis" = "Willis, W.",
                                       "Wendy Willis" = "Willis, W.",
                                       "W.Willis" = "Willis, W.",
                                       "B. Isham" = "Isham, B.",
                                       "Bill Isham" = "Isham, B.",
                                       "Craig Pernot" = "Pernot, C.",
                                       "B. Richards" = "Richards, B.",
                                       "Aaron Webster" = "Webster, A.",
                                       "E. Corona" = "Corona, E.",
                                       "C. Pernot" = "Pernot, C.",
                                       "C. Dougherty" = "Dougherty, C.",
                                       "D. Miller" = "Miller, D.",
                                       "F. Zern" = "Zern, F.",
                                       "M. Canfield" = "Canfield, M.",
                                       
                                       # personnel below are not in look up list, submitted request to get them added
                                       
                                       "S. Holt" = "Holt, S.",
                                       "Sheila Holt" = "Holt, S.",
                                       "T. Gerlinger" = "Gerlinger, T.",
                                       "Thomas Gerlinger" = ", T.",
                                       "Kim Kratz" = "Kratz, K.",
                                       "K. Kratz" = "Kratz, K.",
                                       "Megan Payne" = "Payne, M.",
                                       "L. Mills" = "Mills, L.",
                                       "B.J. Krestian" = "Krestian, B.J.",
                                       "Jennifer Rideout" = "Rideout, J.",
                                       "Zach Meier" = "Meier, Z.",
                                       "K. Jones" = "Jones, K." ,
                                       "E. Dinger" = "Dinger, E.",
                                       "J. Kotynek" = "Kotynek, J.",
                                       "M. Tagg" = "Tagg, M.",
                                       "M. Tagg, J. Kotynek" = "Tagg, M.", #not sure about two name formatting, changed to one for now
                                       "J. Kotynek, M. Tagg" = "Kotynek, J.",
                                       "Analysts, E." = "NA"
                      
                                  
                                       
                                          )) %>% 
  
  
  # personnel formatting changes... didn't finish edits for now because PersonnelCode_LabEffort field is not required
  mutate(PersonnelCode_LabEffort = recode(PersonnelCode_LabEffort,
                                          "Adele Lefors" = "Lefors, A.", #name formatting should be "Last, *First Initial*."
                                          "Alexis Wallengren"= "Wallengren, A.",
                                          "Charis Samia" = "Samia, C.",
                                          "Denay Stevenson" = "Stevenson, D.",
                                          "Elvis Agustin" = "Agustin, E."
  ))  
  


  


# exporting portions of dataset to run through checker

# tax_export <- tax_clean[25000:30584,]
# 
# 
# write.csv(tax_export,"reduced_files/ceden_tax_AH_5.csv", row.names = FALSE)




  
  
  
#### ADDING STATIONS ####
  
  
# checking bug data stations
# workflow: look in CEDEN look up list and in requested stations to be added, see if any stations unique to bug data that need to also be requested

  
  
# import downloaded ceden lookup list
stations_ceden <- read_csv("CEDEN_StationLookUp.csv") %>% 
  select(StationCode) %>% 
  mutate(StationCode_ceden = StationCode)
  

# import stations Jeff already requested to be added
stations_request <- read_csv("ceden_stations_requested.csv") %>% 
  mutate(StationCode_requested = StationCode)


# see if bug data has stations not in ceden lookup or already requested by Jeff
stations_add <- tax_clean %>% 
  distinct(StationCode) %>% 
  left_join(stations_ceden) %>% 
  left_join(stations_request) %>% 
  filter(is.na(StationCode_ceden) & is.na(StationCode_requested))



# for stations to be added, also need StationSource, StationName, StationAgency, CoordinateNumber, TargetLatitude, TargetLongitude, Datum, CoordinateSource, SWRCBWatTypeCode

# getting the stationname and lat/long from SMC...


library(DBI) # needed to connect to database
library(dbplyr) # needed to connect to database
library(RPostgreSQL) # needed to connect to our database
library(rstudioapi) # just so we can type the password as we run the script, so it is not written in the clear

# Create connection to the database
con <- dbConnect(
  PostgreSQL(),
  host = "192.168.1.17",
  dbname = 'smc',
  user = 'smcread',
  password = '1969$Harbor' # if we post to github, we might want to do rstudioapi::askForPassword()
)

#for getting station information
lustations_query <- "select * from sde.lu_stations"
tbl_lustations   <- tbl(con, sql(lustations_query))
lustations.1    <- as.data.frame(tbl_lustations)


# retaining only fields of interest
lustations_add <- lustations.1 %>% select(stationid, masterid, stationname, latitude, longitude)





# prepping final station information
# copy and paste this exported information into vocab template

# used Lat/Long for Target Lat/Long fields.... confirm this decision??
  # StationAgency "SCCWRP"; Datum "NR"; "CoordinateSource "NR"; SWRCBWatTypeCode "R_W" (this is what Jeff had for his requested stations, confirm this)



stations_add_final <- stations_add %>% 
  select(StationCode) %>% 
  left_join(lustations_add, by = c("StationCode" = "stationid")) %>% 
  select(-masterid)


# write_csv(stations_add_final, "taxonomy_stations_add_final.csv")








# extra:
# I noticed the masterids might be in ceden, investigating that below
# from Jeff: decision to stick with stationcodes not masterid, as stationcode is usually what the data owners refer to a site 

# stations_add_2 <- tax_clean %>% 
#   distinct(StationCode) %>% 
#   left_join(lustations_add, by = c("StationCode" = "stationid")) %>% 
#   rename(StationCode_old = StationCode) %>% 
#   mutate(StationCode = masterid) %>% 
#   left_join(stations_ceden) %>% 
#   left_join(stations_request) %>% 
#   filter(is.na(StationCode_ceden) & is.na(StationCode_requested))


         