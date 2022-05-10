## ---------------------------
##
## Script name: SDG_related_indicators
##
## Project: GPE_RF_2025_Indicators
##
## Purpose of script: Wrangle and clean data from UIS
##
## Author: Andrei Wong Espejo
##
## Date Created: 2022-03-31
##
## Email: awongespejo@worldbank.org
##
## ---------------------------
##
## Notes: xxxxxxxxxxxxxxxxxxx
##
##
## ---------------------------

# Program Set-up ------------

options(scipen = 100, digits = 2) # Prefer non-scientific notation

# Load required packages ----

if (!require("pacman")) {
  install.packages("pacman")
}
pacman::p_load(here, datapasta, styler, dplyr, readr, janitor, tidyverse)

# Loading CSV in memory #####################################################
# FROM: https://apiportal.uis.unesco.org/BDDS_R_Tutorial
# Read CSVs while specifying column type
dfNational <- read_csv(here("SDG_related_indicators/OPRI_2022.03", "OPRI_DATA_NATIONAL.csv"), na = "", col_types = cols(
  INDICATOR_ID = col_character(),
  COUNTRY_ID = col_character(),
  YEAR = col_integer(),
  VALUE = col_double(),
  MAGNITUDE = col_character(),
  QUALIFIER = col_character()
))


dfCountryLabels <- read_csv(here("SDG_related_indicators/OPRI_2022.03", "OPRI_COUNTRY.csv"), na = "", col_types = cols(
  COUNTRY_ID = col_character(),
  COUNTRY_NAME_EN = col_character()
))

dfIndicatorLabels <- read_csv(here("SDG_related_indicators/OPRI_2022.03", "OPRI_LABEL.csv"), na = "", col_types = cols(
  INDICATOR_ID = col_character(),
  INDICATOR_LABEL_EN = col_character()
))


dfMetadata <- read_csv(here("SDG_related_indicators/OPRI_2022.03", "OPRI_METADATA.csv"), na = "", col_types = cols(
  INDICATOR_ID = col_character(),
  COUNTRY_ID = col_character(),
  TYPE = col_character(),
  METADATA = col_character()
))

# Creating subsets of the data ##############################################
# 1) Extracting a vectors of sorted unique values for the Year, Country and Indicator variables
# Those vectors' values will serve as the default parameters in the following function
allYears <- sort(unique(dfNational[, "YEAR"])[[1]])
recentYears <- tail(allYears, n = 4)
allCountries <- sort(unique(dfNational[, "COUNTRY_ID"])[[1]])
allIndicators <- sort(unique(dfNational[, "INDICATOR_ID"])[[1]])

# Merging metadata and data subsets #########################################
# Data subset function
subsetData <- function(dataSet, yearList = recentYears, countryList = allCountries, indicatorList = allIndicators) {
  # Subsets the data
  #
  # Parameters
  # ----------
  # dataSet : DataFrame
  # a DataFrame to be subsetted
  # yearList: a list of int, default is recentYears
  # a list of years
  # countryList: a list of str, defaults is allCountries
  # a list of 3-letter ISO country code
  # indicatorList: a list of str, default is allIndicators
  # a list of indicator codes
  # Returns
  # -------
  # DataFrame
  # a DataFrame subsetted by a list of years, countries and indicators
  aSubset <- dataSet %>% filter(
    YEAR %in% yearList,
    COUNTRY_ID %in% countryList,
    INDICATOR_ID %in% indicatorList
  )
  return(aSubset)
}

# Add metadata to dataset function
addMetadata <- function(dataSub, metaDataSub, metadataType = "Source:Data sources") {
  # Merges the metadata to the data
  #
  # Parameters
  # ----------
  # dataSub: DataFrame
  # a DataFrame receiving the metadata from another DataFrame
  # metaDataSub: DataFrame
  # a DataFrame giving metadata to another DataFrame
  # metadataType: str {'Source:Data sources','Under Coverage:Students or individuals'}
  # a string for specifying the type of metadata merged to the dataset (note
  # that the number of metadata type will vary across datasets and over time)
  #
  # Returns
  # -------
  # DataFrame
  # a DataFrame with an extra column of metadata
  metadataSubByType <- filter(metaDataSub, TYPE == metadataType) %>% # filter metadata on metadata type
    group_by(YEAR, COUNTRY_ID, INDICATOR_ID, TYPE) %>% # Var on which to Group
    summarise(METADATA = paste(METADATA, collapse = "|")) # Var that will be grouped
  dataSubsetWithMeta <- dataSub %>% left_join(metadataSubByType, by = c("YEAR", "COUNTRY_ID", "INDICATOR_ID"))
  return(dataSubsetWithMeta)
}


# Example 1:# Subsetting by specifying all the parameters into a list
# Defining the list of year, country and indicator of interest
yearsSubset <- c(
  2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019,
  2020, 2021
)
# countrySubset <- c("ARG", "KWT", "SWE", "ZWE", "COG")
countrySubset <- allCountries
indicSubset <- c(
  "X.PPPCONST.02.FSGOV",
  "X.PPPCONST.1.FSGOV",
  "X.PPPCONST.2.FSGOV",
  "X.PPPCONST.3.FSGOV"
)


## Choosen indicators:
# X.PPPCONST.02.FSGOV	Government expenditure on pre-primary education, constant PPP$ (millions)
# X.PPPCONST.1.FSGOV	Government expenditure on primary education, constant  PPP$ (millions)
# X.PPPCONST.2.FSGOV	Government expenditure on lower secondary education, constant  PPP$ (millions)
# X.PPPCONST.3.FSGOV	Government expenditure on upper secondary education, constant  PPP$ (millions)


# Data subset
# myDataSubset <- subsetData(dfNational, yearsSubset, countrySubset, indicSubset)
myDataSubset <- subsetData(dfNational, yearsSubset, countrySubset, indicSubset)


# Metadata subset
# myMetadataSubset <- subsetData(dfMetadata, yearsSubset, countrySubset, indicSubset)
myMetadataSubset <- subsetData(dfMetadata, yearsSubset, indicSubset)


# Merging metadata with data subset using specified metadata type
mySubsetWithUnderCov <- addMetadata(myDataSubset, myMetadataSubset, "Source:Data sources")

mySubsetWith_UnderCov_Source <- addMetadata(mySubsetWithUnderCov, myMetadataSubset)

# Adding labels #############################################################
# Add labels to dataset function
addLabels <- function(dataSetNoLabel, labelSet, keyVariable) {
  # Adds labels to a dataset
  # Adds an additional column with the country or indicators name.
  #
  # Parameters
  # ----------
  # dataSetNoLabel: DataFrame
  # the DataFrame containing the data
  # labelSet: DataFrame
  # the DataFrame containing the labels
  # keyVariable: str {'INDICATOR_ID', 'COUNTRY_ID'}
  # a string specifying the key variable for the merge
  #
  # Returns
  # -------
  # DataFrame
  # a DataFrame with extra columns for labels
  dataSetWithLabels <- dataSetNoLabel %>% left_join(labelSet, by = keyVariable)
  return(dataSetWithLabels)
}


# Adding country labels
mySubsetWith_Meta_countryLabel <- addLabels(
  mySubsetWith_UnderCov_Source,
  dfCountryLabels, "COUNTRY_ID"
)

# Adding indicator labels (to the previous subset with labels)
mySubsetWith_Meta_allLabels <- addLabels(
  mySubsetWith_Meta_countryLabel,
  dfIndicatorLabels, "INDICATOR_ID"
)


# Export subset to CSV #############################################################
# write.csv(mySubsetWith_Meta_allLabels, na = "", here("SDG_related_indicators","Gov_exp_realease_march2022.csv"))

head(mySubsetWith_Meta_allLabels)

data_list <- mySubsetWith_Meta_allLabels |>
  clean_names() |>
  mutate_at(
    vars(country_name_en),
    ~ recode(.,
      "Democratic Republic of the Congo" = "Congo, Democratic Republic of",
      "Congo, DR"  = "Congo, Democratic Republic of",
      "Gambia"     = "Gambia, The",
      "Lao PDR"    = "Lao People's Democratic Republic",
      "Kyrgyz Republic"   = "Kyrgyzstan",
      "Republic of Congo" = "Congo, Republic of",
      "Côte d'Ivoire"     = "Cote d'Ivoire",
      "Republic of Moldova"   = "Moldova",
      "St. Lucia"             = "Saint Lucia",
      "St. Vincent and the Grenadines"  = "Saint Vincent and the Grenadines",              "United Republic of Tanzania"     = "Tanzania",
      "United Rep. of Tanzania"         = "Tanzania",
      "Viet Nam"  = "Vietnam",
      "Yemen"     = "Yemen, Republic of"
    )
  ) |>
  arrange(indicator_id, year, country_name_en) |>
  group_by(indicator_id) |>
  pivot_wider(
    id_cols = c(indicator_id, country_name_en),
    names_from = year,
    values_from = value
  ) |>
  group_map(~.x, .keep = TRUE)


openxlsx::write.xlsx(data_list, here("SDG_related_indicators", "Gov_exp_realease_march2022.xlsx"))
