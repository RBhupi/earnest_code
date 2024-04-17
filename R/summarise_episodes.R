#' Author: Bhupendra Raut
#' Date: Apr 16 2024
#' Project: EARNEST
#' 
#' The code analyzes windy weather episodes from NWS reports specific to Iowa, 
#' extracting event counts, wind characteristics, and other weather types 
#' reported during the episode.
#' It preprocesses the data, filtering columns and standardizing date-time 
#' formats. Then iterates over each unique episode, calculating summary 
#' statistics saving to a CSV file. 
#' The Derecho events are not listed in the NWS reports as standard events, but the
#' descriptions refers to the Dericho, which is extracted.

state="IOWA"
select_events <- c("Thunderstorm Wind", "Strong Wind", "High wind")


# Set working directory
setwd("/Users/bhupendra/projects/earnest")


# Read event details extract required columns
read_event_details <- function(file_path, state) {
  details <- read.table(file_path, header = TRUE, sep = ",")
  #details$tornadof <- ifelse(details$TOR_F_SCALE=="", NA, details$TOR_F_SCALE)
  details$derecho <- stringr::str_detect(tolower(details$EPISODE_NARRATIVE), 
                                         pattern = "derecho")
  # Convert date-time columns to POSIXct format
  details$begin_time <- strptime(details$BEGIN_DATE_TIME, format = "%d-%b-%y %H:%M:%S")
  details$end_time <- strptime(details$END_DATE_TIME, format = "%d-%b-%y %H:%M:%S")
  
  details <- details[ ,c(7:9, 13, 18:20, 28, 29, 45:48, 52, 53)]
  
  # Filter relevant columns to find episodes
  details_df <- details[details$STATE == state & 
                          (details$EVENT_TYPE %in% select_events),]
  unique_episodes <- unique(details_df$EPISODE_ID)

  # select data for only these episodes
  details <- details[details$EPISODE_ID %in% unique_episodes,]
  colnames(details) <- tolower(colnames(details))
  

  
  return(details)
}


summarize_episodes <- function(unique_episodes, details_df) {
  summary_df <- data.frame()  
  
  for (episode in unique_episodes) {
    df <- details_df[details_df$episode_id == episode,] 
    
    
    # Check if all necessary columns are missing values
    if (is.null(df) || nrow(df) == 0 ||
        all(is.na(df$event_type)) &&  # Check event_type for missing values
        all(is.na(df$magnitude_type)) &&  # Check magnitude_type for missing values
        #all(is.na(df$magnitude)) &&  # Check magnitude for missing values
        #all(is.na(df$begin_lat)) &&  # Check begin_lat for missing values
        #all(is.na(df$end_lat)) &&    # Check end_lat for missing values
        #all(is.na(df$begin_lon)) &&  # Check begin_lon for missing values
        #all(is.na(df$end_lon)) &&    # Check end_lon for missing values
        all(is.na(df$begin_time)) && # Check begin_time for missing values
        all(is.na(df$end_time))) {  # Check end_time for missing values
      
      next
    }
    
    # Initialize variables for additional event types
    avalanche <- FALSE
    blizzard <- FALSE
    coastal_flood <- FALSE
    cold_wind_chill <- FALSE
    debris_flow <- FALSE
    dense_fog <- FALSE
    dense_smoke <- FALSE
    drought <- FALSE
    dust_devil <- FALSE
    dust_storm <- FALSE
    excessive_heat <- FALSE
    extreme_cold_wind_chill <- FALSE
    flash_flood <- FALSE
    flood <- FALSE
    freezing_fog <- FALSE
    frost_freeze <- FALSE
    funnel_cloud <- FALSE
    hail <- FALSE
    heat <- FALSE
    heavy_rain <- FALSE
    heavy_snow <- FALSE
    high_surf <- FALSE
    high_wind <- FALSE
    hurricane_typhoon <- FALSE
    ice_storm <- FALSE
    lake_effect_snow <- FALSE
    lakeshore_flood <- FALSE
    lightning <- FALSE
    marine_hail <- FALSE
    marine_high_wind <- FALSE
    marine_strong_wind <- FALSE
    marine_thunderstorm_wind <- FALSE
    rip_current <- FALSE
    seiche <- FALSE
    sleet <- FALSE
    storm_surge_tide <- FALSE
    strong_wind <- FALSE
    thunderstorm_wind <- FALSE
    tornado <- FALSE
    tropical_depression <- FALSE
    tropical_storm <- FALSE
    tsunami <- FALSE
    volcanic_ash <- FALSE
    waterspout <- FALSE
    wildfire <- FALSE
    winter_storm <- FALSE
    winter_weather <- FALSE
    
    # Check event types and set corresponding variables
    if ("Avalanche" %in% df$event_type) avalanche <- TRUE
    if ("Blizzard" %in% df$event_type) blizzard <- TRUE
    if ("Coastal Flood" %in% df$event_type) coastal_flood <- TRUE
    if ("Cold/Wind Chill" %in% df$event_type) cold_wind_chill <- TRUE
    if ("Debris Flow" %in% df$event_type) debris_flow <- TRUE
    if ("Dense Fog" %in% df$event_type) dense_fog <- TRUE
    if ("Dense Smoke" %in% df$event_type) dense_smoke <- TRUE
    if ("Drought" %in% df$event_type) drought <- TRUE
    if ("Dust Devil" %in% df$event_type) dust_devil <- TRUE
    if ("Dust Storm" %in% df$event_type) dust_storm <- TRUE
    if ("Excessive Heat" %in% df$event_type) excessive_heat <- TRUE
    if ("Extreme Cold/Wind Chill" %in% df$event_type) extreme_cold_wind_chill <- TRUE
    if ("Flash Flood" %in% df$event_type) flash_flood <- TRUE
    if ("Flood" %in% df$event_type) flood <- TRUE
    if ("Freezing Fog" %in% df$event_type) freezing_fog <- TRUE
    if ("Frost/Freeze" %in% df$event_type) frost_freeze <- TRUE
    if ("Funnel Cloud" %in% df$event_type) funnel_cloud <- TRUE
    if ("Hail" %in% df$event_type) hail <- TRUE
    if ("Heat" %in% df$event_type) heat <- TRUE
    if ("Heavy Rain" %in% df$event_type) heavy_rain <- TRUE
    if ("Heavy Snow" %in% df$event_type) heavy_snow <- TRUE
    if ("High Surf" %in% df$event_type) high_surf <- TRUE
    if ("High Wind" %in% df$event_type) high_wind <- TRUE
    if ("Hurricane (Typhoon)" %in% df$event_type) hurricane_typhoon <- TRUE
    if ("Ice Storm" %in% df$event_type) ice_storm <- TRUE
    if ("Lake-Effect Snow" %in% df$event_type) lake_effect_snow <- TRUE
    if ("Lakeshore Flood" %in% df$event_type) lakeshore_flood <- TRUE
    if ("Lightning" %in% df$event_type) lightning <- TRUE
    if ("Marine Hail" %in% df$event_type) marine_hail <- TRUE
    if ("Marine High Wind" %in% df$event_type) marine_high_wind <- TRUE
    if ("Marine Strong Wind" %in% df$event_type) marine_strong_wind <- TRUE
    if ("Marine Thunderstorm Wind" %in% df$event_type) marine_thunderstorm_wind <- TRUE
    if ("Rip Current" %in% df$event_type) rip_current <- TRUE
    if ("Seiche" %in% df$event_type) seiche <- TRUE
    if ("Sleet" %in% df$event_type) sleet <- TRUE
    if ("Storm Surge/Tide" %in% df$event_type) storm_surge_tide <- TRUE
    if ("Strong Wind" %in% df$event_type) strong_wind <- TRUE
    if ("Thunderstorm Wind" %in% df$event_type) thunderstorm_wind <- TRUE
    if ("Tornado" %in% df$event_type) tornado <- TRUE
    if ("Tropical Depression" %in% df$event_type) tropical_depression <- TRUE
    if ("Tropical Storm" %in% df$event_type) tropical_storm <- TRUE
    if ("Tsunami" %in% df$event_type) tsunami <- TRUE
    if ("Volcanic Ash" %in% df$event_type) volcanic_ash <- TRUE
    if ("Waterspout" %in% df$event_type) waterspout <- TRUE
    if ("Wildfire" %in% df$event_type) wildfire <- TRUE
    if ("Winter Storm" %in% df$event_type) winter_storm <- TRUE
    if ("Winter Weather" %in% df$event_type) winter_weather <- TRUE
    
    
    wind_mg <- NA
    wind_eg <- NA
    wind_ms <- NA
    wind_es <- NA
    if (any(df$magnitude_type == "MG", na.rm = TRUE)){
      wind_mg <- max(df$magnitude[df$magnitude_type == "MG"])
    }
    if (any(df$magnitude_type == "EG", na.rm = TRUE)) {
      wind_eg <- max(df$magnitude[df$magnitude_type == "EG"])
    }
    if (any(df$magnitude_type == "MS", na.rm = TRUE)) {
      wind_ms <- max(df$magnitude[df$magnitude_type == "MS"])
    }
    if (any(df$magnitude_type == "ES", na.rm = TRUE)) {
      wind_es <- max(df$magnitude[df$magnitude_type == "ES"])
    }
    
    # Compute summary statistics
    events <- length(df$event_id)
    tzone <- unique(df$cz_timezone)
    state <- unique(df$state)
    lat <- mean(c(df$begin_lat, df$end_lat), na.rm=TRUE)
    lon <- mean(c(df$begin_lon, df$end_lon), na.rm=TRUE)
    time <- round(mean(c(df$begin_time, df$end_time), na.rm=TRUE))
    time <- strftime(time, format = "%Y-%m-%d %H:%M:%S")
    derecho <- any(df$derecho, na.rm = TRUE)
    #torf_max <- ifelse(all(is.na(df$tornadof), NA, max(df$tornadof, na.rm = TRUE)))
    
    # Create dataframe for current episode summary
    episode_summary <- data.frame(time, tzone, lat, lon, state, episode, events,
                                  derecho, tornado, funnel_cloud,
                                  wind_mg, wind_eg, wind_ms, wind_es, flash_flood, 
                                  flood, heavy_rain, heavy_snow,
                                  high_wind, hurricane_typhoon, lightning,
                                  marine_hail, marine_high_wind, marine_strong_wind, 
                                  marine_thunderstorm_wind, strong_wind, thunderstorm_wind,
                                  avalanche, blizzard, coastal_flood, cold_wind_chill, 
                                  debris_flow, dense_fog, dense_smoke, drought, dust_devil, 
                                  dust_storm, excessive_heat, extreme_cold_wind_chill, 
                                  freezing_fog, frost_freeze, hail, heat, 
                                  high_surf, ice_storm, lake_effect_snow, lakeshore_flood, 
                                  rip_current, seiche, sleet, 
                                  storm_surge_tide, tropical_depression, 
                                  tropical_storm, tsunami, volcanic_ash, 
                                  waterspout, wildfire, winter_storm, winter_weather)
    
    
    
    # Append current episode summary to summary dataframe
    summary_df <- rbind(summary_df, episode_summary)
  }
  
  return(summary_df)  # Return the summary dataframe
}



run_all_files <- function(f, state) {
  details_df <- read_event_details(f, state)
  unique_episodes <- unique(details_df$episode_id)
  summary_df <- summarize_episodes(unique_episodes, details_df)
  return(summary_df)
}




event_files <- Sys.glob(path = "./data/stormevents/StormEvents_details-ftp_v1.0_d*.csv")
event_files <- event_files[order(event_files)]
summary_df_list <- lapply(event_files, run_all_files, state)

# unlist and combine dataframes from list into one
summary_df <- do.call(rbind, Filter(Negate(is.null), summary_df_list))
summary_df <- summary_df[order(summary_df$time),]
summary_df <- summary_df[which(!is.na(summary_df$time)),]

period <- paste((strftime(min(summary_df$time), format="%Y")),
                (strftime(max(summary_df$time), format="%Y")), sep = "-")


outfile <- paste("./data/out/", state, 
                 "_windy-weather-episodes_", period, strftime(Sys.time(), format = "_v%y-%m"), ".csv", sep = "")
write.csv(summary_df, file = outfile, quote = FALSE, row.names = FALSE)


