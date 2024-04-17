#' Author: Bhupendra Raut
#' Date: Apr 16 2024
#' Project: EARNEST
#' 
#' The code analyzes storm event details from NWS reports specific to Iowa, 
#' extracting event counts, wind characteristics, and spatial-temporal averages.
#'  It preprocesses the data, filtering columns and standardizing date-time 
#'  formats. Then iterates over each unique episode, calculating summary 
#'  statistics for multiple files. 
#'  Finally, the summarized results are exported to a CSV file 

state="IOWA"

library(plyr)

# Set working directory
setwd("/Users/bhupendra/projects/earnest")


# Read event details extract required columns
read_event_details <- function(file_path, state) {
  details <- read.table(file_path, header = TRUE, sep = ",")
  details$tornado <- ifelse(details$TOR_F_SCALE=="", FALSE, TRUE)
  
  # Filter relevant columns and rows
  details_df <- details[details$STATE == state & (details$EVENT_TYPE == "Thunderstorm Wind" |
                                                     details$EVENT_TYPE == "Strong Wind"),
                        c(7:9, 13, 18:20, 28, 29, 45:48, 52)]
  colnames(details_df) <- tolower(colnames(details_df))
  
  # Convert date-time columns to POSIXct format
  details_df$begin_time <- strptime(details_df$begin_date_time, format = "%d-%b-%y %H:%M:%S")
  details_df$end_time <- strptime(details_df$end_date_time, format = "%d-%b-%y %H:%M:%S")
  
  return(details_df)
}


summarize_episodes <- function(unique_episodes, details_df) {
  summary_df <- data.frame()  
  
  for (episode in unique_episodes) {
    df <- details_df[details_df$episode_id == episode,] 
    
    
    # Check if all necessary columns do not contain missing values
    if (is.null(df) || nrow(df) == 0 ||
        all(is.na(df$event_type)) &&  # Check event_type for missing values
        all(is.na(df$magnitude_type)) &&  # Check magnitude_type for missing values
        all(is.na(df$magnitude)) &&  # Check magnitude for missing values
        all(is.na(df$begin_lat)) &&  # Check begin_lat for missing values
        all(is.na(df$end_lat)) &&    # Check end_lat for missing values
        all(is.na(df$begin_lon)) &&  # Check begin_lon for missing values
        all(is.na(df$end_lon)) &&    # Check end_lon for missing values
        all(is.na(df$begin_time)) && # Check begin_time for missing values
        all(is.na(df$end_time))) {  # Check end_time for missing values
      
      next
    }
    
    thunderstorm_wind <- NA
    strong_wind <- NA
    wind_mg <- NA
    wind_eg <- NA
    wind_ms <- NA
    wind_es <- NA
    

    
    if (any(df$event_type == "Thunderstorm Wind")) {
      thunderstorm_wind <- TRUE
    }
    if (any(df$event_type == "Strong Wind")) {
      strong_wind <- TRUE
    }
    if (any(df$magnitude_type == "MG")) {
      wind_mg <- max(df$magnitude[df$magnitude_type == "MG"])
    }
    if (any(df$magnitude_type == "EG")) {
      wind_eg <- max(df$magnitude[df$magnitude_type == "EG"])
    }
    if (any(df$magnitude_type == "MS")) {
      wind_ms <- max(df$magnitude[df$magnitude_type == "MS"])
    }
    if (any(df$magnitude_type == "ES")) {
      wind_es <- max(df$magnitude[df$magnitude_type == "ES"])
    }
    
    # Compute summary statistics
    events <- length(df$event_id)
    tzone <- unique(df$cz_timezone)
    state <- unique(df$state)
    lat <- mean(c(df$begin_lat, df$end_lat), na.rm=TRUE)
    lon <- mean(c(df$begin_lon, df$end_lon), na.rm=TRUE)
    time <- round(mean(c(df$begin_time, df$end_time), na.rm=TRUE))
    
    # Create dataframe for current episode summary
    episode_summary <- data.frame(episode, events, lat, lon, time, tzone, state, 
                                  thunderstorm_wind, strong_wind, wind_mg, 
                                  wind_eg, wind_ms, wind_es)
    
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

outfile <- paste("./data/out/summary_episodes_", state, ".csv", sep = "")
write.csv(summary_df, file = outfile, quote = FALSE, row.names = FALSE)




