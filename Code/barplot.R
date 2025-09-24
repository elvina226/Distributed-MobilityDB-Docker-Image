# Sample data
systems <- c("PostgreSQL+PostGIS", "Centralized MobilityDB", "Distributed MobilityDB")
time_scales <- c("100 ships", "1000 ships", "3000 ships")
time_scales <- c("1 month", "3 months", "6 months")
time_scales <- c("1km", "2km", "3km")
time_scales <- c("Small Polygon", "Medium Polygon", "Large Polygon")
time_scales <- c("1 month\nSmall Polygon", "3 months\nMedium Polygon", "6 months\nLarge Polygon")
time_scales <- c("20 areas", "50 areas", "all areas")
time_scales <- c("Wind speed>20")
time_scales <- c("Wind gust>30 and speed<5")

# Example execution time data (rows = systems, columns = time scales)
execution_times <- matrix(c(
  186.675	,213.7075	,233.5725,    # PostgreSQL+PostGIS"
  1.3485,	21.486,	50.563,        # Centralized MobilityDB
  33.534,	104.504	,133.8055# Distributed MobilityDB
), nrow = 3, byrow = TRUE)

# Define custom colors matching the image
custom_colors <- c("#66CCFF", "#FF6666", "#66CC66")


# Adjust plot margins to make space for legend on the right
par(mar = c(5, 4, 4, 8), xpd = TRUE)

# Create grouped barplot
barplot(execution_times,
        beside = TRUE,
        names.arg = time_scales,
        col = custom_colors,
        ylim =  c(0, max(execution_times)),
        main = "Q11b High-Density Routes",
        ylab = "Time (seconds)")


# Create grouped barplot for big numeric diferences
barplot(execution_times,
        beside = TRUE,
        names.arg = time_scales,
        col = custom_colors,
        log = "y",  # logarithmic y-axis
        main = "Q11b High-Density Routes" ,
        ylab = "Time (seconds, log scale)")

# Add legend 
legend("topright",
       inset = c(-0.19, 0.1),
       legend = systems,
       fill = custom_colors,
       title = "System",
       xpd = TRUE)

