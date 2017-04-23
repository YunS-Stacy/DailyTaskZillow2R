library('bigrquery')
library('DBI')
library('RSelenium')
library('rvest')
library('stringr')
library('sqldf')
library('shinyFiles')
#Open with chrome
rD <- rsDriver(verbose = FALSE, port = 4446L)
remDr <- remoteDriver( port = 4446L, browserName = 'chrome' )
remDr <- rD$client
webElem
#Open the Terminal
#java -Dwebdriver.gecko.driver=geckodriver -jar selenium-server-standalone-3.0.1.jar
#Launch the browser
remDr$open()

#Navigate to Zillow recent sold(Philly)
remDr$navigate("https://www.zillow.com/homes/recently_sold/Philadelphia-PA/apartment_duplex_type/days_sort/")

#Get Prices
price <- remDr$findElements('css', '.zsg-photo-card-status') %>%
           sapply(function(x){
            x$getElementText() %>%
            str_replace("SOLD: ", "")})
#Get Housing Info
infos <- remDr$findElements('css', '.zsg-photo-card-info') %>%
  sapply(function(x){
    x$getElementText()
    })
##price per sqft
unitprice <- infos %>% 
  sapply(function(x){
      # unit price
      strsplit(x,' · ')[[1]][1] %>%
      str_replace(fixed("Price/sqft: "), "")})
##beds
beds <- infos %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][2] %>%
      str_replace(fixed(" bds"), "") %>%
      str_replace(fixed(" bd"), "")
  })

##baths
baths <- infos %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][3] %>%
      str_replace(fixed(" ba"), "")
  })
#area
area <- infos %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][4] %>%
      str_replace(fixed(" sqft"), "")
  })

#Get sold date
solddate<- remDr$findElements('css', '.zsg-photo-card-notification') %>% 
  sapply(function(x){
    x$getElementText() %>%
             sapply(function(x){
               str_replace(x, "Sold ",'') %>%
                 unlist()
             })
  }) %>%
  as.Date(format = '%m/%d/%Y')

#Get address
address <- remDr$findElements('css', '.zsg-photo-card-address') %>%
  sapply(function(x){
    unlist(x$getElementText() %>%
             sapply(function(x){
               str_replace(x, ", Philadelphia, PA",'')
             }))
  })

datestart <- Sys.Date() - 30

latest <- data.frame(address, solddate, price, unitprice, beds, baths, area, datestart)

remDr$close()
rD$server$stop()

# upload to google query
project <- "smartselect-34c02"
dataset <- 'houseListing'
table <- 'zillow'
get_dataset(project, dataset)

#at first time, write_append to the last table
insert_upload_job(project, dataset, table, latest, billing = project, write_disposition = "WRITE_APPEND" )

#query the latest 30 days
sql <- 'SELECT address, solddate, price, unitprice, beds, baths, area, datestart FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY address) row_number FROM houseListing.zillow
          )
          WHERE row_number = 1 AND DATE(solddate) >= DATE(datestart)'
today <- query_exec(sql, project, destination_table = NULL, default_dataset = dataset)

#delete the last table and upload the new one
delete_table(project,dataset,table)
insert_upload_job(project, dataset, table, today, billing = project)