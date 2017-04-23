library('bigrquery')
library('rvest')
library('stringr')
library('shinyFiles')
library('xml2')

html_1 <- read_html("https://www.zillow.com/homes/recently_sold/Philadelphia-PA/apartment_duplex_type/days_sort/")

#Get Prices
price_1 <- html_nodes(html_1, css='.zsg-photo-card-status') %>% 
  html_text() %>% str_replace("SOLD: ", "")
#Get Housing Info
infos_1 <- html_nodes(html_1, css='.zsg-photo-card-info') %>% 
  html_text()
##price per sqft
unitprice_1 <- infos_1 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][1] %>%
      str_replace(fixed("Price/sqft: "), "")})
##beds
beds_1 <- infos_1 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][2] %>%
      str_replace(fixed(" bds"), "") %>%
      str_replace(fixed(" bd"), "")
  })

##baths
baths_1 <- infos_1 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][3] %>%
      str_replace(fixed(" ba"), "")
  })
#area
area_1 <- infos_1 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][4] %>%
      str_replace(fixed(" sqft"), "")
  })

#Get sold date
solddate_1 <- html_nodes(html_1, css='.zsg-photo-card-notification') %>% 
  html_text() %>%
  str_replace("Sold ",'') %>%
  as.Date(format = '%m/%d/%Y')

#Get address
address_1 <- html_nodes(html_1, css='.zsg-photo-card-address') %>% 
  html_text() %>%
  str_replace(", Philadelphia, PA",'')

latest_1 <- data.frame(address_1, solddate_1, price_1, unitprice_1, beds_1, baths_1, area_1, row.names = NULL)
names(latest_1)  <- c('address', 'solddate', 'price', 'unitprice', 'beds', 'baths', 'area')

html_2 <- read_html("https://www.zillow.com/homes/recently_sold/Philadelphia-PA/apartment_duplex_type/days_sort/2_p")

#Get Prices
price_2 <- html_nodes(html_2, css='.zsg-photo-card-status') %>% 
  html_text() %>% str_replace("SOLD: ", "")
#Get Housing Info
infos_2 <- html_nodes(html_2, css='.zsg-photo-card-info') %>% 
  html_text()
##price per sqft
unitprice_2 <- infos_2 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][1] %>%
      str_replace(fixed("Price/sqft: "), "")})
##beds
beds_2 <- infos_2 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][2] %>%
      str_replace(fixed(" bds"), "") %>%
      str_replace(fixed(" bd"), "")
  })

##baths
baths_2 <- infos_2 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][3] %>%
      str_replace(fixed(" ba"), "")
  })
#area
area_2 <- infos_2 %>% 
  sapply(function(x){
    # unit price
    strsplit(x,' · ')[[1]][4] %>%
      str_replace(fixed(" sqft"), "")
  })

#Get sold date
solddate_2 <- html_nodes(html_2, css='.zsg-photo-card-notification') %>% 
  html_text() %>%
  str_replace("Sold ",'') %>%
  as.Date(format = '%m/%d/%Y')

#Get address
address_2 <- html_nodes(html_2, css='.zsg-photo-card-address') %>% 
  html_text() %>%
  str_replace(", Philadelphia, PA",'')
latest_2 <- data.frame(address_2, solddate_2, price_2, unitprice_2, beds_2, baths_2, area_2, row.names = NULL)
names(latest_2)  <- c('address', 'solddate', 'price', 'unitprice', 'beds', 'baths', 'area')

datestart <- Sys.Date() - 30

latest <- cbind(rbind(latest_1,latest_2),datestart)

# upload to google query
project <- "smartselect-34c02"
dataset <- 'houseListing'
table <- 'zillow'
get_dataset(project, dataset)

#at first time, write_append to the last table
insert_upload_job(project, dataset, table, latest, billing = project, write_disposition = "WRITE_APPEND" )

#query the latest 30 days
sql <- 'SELECT address, solddate, price, unitprice, beds, baths, area, datestart FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY address) row_number FROM houseListing.zillow)
        WHERE row_number = 1 AND DATE(solddate) >= DATE(datestart)
        ORDER BY solddate desc'
today <- query_exec(sql, project, destination_table = NULL, default_dataset = dataset)

#delete the last table and upload the new one
delete_table(project,dataset,table)
insert_upload_job(project, dataset, table, today, billing = project)
