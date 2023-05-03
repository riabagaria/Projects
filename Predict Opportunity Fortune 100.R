
## FINAL

## "North America Category Sales per Zip"
##  Notes
## Goal: Build category specific models that predict opportunity for a postal code
## The grain of prediction will be at Division - Gender - Category

old <- Sys.time()

City <- "NA"

##Load in libraries and set working directory

library(geosphere)
library(sp)
library(rgeos)
library(fossil)
library(RANN)
library(dplyr)
library(plyr)
library(sqldf)
library(klaR)
library(dummies)
library(openxlsx)
library(caret)
library(data.table)
library(ModelMetrics)

# ## LOADING DATASET FROM SNOWFLAKE ##
library("RJDBC")
options(java.parameters = "-Xmx16g")
#Establish connection to snowflake
jdbcDriver <- JDBC(driverClass=" ",
                   classPath=" ")
jdbcConnection <- dbConnect(jdbcDriver, " ",
                            "mail ID", getPass::getPass("Enter your password: "))

res1 <- dbGetQuery(jdbcConnection, "use role") #// mention your Role name of snowflake //
res2 <- dbGetQuery(jdbcConnection, "use warehouse") #// mention your wareHOuse name of snowflake //
res3 <- dbGetQuery(jdbcConnection, "use database") #// mention your database name of snowflake//
res4 <- dbGetQuery(jdbcConnection, "use schema") #// mention your schema name of snowflake//
sales <- dbGetQuery(jdbcConnection, "SELECT * FROM table") #// mention your query for snowflake
agg <- dbGetQuery(jdbcConnection, "SELECT * FROM table")

getwd()
sales[is.na(sales)] <- 0
agg[is.na(agg)] <- 0

##Load in libraries and set working directory
sales$X <- NULL

sales[is.na(sales)] <- 0
agg[is.na(agg)] <- 0

sales$MODEL_GRAIN <- sales$UNIV_CATEGORY
#Transform AggData dataset for analysis
agg <- agg[complete.cases(agg),]

agg$LATITUDE <- as.numeric(agg$LATITUDE)
agg <- agg[agg$LATITUDE <= 90 & agg$LATITUDE >= -90,]

agg$LONGITUDE <- as.numeric(agg$LONGITUDE)
agg <- agg[agg$LONGITUDE <= 180 & agg$LONGITUDE >= -180,]

#Create a dataset for only own stores
agg_nike <- sqldf("select * from agg where LIST_NAME like '%N%'")

#How many stores are in each zip code?
acc <- data.frame(agg$LIST_NAME, agg$POSTAL_CODE)
names(acc) <- c("acc_name", "postal_code")
acc <- na.omit(acc)
for (i in unique(acc$acc_name)){
  acc[,paste0(i)]=ifelse(acc$acc_name==i,1,0)
}
acc$acc_name <- NULL
acc <- aggregate(acc[,2:length(acc)], by = list(acc$postal_code), FUN = sum)
names(acc)[1] <- "postal_code"

limit <- nrow(acc) *.1
acc <- acc[, which(as.numeric(colSums(acc != 0)) > limit)]

### rename columns
names(acc) <-gsub(" ", "_", names(acc))
names(acc) <-gsub(" ", "", names(acc))
names(acc) <-gsub("`", "", names(acc))
names(acc) <-gsub("&", "", names(acc))
names(acc) <-gsub("-", "", names(acc))
names(acc) <-gsub("/", "", names(acc))

#How many competitors are in each postal code? 
#comp <- aggregate(agg$COMPETITOR_FLG, by = list(agg$POSTAL_CODE), FUN = sum)
#names(comp) <- c("postal_code", "competitor_count")

#What is the count of store category for each zip code? 
store_cat <- data.frame(agg$CATEGORY, agg$POSTAL_CODE)
names(store_cat) <- c("store_category", "postal_code")
store_cat <- na.omit(store_cat)

for (i in unique(store_cat$store_category)){
  store_cat[,paste0(i)]=ifelse(store_cat$store_category==i,1,0)
}

store_cat$store_category <- NULL
store_cat <- aggregate(store_cat[,2:length(store_cat)], by = list(store_cat$postal_code), FUN = sum)
names(store_cat)[1] <- "postal_code"


#How close is the nearest store per business? Of those, how close is the nearest store? 
nearest <- data.frame(distm(agg[,c(5,4)],agg_nike[,c(5,4)], fun = distHaversine)/ 1609)
nearest <- data.frame(do.call(pmin, nearest_nike[,1:length(nearest_nike)]))
nearest <- cbind(nearest, agg$POSTAL_CODE, agg$LIST_NAME)
names(nearest) <- c("nearest_miles", "postal_code", "account")

nearest <- sqldf("select distinct a.min_dist, a.postal_code, b.account from 
                      (select min(nearest_miles) as min_dist, postal_code 
                      from nearest group by postal_code) a
                      inner join (select nearest_miles, account, postal_code
                      from nearest) b
                      on a.min_dist = b.nearest_miles
                      and a.postal_code = b.postal_code")

nearest$ID <- seq.int(nrow(nearest))

nearest <- sqldf("select distinct a.min_dist, a.postal_code, a.account from 
                      (select min_dist, postal_code, account, ID from nearest) a 
                      inner join (select min(ID) as ID, postal_code from nearest group by postal_code) b
                      on a.ID = b.ID and a.postal_code = b.postal_code")


#Merge acc, comp, nearest, & store_cat
agg2 <- join_all(list(acc,nearest,store_cat), by = 'postal_code', type = 'full')

#Join sales (includes sales & spotzi) with agg2
finaldf_all <- merge(sales, agg2, by.x = c("POSTAL_CODE"), by.y = c("postal_code"), all.x = TRUE)

## WRAP IN A FUNCTION TO RUN THROUGH THE CITIES
city <- unique(finaldf_all$CITY)
allcities <- data.frame()
accuracymeasures <- data.frame()
#finaldf_all1<- subset(finaldf_all,(!(finaldf_all$UNIV_CATEGORY=='WOMENS TRAINING' & finaldf_all$CITY=='BALTIMORE')))

for(cy in city){
  
  finaldf <- finaldf_all[finaldf_all$CITY == cy,]
  
  
  finaldf$YEAR_SSN <- finaldf$YEAR_SSN
  #Add in additional variables
  #Share of demand by category_division_gender
  cat_shareofdemand <- aggregate(finaldf$SALES_DOLLARS, by = list(finaldf$MODEL_GRAIN, finaldf$YEAR_SSN), FUN = sum)
  names(cat_shareofdemand) <- c("MODEL_GRAIN", "YEAR_SSN", "TOTAL_CAT_SEASON_SALES")
  
  finaldf <- merge(finaldf, cat_shareofdemand, by = c("MODEL_GRAIN", "YEAR_SSN"), all.x = TRUE)
  finaldf$CAT_SEASON_SHARE <- finaldf$SALES_DOLLARS/finaldf$TOTAL_CAT_SEASON_SALES
  
  #Sales per capita
  finaldf$DIG_SALES_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$finaldf$DIG_SALES / finaldf$TOTAL_POP, 0) 
  finaldf$FS_SALES_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$FS_SALES / finaldf$TOTAL_POP, 0) 
  finaldf$SO_SALES_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$SO_SALES / finaldf$TOTAL_POP, 0) 
  finaldf$PARTNER_DIG_SALES_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$PARTNER_DIG_SALES / finaldf$TOTAL_POP, 0) 
  finaldf$PARTNER_SALES_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$PARTNER_SALES / finaldf$TOTAL_POP, 0) 
  
  #units per capita
  finaldf$DIG_UNITS_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$finaldf$DIG_UNITS / finaldf$TOTAL_POP, 0) 
  finaldf$FS_UNITS_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$FS_UNITS / finaldf$TOTAL_POP, 0) 
  finaldf$SO_UNITS_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$SO_UNITS / finaldf$TOTAL_POP, 0) 
  finaldf$PARTNER_DIG_UNITS_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$PARTNER_DIG_UNITS / finaldf$TOTAL_POP, 0) 
  finaldf$PARTNER_UNITS_percap <- ifelse(finaldf$TOTAL_POP != 0, finaldf$PARTNER_UNITS / finaldf$TOTAL_POP, 0) 
  
  #AUR
  finaldf$DIG_AUR <- finaldf$DIG_SALES / finaldf$DIG_UNITS
  finaldf$FS_AUR <- finaldf$FS_SALES / finaldf$FS_UNITS
  finaldf$SO_AUR <- finaldf$SO_SALES / finaldf$SO_UNITS
  finaldf$PARTNER_DIG_AUR <- finaldf$PARTNER_DIG_SALES / finaldf$PARTNER_DIG_UNITS
  finaldf$PARTNER_AUR <- finaldf$PARTNER_SALES / finaldf$PARTNER_UNITS
  
  
  #Population percentage
  finaldf$age0_14_percent <- ifelse(finaldf$TOTAL_POP != 0, finaldf$TOTAL_POP_AGE0_14/finaldf$TOTAL_POP, 0) 
  finaldf$age15_29_percent <- ifelse(finaldf$TOTAL_POP != 0, finaldf$TOTAL_POP_AGE15_29/finaldf$TOTAL_POP, 0) 
  finaldf$age30_44_percent <- ifelse(finaldf$TOTAL_POP != 0, finaldf$TOTAL_POP_AGE30_44/finaldf$TOTAL_POP, 0) 
  finaldf$age45_59_percent <- ifelse(finaldf$TOTAL_POP != 0, finaldf$TOTAL_POP_AGE45_59/finaldf$TOTAL_POP, 0) 
  finaldf$age60UP_percent <- ifelse(finaldf$TOTAL_POP != 0, finaldf$TOTAL_POP_AGE60UP/finaldf$TOTAL_POP, 0) 
  
  finaldf$MALE_POP_PERC <- ifelse(finaldf$FEMALE_POP != 0, finaldf$MALE_POP / (finaldf$MALE_POP + finaldf$FEMALE_POP), 0) 
  finaldf$FEMALE_POP_PERC <- ifelse(finaldf$MALE_POP != 0, finaldf$FEMALE_POP / (finaldf$MALE_POP + finaldf$FEMALE_POP), 0) 
  
  
  #Add in a seasonal flag
  # finaldf$SPRING<- ifelse(finaldf$MP_SEASON == "SP", 1,0)
  # finaldf$SUMMER<- ifelse(finaldf$MP_SEASON == "SU", 1,0)
  # finaldf$FALL<- ifelse(finaldf$MP_SEASON == "FA", 1,0)
  # finaldf$HOLIDAY<- ifelse(finaldf$MP_SEASON == "HO", 1,0)
  # 
  
  #What is the consumer spend per footwear door & per clothing door? 
  
  finaldf$footwear_spend_per_door <- ifelse(finaldf$footwear != 0, finaldf$FOOTWEAR_SPOTZI /finaldf$footwear,0)
  finaldf$apparel_spend_per_door <- ifelse(finaldf$clothing != 0, finaldf$CLOTHING_SPOTZI/finaldf$clothing,0)
  
  
  #### Rename columns
  names(finaldf) <-gsub(" ", "_", names(finaldf))
  names(finaldf) <-gsub(" ", "", names(finaldf))
  names(finaldf) <-gsub("`", "", names(finaldf))
  names(finaldf) <-gsub("&", "", names(finaldf))
  names(finaldf) <-gsub("-", "", names(finaldf))
  names(finaldf) <-gsub("/", "", names(finaldf))
  
  #Remove redundancy before modeling
  finaldf$DIG_SALES <- NULL
  finaldf$FS_SALES <- NULL
  finaldf$SO_COUNT <- NULL
  finaldf$PARTNER_DIG_SALES <- NULL
  finaldf$PARTNER_SALES <- NULL
  finaldf$NET_SALES_UNITS <- NULL
  
  
  finaldf$TOTAL_CAT_SEASON_SALES <- NULL
  
  finaldf$FEMALE_POP <- NULL
  finaldf$MALE_POP <- NULL
  finaldf$TOTAL_POP_AGE0_14 <- NULL
  finaldf$TOTAL_POP_AGE15_29 <- NULL
  finaldf$TOTAL_POP_AGE30_44 <- NULL
  finaldf$TOTAL_POP_AGE45_59 <- NULL
  finaldf$TOTAL_POP_AGE60UP <- NULL
  
  finaldf$UNIV_CATEGORY <- NULL
  #finaldf$POSTAL_CODE <- NULL
  
  finaldf$ROW_NUMBER <- NULL
  finaldf$X <- NULL
  #finaldf$YEAR_SSN <- NULL
  #finaldf$MP_SEASON <- NULL
  #finaldf$SPOTZI_YEAR <- NULL
  
  finaldf$DISPOSABLE_INCOME <- NULL
  finaldf$DISPOSABLE_INCOME_INDEX <- NULL
  finaldf$FOOTWEAR_SPOTZI <- NULL
  finaldf$FOOTWEAR_INDEX <- NULL
  finaldf$CLOTHING_SPOTZI <- NULL
  finaldf$CLOTHING_INDEX <- NULL
  finaldf$REC_HOBBIES_SPOTZI <- NULL
  finaldf$RECREATION_HOBBIES_INDEX <- NULL
  finaldf$account <- NULL
  finaldf$closed <- NULL
  
  finaldf$BOTTOM <- NULL
  finaldf$HEADWEAR <- NULL
  finaldf$OTHER_SILHOUETTE <- NULL
  finaldf$EYEWEAR <- NULL
  finaldf$EYEWEAR <- NULL
  finaldf$SHOE <- NULL
  finaldf$TOP <- NULL
  
  #finaldf$DIVISION <- NULL
  #finaldf$GENDER <- NULL
  
  finaldf[is.na(finaldf)] <- 0
  
  finaldf <- finaldf[finaldf$MODEL_GRAIN != "HURLEY" & finaldf$MODEL_GRAIN != "OTHER",]
  
  #finaldf$MODEL_GRAIN <- NULL
  finaldf$EQUIPMENT_DIVISION <- NULL
  
  
  finaldf <- finaldf[finaldf$SALES_DOLLARS > 0,]
  
  category <- unique(finaldf$MODEL_GRAIN)
  output <- data.frame()
  test2 <- data.frame()
  rmse <- data.frame()
  accuracy_output2 <- data.frame()
  for(c in category){
    
    finaldf2 <- finaldf[finaldf$MODEL_GRAIN == c,]
    #finaldf2 <- finaldf2[finaldf2$SALES_DOLLARS > (quantile(finaldf2$SALES_DOLLARS)[4]),]
    print("=======")
    print(c)
    print("Results")
    
    finaldf3 <- data.frame(finaldf2$SALES_DOLLARS, finaldf2$MODEL_GRAIN, finaldf2$POSTAL_CODE, finaldf2$YEAR_SSN, finaldf2$DIVISION, finaldf2$GENDER, finaldf2$CITY)
    names(finaldf3) <- c("SALES_DOLLARS", "MODEL_GRAIN", "POSTAL_CODE", "YEAR_SSN", "DIVISION", "GENDER", "CITY")
    
    finaldf2 <- Filter(function(x) length(unique(x))>1, finaldf2)
    
    notfactor <- finaldf2[, sapply(finaldf2, class) != 'character' & sapply(finaldf2, class) != 'factor']
    notfactor$POSTAL_CODE <- NULL
    
    giantcor <- cor(notfactor)
    giantcor[is.na(giantcor)] <- 0
    hc = findCorrelation(giantcor, cutoff=0.75)
    hc = sort(hc)
    reduced_Data <- notfactor[,-c(hc)]
    
    finalred <- cbind(finaldf3, reduced_Data)
    is.na(finalred)<-sapply(finalred, is.infinite)
    finalred[is.na(finalred)] <- 0
    
    set.seed(123)
    sample <- sort(sample(nrow(finalred), nrow(finalred) *.7))
    train <- finalred[sample,]
    train <- train[train$SALES_DOLLARS > 0,]
    test <- finalred[-sample,]
    
    summary(test$SALES_DOLLARS)
    
    min <- min(test$SALES_DOLLARS)
    max <- max(test$SALES_DOLLARS)
    
    train$MODEL_GRAIN <- NULL
    train$YEAR_SSN<- NULL
    train$DIVISION <- NULL
    train$GENDER <- NULL
    train$CITY <- NULL
    
    fit <- lm(train$SALES_DOLLARS ~. -POSTAL_CODE, data = train, na.action=na.exclude)
    
    ss <- coef(summary(fit))
    ss_sig <- ss[ss[,"Pr(>|t|)"]<.05,]
    names <- rownames(ss_sig)
    names.use <- names(train)[(names(train) %in% names)]
    train2 <- train[,names.use]
    
    fit2 <- lm(train$SALES_DOLLARS ~., data = train2)
    
    #ss2 <- coef(summary(fit2))
    #ss_sig2 <- ss2[ss2[,"Pr(>|t|)"]<.05,]
    #names2 <- rownames(ss_sig2)
    #names.use2 <- names(train2)[(names(train2) %in% names2)]
    #train3 <- train2[,names.use2]
    
    #fit3 <- lm(train$SALES_DOLLARS ~., data = train2)
    
    ### VIFS
    VIFs <- (car::vif(fit2))
    multicoll <- VIFs > 10
    multicoll <- multicoll[multicoll == TRUE]
    usenames <- names(VIFs)[!names(VIFs) %in% names(multicoll)]
    ## rerun
    usenames2 <- names(train2)[names(train2) %in% usenames]
    train4 <- train2[,usenames2]
    
    fit4 <- lm(train$SALES_DOLLARS ~., data = train4)
    ss4 <- coef(summary(fit4))
    ss_sig4 <- ss4[ss4[,"Pr(>|t|)"]<.05,]
    names4 <- rownames(ss_sig4)
    names.use4 <- names(train4)[(names(train4) %in% names4)]
    train5 <- train4[,names.use4]
    
    
    #fit_final <- lm(train$SALES_DOLLARS ~., data = train5)
    fit_final_sqt <- lm((train$SALES_DOLLARS) ~., data = train5)
    
    print(summary(fit_final_sqt))
    #print((car::vif(fit_final)))
    #print(varImp(fit_final))
    
    
    test$PRED <- (predict(fit_final_sqt, newdata = test, type = "response"))
    finalred$PRED <- (predict(fit_final_sqt, newdata = finalred, type = "response"))
    
    summary(test$PRED)
    summary(finalred$PRED)
    
    keep <- c("POSTAL_CODE", "YEAR_SSN", "MODEL_GRAIN", "DIVISION", "GENDER", "SALES_DOLLARS", "PRED", "CITY")
    category2 <- finalred[,keep]
    
    keep_test <- c("POSTAL_CODE", "YEAR_SSN", "MODEL_GRAIN", "DIVISION", "GENDER", "SALES_DOLLARS", "PRED", "CITY")
    cattest <- test[,keep_test]
    
    output <- rbind(output, category2)
    
    summary(output$SALES_DOLLARS)
    
    rmse <- data.frame(c, rmse(test$SALES_DOLLARS, test$PRED), summary(fit_final_sqt)$adj.r.squared)
    names(rmse) <- c("category", "RMSE", "R2")
    accuracy <- data.frame(min, max)
    accuracy2 <- data.frame(rmse, accuracy)
    accuracy2$Normalized_percentage <- (accuracy2$RMSE/(accuracy2$max - accuracy2$min)) * 100
    accuracy2$CITY <- cy
    accuracy_output2 <- rbind(accuracy_output2, accuracy2)
  } 
  allcities <- rbind(allcities, output)
  accuracymeasures <- rbind(accuracymeasures, accuracy_output2)
}


accuracymeasures$Normalized_percentage <- (accuracymeasures$RMSE/(accuracymeasures$max - accuracymeasures$min)) * 100
accuracymeasures


allcities$Delta <- allcities$PRED - allcities$SALES_DOLLARS
#allcities$DeltaSqrt <- allcities$SQRTPRED - allcities$SALES_DOLLARS
allcities$Opportunity <- allcities$Delta
allcities$Opportunity[allcities$Opportunity <= 0] <- 0
#allcities$OpportunitySqrt <- allcities$DeltaSqrt
#allcities$OpportunitySqrt[allcities$OpportunitySqrt <= 0] <- 0
allcities$Over_performing <- allcities$Delta
allcities$Over_performing[allcities$Over_performing >= 0] <- 0
allcities$Over_performing <- abs(allcities$Over_performing)

###allcities <- allcities[allcities$YEAR_SSN %like% '18' ,]
#allcities<- allcities[allcities$TRUNC_POSTAL_CODE != "SW1E 5",]

sqldf("Select MODEL_GRAIN, sum(Opportunity), SUM(SALES_DOLLARS), SUM(PRED) from allcities where YEAR_SSN LIKE '%18%' and CITY LIKE 'LOS ANGELES'
      GROUP BY MODEL_GRAIN")

sqldf("Select SUM(OPPORTUNITY) from allcities where YEAR_SSN LIKE '%18%' and CITY LIKE 'NEW YORK'")
sqldf("Select sum(SALES_DOLLARS) from allcities where YEAR_SSN like '%18%' and CITY LIKE 'NEW YORK'")

#sqldf("Select sum(Opportunity) from outputfinal_grouped where YEAR_SSN like '%18'")
#sqldf("Select sum(OpportunitySqrt) from output where YEAR_SSN like '%18'")

sqldf("Select CITY, sum(Opportunity), SUM(SALES_DOLLARS), SUM(PRED) from allcities where YEAR_SSN LIKE '%18%'
      GROUP BY CITY")


##########################################################################################################################################################
##########################################################################################################################################################
##########################################################################################################################################################
getwd()
outputfile <- paste("/home/",  "ALLNACITIES_CategoryOutput", Sys.Date(), ".xlsx", sep = "")
write.xlsx(allcities, outputfile)

accuracyfile <- paste("/home/",  "ALLNACITIES_AccuracyMeasures", Sys.Date(), ".xlsx", sep = "")
write.xlsx(accuracymeasures, accuracyfile)



data <- dbWriteTable(jdbcConnection,"NA_DIG_OPP",allcities) #dbWriteTable ( connection_name, table_name_on_snowflake, data_frame_name )

