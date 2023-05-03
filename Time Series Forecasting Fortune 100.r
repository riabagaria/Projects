## Forecasting IMP Opportunity#####

## LOADING REQUIED LIBRARIES ##
library("forecast")
library("dplyr")
library("sqldf")
library("lubridate")
library("tidyverse")
library(zoo)
library(rlist)
library(doParallel)
library(data.table)



## READ INPUT DATA ##

#For Tokyo, use tokyo_data
library("RJDBC")
options(java.parameters = "-Xmx16g")
#Establish connection to snowflake
jdbcDriver <- JDBC(driverClass="com.snowflake.client.jdbc.SnowflakeDriver",
                   classPath="/home/")
jdbcConnection <- dbConnect(jdbcDriver, "jdbc:snowflake://",
                            "your mail id", getPass::getPass("Enter your password: "))

res1 <- dbGetQuery(jdbcConnection, "use role") #// mention your Role name of snowflake //
res2 <- dbGetQuery(jdbcConnection, "use warehouse") #// mention your wareHOuse name of snowflake //
res3 <- dbGetQuery(jdbcConnection, "use database") #// mention your database name of snowflake//
res4 <- dbGetQuery(jdbcConnection, "use schema") #// mention your schema name of snowflake//
res5 <- dbGetQuery(jdbcConnection, "SELECT POSTAL_CODE,
                   CATEGORY,
                   GENDER,
                   DIVISION,
                   SEASON AS MP_YEAR_SEASON,
                   SUM(OPPORTUNITY) AS OPPORTUNITY
                   FROM NA_DIG_OPP_PRED_WO_PARTNER_1010
                   WHERE 1=1
                   GROUP BY POSTAL_CODE,
                   CATEGORY,
                   GENDER,
                   DIVISION,
                   SEASON ;")

N_output <- res5

#For NA, use na_data
#na_data <- read.csv("NA_2015_TO_2019_TOT_OPP_8_1.csv", stringsAsFactors = FALSE)
#N_output <- na_data


## CREATING YEAR QUARTER COLUMN ##

N_output$QUARTER <- N_output$MP_YEAR_SEASON 	#format of MP_YEAR_SEASON = '2018SP'
N_output$QUARTER <- substr(N_output$QUARTER, start = 5, stop = 6)
N_output$QUARTER[N_output$QUARTER == "SP"] <- 1
N_output$QUARTER[N_output$QUARTER == "SU"] <- 2
N_output$QUARTER[N_output$QUARTER == "FA"] <- 3
N_output$QUARTER[N_output$QUARTER == "HO"] <- 4

N_output$YEAR <- N_output$MP_YEAR_SEASON
N_output$YEAR <- substr(N_output$YEAR, start = 3, stop = 4)		#format of MP_YEAR_SEASON = '2018SP'
N_output$TS_SEASON_YEAR <- paste(N_output$QUARTER, N_output$YEAR, sep = "-")
N_output$TS_SEASON_YEAR <- as.Date(as.yearqtr(N_output$TS_SEASON_YEAR, format = "%q-%y"))

### DROP THE CURRENT QUARTER
currquarter <- substr(quarters(as.Date(Sys.Date())), 2, 2)
curryear <-lubridate::year(Sys.Date())
curryear <- substr(curryear, start = 3, stop = 4 )
N_output <- N_output[!(N_output$QUARTER == currquarter  & N_output$YEAR == curryear), ]

## STEPS
# DROP THE AREAS WITH LESS THAN 2
# THEN REFILL IN THE DATES
# AVERAGE THE REMAINING

N_opp <- data.frame(N_output$TS_SEASON_YEAR, N_output$CATEGORY, N_output$OPPORTUNITY, N_output$DIVISION, N_output$GENDER, N_output$POSTAL_CODE)
names(N_opp) <- c("TS_SEASON_YEAR", "CATEGORY", "Opportunity", "DIVISION", "GENDER", "POSTAL_CODE")

N_opp <- N_opp %>%
  group_by(TS_SEASON_YEAR, CATEGORY, DIVISION, GENDER, POSTAL_CODE) %>%
  summarise_all(funs(sum))


#N_opp <- N_opp[N_opp$Opportunity > 0, ]

### Filter out too short time series
appearances <- sqldf("select CATEGORY, COUNT(TS_SEASON_YEAR) AS APPEARANCES, DIVISION, GENDER, POSTAL_CODE 
                     FROM N_opp group by DIVISION, GENDER, CATEGORY, POSTAL_CODE")

tooshort <- sqldf("select a.* from N_opp A
                  inner join (select CATEGORY, POSTAL_CODE, DIVISION, GENDER FROM appearances where APPEARANCES < 2) B
                  ON A.CATEGORY = B.CATEGORY
                  AND A.POSTAL_CODE = B.POSTAL_CODE
                  AND A.GENDER = B.GENDER
                  AND A.DIVISION = B.DIVISION")

N_opp <- sqldf("select a.* from N_opp A
                  inner join (select CATEGORY, POSTAL_CODE, DIVISION, GENDER FROM appearances where APPEARANCES >= 2) B
                  ON A.CATEGORY = B.CATEGORY
                  AND A.POSTAL_CODE = B.POSTAL_CODE
                  AND A.DIVISION = B.DIVISION
                  AND A.GENDER =B.GENDER")

## Fill in missing dates
N_opp <- N_opp %>%
  group_by(CATEGORY, DIVISION, GENDER, POSTAL_CODE) %>%
  complete(TS_SEASON_YEAR = seq.Date(as.Date('2015-01-01'), as.Date('2019-04-01'), by="quarter"))

# N_opp2 <- sqldf("select a.CATEGORY, a.POSTAL_CODE, a.GENDER, a.DIVISION,a.Opportunity, b.TS_SEASON_YEAR from N_opp a
#                    cross join (select ts_season_year from N_opp group by ts_season_year) b")

N_opp[is.na(N_opp)] <- 0
N_opp$YEAR <- year(N_opp$TS_SEASON_YEAR)

# change any Opportunity = 0 to the average opportunity for that year
mutate <- sqldf("SELECT CATEGORY, DIVISION, GENDER, POSTAL_CODE,YEAR, CASE WHEN Opportunity = 0 then AVG(Opportunity) else Opportunity end as Opportunity
                FROM N_opp 
                group by CATEGORY, DIVISION, GENDER, POSTAL_CODE, YEAR")

mutate2 <- sqldf("SELECT CATEGORY, DIVISION, GENDER, POSTAL_CODE, CASE WHEN Opportunity = 0 then AVG(Opportunity) else Opportunity end as Opportunity
                 from N_opp group by CATEGORY, DIVISION, GENDER, POSTAL_CODE")

N_opp <- sqldf("SELECT A.CATEGORY, A.DIVISION, A.GENDER, A.POSTAL_CODE, A.TS_SEASON_YEAR, CASE WHEN A.Opportunity = 0 then B.Opportunity else A.Opportunity end as Opportunity
                  FROM N_opp A
                  INNER JOIN mutate B
                  ON A.CATEGORY = B.CATEGORY
                  AND A.DIVISION = B.DIVISION
                  AND A.GENDER = B.GENDER
                  AND A.POSTAL_CODE = B.POSTAL_CODE
                  AND A.YEAR = B.YEAR
                  ")

N_opp <- sqldf("SELECT A.CATEGORY, A.DIVISION, A.GENDER, A.POSTAL_CODE, A.TS_SEASON_YEAR, CASE WHEN A.Opportunity = 0 then (B.Opportunity+1) else A.Opportunity end as Opportunity
                  FROM N_opp A
                  INNER JOIN mutate2 B
                  ON A.CATEGORY = B.CATEGORY
                  AND A.DIVISION = B.DIVISION
                  AND A.GENDER = B.GENDER
                  AND A.POSTAL_CODE = B.POSTAL_CODE
                  ")

##############################################################################

postal <- unique(N_opp$POSTAL_CODE)

no_cores <- detectCores() - 1  
cl <- parallel::makeCluster(no_cores, type="FORK")  
registerDoParallel(cl) 


f_opp <- foreach(p = postal, .combine = rbind) %dopar%{
  min_ape_lst <- list()
  oneseason <- data.frame()
  noforecast_output <- data.frame()
  postal_codes <- N_opp[N_opp$POSTAL_CODE == p,]
  category <- unique(postal_codes$CATEGORY)
  for(c in category){
    sales <- postal_codes[postal_codes$CATEGORY == c,]
    print("=========category ID==========")
    print(c)
    print("===========================")
    gender <- unique(sales$GENDER)
    for(g in gender){
      gen <- sales[sales$GENDER == g,]
      print("========GENDER============")
      print(g)
      print("===========================")
      division <- unique(gen$DIVISION)
      for(d in division){
        div <- gen[gen$DIVISION ==  d,]
        print("========DIVISION============")
        print(d)
        print("===========================")
        div <- div[div$Opportunity > 0,]
        
        start_date <- as.Date(min(div$TS_SEASON_YEAR))
        
        opp_ts <- ts(div$Opportunity, frequency = 4, start = as.yearqtr(min(div$TS_SEASON_YEAR)))
        print(min(div$TS_SEASON_YEAR))
        print(max(div$TS_SEASON_YEAR))
        k <- 8 # minimum data length for fitting a model ... 2 years + 2 seasons = 10 seasons
        z <- length(opp_ts) - (k+6) # 
        if (z <= 0) {
          min_ape_lst <- list.append(min_ape_lst, NULL)
          print(opp_ts)
          print("next")
          print("====================")
          #noforecast <- data.frame(div$CATEGORY, div$DIVISION, div$GENDER, div$POSTAL_CODE)
          #names(noforecast) <- c("category", "division", "gender", "postal_code")
          #noforecast <- sqldf("select distinct category, division, gender, postal_code from noforecast")
          #noforecast_output <- rbind(noforecast_output, noforecast)
          #print(noforecast_output)
          next
        }
        
        h <- 18
        ape1_lst <- vector("list", z)
        ape2_lst <- vector("list", z)
        ape4_lst <- vector("list", z)
        
        ape1_meanlst <- vector("list", z)
        ape2_meanlst <- vector("list", z)
        ape4_meanlst <- vector("list", z)
        
        
        for(i in 1:z){
          train <- opp_ts[0: (k+i)] 
          #print(train)
          test <- opp_ts[(k+i+1) : (k+i+h)] 
          #print(test)
          train_ts <- ts(train, freq = 4) 
          #print(train_ts)
          
          for(t in 1:4){
            fit1 <- tslm(train_ts ~ trend + season, lambda=0)
            fcast1 <- forecast(fit1, h=h)
            print("forecast")
            print(fcast1)
            fcst1 <- fcast1$mean[t]
            #print(fcst1)
            actual1 <- test[t]
            print("actual1")
            print(actual1)
            ape1 <- abs(fcst1 - actual1) / actual1
            ape1_lst[[i]][[t]] = ape1		#ADDED ANOTHER PARAMETER 't'
            print(ape1)
            
            fit2 <- ets(train_ts,model="ZZZ",damped=FALSE)
            fcast2 <- forecast(fit2, h=h)
            fcst2 <- fcast2$mean[t]
            actual2 <- test[t]
            ape2 <- abs(fcst2 - actual2) / actual2
            ape2_lst[[i]][[t]] = ape2		#ADDED ANOTHER PARAMETER 't'
            
            fcast4 <- stlf(train_ts, s.window = "periodic", h = h)
            fcst4 <- fcast4$mean[t]
            actual4 <- test[t]
            #print("actual4")
            #print(actual4)
            ape4 <- abs(fcst4 - actual4) / actual4
            ape4_lst[[i]][[t]] = ape4		#ADDED ANOTHER PARAMETER 't'
          }
          
          #TAKING MEAN OF THE MAPE VALUES OBTAINED ABOVE FOR EACH OF THE THREE MODELS
          ape1_meanlst[[i]] <- mean(as.numeric(ape1_lst[[i]]),na.rm=TRUE)
          ape2_meanlst[[i]] <- mean(as.numeric(ape2_lst[[i]]),na.rm=TRUE)
          ape4_meanlst[[i]] <- mean(as.numeric(ape4_lst[[i]]),na.rm=TRUE)
          
        }
        
        #TO CALCULATE SEASON OUT MAPE VALUES (1ST SEASON OUT, 2ND SEASON OUT, 3RD SEASON OUT & 4TH SEASON OUT)
        
        n=4 	#TAKING FIRST 12 SEASONS AS TRAINING
        train_np <- opp_ts[0: (k+n)] 
        print("train")
        print(train_np)
        test_np <- opp_ts[(k+n+1) : (k+n+h)] 
        print("test")
        print(test_np)
        train_ts_np <- ts(train_np, freq = 4) 
        #print(train_ts)
        
        #tslm forecast model
        fit1_np <- tslm(train_ts_np ~ trend + season, lambda=0)
        fcast1_np <- forecast(fit1_np, h=h)
        print("tslm")
        print(fcast1_np)
        fcst1_1sn <- fcast1_np$mean[1] 	#1st season out forecast value
        fcst1_2sn <- fcast1_np$mean[2]	#2nd season out	forecast value
        fcst1_3sn <- fcast1_np$mean[3]	#3rd season out forecast value
        fcst1_4sn <- fcast1_np$mean[4]	#4th season out forecast value
        actual1_1sn <- test_np[1]
        actual1_2sn <- test_np[2]
        actual1_3sn <- test_np[3]
        actual1_4sn <- test_np[4]
        # print("actual1")
        # print(actual1)
        ape1_1sn <- abs(fcst1_1sn - actual1_1sn) / actual1_1sn	#1st season out APE
        ape1_2sn <- abs(fcst1_2sn - actual1_2sn) / actual1_2sn	#2nd season out APE
        ape1_3sn <- abs(fcst1_3sn - actual1_3sn) / actual1_3sn	#3rd season out APE
        ape1_4sn <- abs(fcst1_4sn - actual1_4sn) / actual1_4sn	#4th season out APE
        
        #ets forecast model
        fit2_np <- ets(train_ts_np,model="ZZZ",damped=FALSE)
        fcast2_np <- forecast(fit2_np, h=h)
        print("ets")
        print(fcast2_np)
        fcst2_1sn <- fcast2_np$mean[1]	#1st season out forecast value 
        fcst2_2sn <- fcast2_np$mean[2]	#2nd season out forecast value
        fcst2_3sn <- fcast2_np$mean[3]	#3rd season out forecast value
        fcst2_4sn <- fcast2_np$mean[4]	#4th season out forecast value
        actual2_1sn <- test_np[1]
        actual2_2sn <- test_np[2]
        actual2_3sn <- test_np[3]
        actual2_4sn <- test_np[4]
        # print("actual2")
        # print(actual2)
        ape2_1sn <- abs(fcst2_1sn - actual2_1sn) / actual2_1sn	#1st season out APE
        ape2_2sn <- abs(fcst2_2sn - actual2_2sn) / actual2_2sn	#2nd season out APE
        ape2_3sn <- abs(fcst2_3sn - actual2_3sn) / actual2_3sn	#3rd season out APE
        ape2_4sn <- abs(fcst2_4sn - actual2_4sn) / actual2_4sn	#4th season out APE
        
        #stlf forecast model
        fcast4_np <- stlf(train_ts_np, s.window = "periodic", h = h)
        print("stlf")
        print(fcast4_np)
        fcst4_1sn <- fcast4_np$mean[1]	#1st season out forecast value
        fcst4_2sn <- fcast4_np$mean[2]	#2nd season out forecast value
        fcst4_3sn <- fcast4_np$mean[3]	#3rd season out forecast value
        fcst4_4sn <- fcast4_np$mean[4]	#4th season out forecast value
        actual4_1sn <- test_np[1]
        actual4_2sn <- test_np[2]
        actual4_3sn <- test_np[3]
        actual4_4sn <- test_np[4]
        # print("actual4")
        # print(actual4)
        ape4_1sn <- abs(fcst4_1sn - actual4_1sn) / actual4_1sn	#1st season out APE
        ape4_2sn <- abs(fcst4_2sn - actual4_2sn) / actual4_2sn	#2nd season out APE
        ape4_3sn <- abs(fcst4_3sn - actual4_3sn) / actual4_3sn	#3rd season out APE
        ape4_4sn <- abs(fcst4_4sn - actual4_4sn) / actual4_4sn	#4th season out APE
        
        
        #TO SELECT THE BEST MODEL WITH LEAST MAPE  (replace ape1_lst with ape1_meanlst and so on)
        winner <- if(mean(as.numeric(ape1_meanlst), na.rm=TRUE) < mean(as.numeric(ape2_meanlst), na.rm=TRUE)
                     & mean(as.numeric(ape1_meanlst), na.rm=TRUE) < mean(as.numeric(ape4_meanlst), na.rm=TRUE)) {"tslm"} 
        else if(mean(as.numeric(ape2_meanlst), na.rm=TRUE) < mean(as.numeric(ape1_meanlst), na.rm=TRUE)
                & mean(as.numeric(ape2_meanlst), na.rm=TRUE) < mean(as.numeric(ape4_meanlst), na.rm=TRUE)) {'ets'}
        else {'stlf'}
        
        min_ape <- min(mean(as.numeric(ape1_meanlst), na.rm=TRUE), mean(as.numeric(ape2_meanlst), na.rm=TRUE), mean(as.numeric(ape4_meanlst), na.rm=TRUE))
        
        #FIRST SEASON OUT MAPE
        mape_1sn <- if(winner == 'tslm'){as.numeric(ape1_1sn)}
        else if(winner == 'ets'){as.numeric(ape2_1sn)}
        else if(winner == 'stlf'){as.numeric(ape4_1sn)}
        
        #SECOND SEASON OUT MAPE	
        mape_2sn <- if(winner == 'tslm'){as.numeric(ape1_2sn)}
        else if(winner == 'ets'){as.numeric(ape2_2sn)}
        else if(winner == 'stlf'){as.numeric(ape4_2sn)}
        
        #THIRD SEASON OUT MAPE						
        mape_3sn <- if(winner == 'tslm'){as.numeric(ape1_3sn)}
        else if(winner == 'ets'){as.numeric(ape2_3sn)}
        else if(winner == 'stlf'){as.numeric(ape4_3sn)}
        #FOURTH SEASON OUT MAPE			
        mape_4sn <- if(winner == 'tslm'){as.numeric(ape1_4sn)}
        else if(winner == 'ets'){as.numeric(ape2_4sn)}
        else if(winner == 'stlf'){as.numeric(ape4_4sn)}
        
        
        model <- if(winner == 'tslm'){
          ts_winner <- div
          finaltswin <- ts(ts_winner$Opportunity, frequency = 4, start = as.yearqtr(min(div$TS_SEASON_YEAR)))
          final_fit <- tslm(finaltswin ~ trend + season, lambda=0)
          fcast <- data.frame(forecast(final_fit, h = h))
          postal <- p
          category <- c
          division <- d
          gender <- g
          finalm<-cbind(fcast, postal, category, division, gender, winner,  min_ape, mape_1sn, mape_2sn, mape_3sn, mape_4sn)
        } else if(winner == 'ets'){
          ts_winner <- div
          finaltswin <- ts(ts_winner$Opportunity, frequency = 4, start = as.yearqtr(min(div$TS_SEASON_YEAR)))
          final_fit <- ets(finaltswin,model="ZZZ",damped=FALSE)
          fcast <- data.frame(forecast(final_fit, h = h))
          postal <- p
          category <- c
          division <- d
          gender <- g
          finalm<-cbind(fcast, postal, category, division, gender, winner,  min_ape, mape_1sn, mape_2sn, mape_3sn, mape_4sn)
        } else if(winner == 'stlf'){
          ts_winner <- div
          finaltswin <- ts(ts_winner$Opportunity, frequency = 4, start = as.yearqtr(min(div$TS_SEASON_YEAR)))
          fcast <- data.frame(stlf(finaltswin, s.window = "periodic", h = h))
          postal <- p
          category <- c
          division <- d
          gender <- g
          finalm<-cbind(fcast, postal, category, division, gender, winner,  min_ape, mape_1sn, mape_2sn, mape_3sn, mape_4sn)
        }
        print("model type")
        print(winner)
        print("==========")
        print("min_ape")
        print(min_ape)
        print("==========")
        print("finaldata")
        print(finaltswin)
        print("model")
        print(model)
        
        min_ape_lst <- list.append(min_ape_lst, c(winner, min_ape, model))
        oneseason <- rbind(oneseason, model)
      }
    }
  }
  oneseason
}

stopCluster() 

f <- f_opp

f$name <- row.names(f)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2019 Q1" ,"SP19", f$name)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2019 Q2" ,"SU19", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2019 Q3" ,"FA19", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2019 Q4" ,"HO19", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2020 Q1" ,"SP20", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2020 Q2" ,"SU20", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2020 Q3" ,"FA20", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2020 Q4" ,"HO20", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2021 Q1" ,"SP21", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2021 Q2" ,"SU21", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2021 Q3" ,"FA21", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2021 Q4" ,"HO21", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2022 Q1" ,"SP22", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2022 Q2" ,"SU22", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2022 Q3" ,"FA22", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2022 Q4" ,"HO22", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2023 Q1" ,"SP23", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2023 Q2" ,"SU23", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2023 Q3" ,"FA23", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2023 Q4" ,"HO23", f$CALENDAR_MP_YEAR_SEASON)
f$CALENDAR_MP_YEAR_SEASON <- ifelse(f$name %like% "2018 Q4" ,"HO18", f$CALENDAR_MP_YEAR_SEASON)


f$name2 <- row.names(f)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2019 Q1" ,"SP19", f$name2)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2019 Q2" ,"SU19", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2019 Q3" ,"FA20", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2019 Q4" ,"HO20", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2020 Q1" ,"SP20", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2020 Q2" ,"SU20", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2020 Q3" ,"FA21", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2020 Q4" ,"HO21", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2021 Q1" ,"SP21", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2021 Q2" ,"SU21", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2021 Q3" ,"FA22", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2021 Q4" ,"HO22", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2022 Q1" ,"SP22", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2022 Q2" ,"SU22", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2022 Q3" ,"FA23", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2022 Q4" ,"HO23", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2023 Q1" ,"SP23", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2023 Q2" ,"SU23", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2023 Q3" ,"FA24", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2023 Q4" ,"HO24", f$RETAIL_YEAR_SSN)
f$RETAIL_YEAR_SSN <- ifelse(f$name %like% "2018 Q4" ,"HO19", f$RETAIL_YEAR_SSN)


f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "19" ,"FY19", f$name)
f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "20" ,"FY20", f$FY)
f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "21" ,"FY21", f$FY)
f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "22" ,"FY22", f$FY)
f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "23" ,"FY23", f$FY)
f$FY <- ifelse(f$RETAIL_YEAR_SSN %like% "24" ,"FY24", f$FY)

 f$name2 <- NULL
f$RETAIL_YEAR_SSN <- NULL
# #f_backup <- f 
names(f) <- c("OPP_FORECAST", "LOW_80", "HIGH_80", "LOW_95", "HIGH_95", "POSTAL_CODE", "CATEGORY", "DIVISION",
              "GENDER", "WINNER", "MAPE", "FIRST_SEASON_OUT_MAPE_SP18", "SECOND_SEASON_OUT_MAPE_SU18",
              "THIRD_SEASON_OUT_MAPE_FA18", "FOURTH_SEASON_OUT_MAPE_HO18","YEAR_QUARTER", "CALENDAR_MP_YEAR_SEASON","FY")
# 
write.csv(f, "~/NA_DGT_OPP_FCST_2023.csv",row.names=FALSE)
