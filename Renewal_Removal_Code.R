setwd("C:/Users/Practo/Dropbox/Daily_Reporting_Dashboard")

################ Setting up Libraries
library(RMySQL) #Loads required libraries
source("../Daily_Reporting_Dashboard/R/functions.R") #Loads required libraries
print("Initial Libraries loaded.")

############### Connecting to the database through credentials
source("../Daily_Reporting_Dashboard/R/db_creds.R") #All the db connection info
con.payment <- mysqlConnect(paymentCreds5)
print("Connection to database established. Going to read practice_doctors table.")


o22<- dbGetQuery(con.payment , "select distinct ord.id as 'Order_ID',
                subs.practice_id as 'Practice_ID',
                subs.plan_id as 'Plan_ID',
                ord.placed_at as 'OrderDate',
                subs.start_date as 'StartDate',
                subs.duration_days as 'Duration_Days',
                subs.original_duration_months as 'Duration_Months',
                subs.revoked as 'Revoked',
                adct.name as 'SCity',
                adzn.name as 'SZone',
                adkw.name as 'SKeyword',
                adpln.fabric_ad_code 'Ad-Code'
                from payment.orders ord
                left join payment.payments pymts on pymts.id=ord.payment_id
                left join payment.users usr on usr.id=pymts.claimed_sales_user_id
                left join payment.users usr1 on usr1.id = ord.placing_user_id
                left join payment.tablet_order_ids tab on tab.id = ord.id
                left join payment.order_details orddet on orddet.order_id=ord.id
                left join payment.subscriptions subs on subs.id = orddet.new_subscription_id
                left join payment.plans pln on pln.id = subs.plan_id
                left join payment.services svcs on svcs.id = pln.service_id
                left join payment.practices prct on prct.id=subs.practice_id
                left join payment.addresses addr on addr.id=prct.billing_address_id
                left join fabric_ad_zone_plans adpln on adpln.id=pln.id
                left join fabric_ad_zones adzn on adzn.id=adpln.fabric_ad_zone_id
                left join fabric_ad_keywords adkw on adkw.id=adpln.fabric_ad_keyword_id
                left join fabric_ad_cities adct on adct.id=adzn.city_id
                where ((pymts.processed=1 and pymts.successful=1) or (pymts.processed=0 and pymts.successful=0)) and svcs.id=8
                and subs.revoked=0
                and placed_at>='2016-02-01'
                order by 1;")

o22$EndDate <- as.Date(o22$OrderDate) + (o22$Duration_Days) 
# o22 <- o22[c("Practice_ID", "Ad-Code", "StartDate", "OrderDate", "EndDate")]
o3 <- o22
o3 <- o3[order(o3$`Ad-Code`, o3$Practice_ID, o3$Practice_ID, o3$StartDate, o3$OrderDate), ]
o3$`Ad-Code`[is.na(o3$`Ad-Code`)] <- "-"
o3$concat <- paste0(o3$Practice_ID, o3$`Ad-Code`)
o4 <- as.data.frame(count(o3$concat))
colnames(o4) <- c("PID|Ad-Code", "count")
o3$`count-PID|Ad-Code` <- o4$count[match(o3$concat, o4$`PID|Ad-Code`)]
o3 <- o3[order(o3$`count-PID|Ad-Code`),]
o3<-subset(o3,o3$`count-PID|Ad-Code`!=1)
o4<-unique(o3$concat)

o3$Concat2 <- paste(o3$OrderDate, o3$concat)

o3$Role<-"-"
o3$Role2<- "-"
for(i in 1:length(o4))
{
  temp<-subset(o3,o3$concat==o4[i])
  temp$halwapuri<-"H"
      for(j in 1 :(length(temp$concat)-1))
        {
          temp[j+1,]$halwapuri<-ifelse(temp[j+1,]$OrderDate>=temp[j,]$StartDate&temp[j+1,]$OrderDate              <=temp[j,]$EndDate,"R","H")
        }
  o3$Role2 <- temp$halwapuri[match(o3$Concat2, temp$Concat2)]
  o3$Role[!is.na(o3$Role2)] <- o3$Role2[!is.na(o3$Role2)]
}

o3$Role2 <- NULL

o22$concat <- paste0(o22$Practice_ID, o22$`Ad-Code`)
o22$Concat2 <- paste(o22$OrderDate, o22$concat)

o22$Type <- "Rudraksh"
o22$Type <- o3$Role[match(o22$Concat2, o3$Concat2)]
o22$Type[is.na(o22$Type)] <- "H"

o22 <- subset(o22, o22$Type != "R")

o22 <-merge(x=o22, y=orders, by.x= 'Order_ID', by.y = 'order_id', all.x = F, all.y = F)

o22<-subset(o22,!is.na(SKeyword))

o22<-o22[c('Order_ID','Duration_Months','Plan','cost','claimed_TM_email','SKeyword','SZone','SCity','amount_in_inr')]


# write.csv(o22, "../BI Shared Folder/test.csv")
