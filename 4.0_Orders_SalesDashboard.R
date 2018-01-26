setwd("../fireball")
library(plyr)
#!/usr/bin/env Rscript

################ Setting up Libraries
library(RMySQL) #Loads required libraries
source("./R/functions.R") #Loads required libraries
print("Initial Libraries loaded.")

############### Connecting to the database through credentials
source("./R/db_creds.R") #All the db connection info
con.payment <- mysqlConnect(paymentCreds2)
print("Connection to database established. Going to read practice_doctors table.")

################################## Defining the start and end dates for the analysis ##################################

st_date0 <- paste0(substr(Sys.Date()-87,1,8),"01")
st_date <- paste0(substr(Sys.Date()-2,1,8),"01") # start date for all the downloads from GA
monthvalue <- substr(st_date,6,7)
yearvalue <- substr(st_date,1,4)
monthvalue2 <- substr(as.Date(st_date)+31,6,7)
yearvalue2 <- substr(as.Date(st_date)+31,1,4)
datetemp <- paste0(yearvalue2,"-",monthvalue2,"-01")
en_date <- as.character(as.Date(datetemp) -1 )
filename <-paste0(format(as.Date(st_date), format="%b"),"_",yearvalue)
filename2 <- gsub("-","", st_date)


month <- as.numeric(substr(st_date , 6,7))
year <- as.numeric(substr(st_date , 1,4))


if (any(month == 1:9)){
  mmm <- paste0("",year,"_0",month)
}  else {
  mmm <- paste0("",year,"_",month)
}

########################################### Orders Load and City Query  #########################################

orders<-read.csv("../../Orders/Order(N).csv")[,-1]
orders$claimed_TM_email<-tolower(as.character(orders$claimed_TM_email))


## Sandeep Data
ordersN<-subset(orders,as.Date(orders$placed_at)>=as.Date('2014-01-01'))

cust<-read.csv("../../Ray Customers/Ray_Customers_Doctors.csv")

cust<-cust[c('PK_idCalendarSettings','DoctorName','DoctorEmail')]

ordersN<-merge(x = ordersN, y = cust, by.x = 'practice_id', by.y ='PK_idCalendarSettings',all.x = T,all.y = F)

################################## Orders Cleanup ##################################

l<-'ship'

orders<-subset(orders,!grepl(l,tolower(orders$Plan)))

orders$temp<-orders$processed+orders$successful
orders<-subset(orders,temp!=1)
orders$temp<-NULL


#orders<-orders[,-as.numeric(length(orders))]


orders<-subset(orders,orders$currency=="INR")
orders<-subset(orders,orders$Master_Plan_2!="Addon")

orders$upgrade_adjustment_old<-NULL
orders$upgrade_adjustment_new<-NULL
orders$days_renewal<-NULL
# orders$hunting_farming_flag<-NULL
orders$Type<-NULL

orders$amount_in_inr<-orders$Revenue

orders$Revenue<-NULL
orders$Master_Plan_2<-NULL
orders$Country<-NULL
orders$upselling<-NULL
orders$processed_at<-NULL
orders$city<-NULL
orders$mode<-NULL

########################## Defining Timeline of Reporting ##########################


orders<-subset(orders,as.Date(orders$placed_at)<=as.Date(en_date))
orders0<-subset(orders,as.Date(orders$placed_at)>=as.Date(st_date0))
orders<-subset(orders,as.Date(orders$placed_at)>=as.Date(st_date))

####################### Corporate Sales Removal

o2<- dbGetQuery(con.payment , "select distinct ord.id as 'Order_ID',
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
                and placed_at>='2015-06-01'
                order by 1;")

print("City Query")

o22<-o2

o2<-merge(x=o2, y=orders, by.x= 'Order_ID', by.y = 'order_id', all.x = F, all.y = F)

o2<-subset(o2,!is.na(SKeyword))

o2<-o2[c('Order_ID','Duration_Months','Plan','cost','claimed_TM_email','SKeyword','SZone','SCity','amount_in_inr')]

orders$city<-as.character(o2$SCity[match(orders$order_id,o2$'Order_ID')])

p<-'fabric'

orders<-subset(orders,!grepl(p,tolower(orders$Plan)) | !is.na(orders$city) )

######################################### Reach Renewals Removal #############################

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


####################################### Reach Big Sale Day(14th and 15th Dec) #######################################
#
# orders2<-subset(orders,grepl(p,tolower(orders$Plan)))
# orders2<-subset(orders2,as.Date(orders2$placed_at)<=as.Date('2015-12-16'))
# orders2<-subset(orders2,as.Date(orders2$placed_at)>as.Date('2015-12-14'))
#
# orders2<-orders2[,c("order_id","placed_at","practice_id","plan_id","Plan","cost", "amt_discount","claimed_TM_email","amount_in_inr","city")]
# orders3<-as.data.frame(count(orders2$city))
# colnames(orders3)<-c('city','slots')
#
# j<-aggregate(cost~city,orders2,mean)
# k<-aggregate(amount_in_inr~city,orders2,mean)
#
# orders3$AvCost<-6*j$cost[match(orders3$city,j$city)]
# orders3$AvRev<-k$amount_in_inr[match(orders3$city,k$city)]
# orders3$AvDisc<-(orders3$AvCost-orders3$AvRev)/orders3$AvCost
#
# order3<-subset(orders3,city!='Tawang')

############################################ Tab Big Sale Day(29th and 30th Dec) #############################################
#
# z<-'tab'
# #orders<-read.csv("../../Sales Reporting Dashboard/Orders_2015_12.csv")
# ordersTab<-subset(orders,grepl(z,tolower(orders$Plan)))
# ordersTab<-subset(ordersTab,as.Date(ordersTab$placed_at)<=as.Date('2015-12-30'))
# ordersTab<-subset(ordersTab,as.Date(ordersTab$placed_at)>=as.Date('2015-12-29'))
#
# ordersTab<-ordersTab[,c("order_id","placed_at","practice_id","plan_id","Plan","cost", "amt_discount","claimed_TM_email","amount_in_inr","city")]
# #ordersTab$city[is.na(ordersTab$city)]<-0
# ordersTab2<-as.data.frame(count(ordersTab$Plan))
# colnames(ordersTab2)<-c('Plan','TabCount')
#
# ordersTab2<-subset(ordersTab2,ordersTab2$Plan!='Tablet-Shipping')
#
# j<-aggregate(amount_in_inr~Plan,ordersTab,sum)
# #k<-aggregate(amount_in_inr~city,ordersTab2,frequency)
#
#
# ordersTab2$Total_Revenue<-j$amount_in_inr[match(ordersTab2$Plan,j$Plan)]
# #ordersTab2$Total_Count<-k$amount_in_inr[match(ordersTab2$city,j$city)]
# #orders3$AvDisc<-(orders3$AvCost-orders3$AvRev)/orders3$AvCost



############################################# Sales Performance Sheet Data #############################################

Teams_C <- read.csv("../../BI Shared Folder/Teams_C.csv")

Zones <- as.data.frame(unique(Teams_C$Zone))
Zones <- subset(Zones,!(Zones == '-') )
colnames(Zones) <- c("Unique Zones")

TeamMerge <- read.csv("../../Sales Reporting Dashboard/Teams/TC-Head.csv") # Can be done via subset

for(i in 1:12)
{
  p <- Zones[i, ]
  fn <- read.csv(paste0("../../Sales Reporting Dashboard/Teams/TC-",p,".csv"))
  TeamMerge <- as.data.frame(rbind(TeamMerge, fn))
}

TeamMerge <- subset(TeamMerge, TeamMerge$PSW.Email != 'zz')

TeamMerge$Target[is.na(TeamMerge$Target)] <- 0

TeamMerge$TargetsH<-TeamMerge$TargetsU<-0

TeamMerge$TargetsH[TeamMerge$Role=="H"]<- TeamMerge$Target[TeamMerge$Role=="H"]
TeamMerge$TargetsU[TeamMerge$Role=="U"]<- TeamMerge$Target[TeamMerge$Role=="U"]


### Why is this step duplicated as above
TeamMerge$Helper1 <- 0

a <- data.frame(c("Country", "Region", "Zone", "AZM", "AM", "AAM", "TM"), c("A", "B", "C", "D", "E", "F", "G"))
colnames(a) <- c("Level", "Helper1")

TeamMerge$Helper1 <- a$Helper1[match(TeamMerge$Level, a$Level)]
TeamMerge <- as.data.frame(TeamMerge[order(TeamMerge$Helper1, TeamMerge$Region, TeamMerge$Zone, TeamMerge$AM.Email, TeamMerge$Role, TeamMerge$Exp., TeamMerge$Target),])
TeamMerge$Helper1 <- NULL
####

teams <- TeamMerge[c(1:15)]

# teams<-read.csv("../../BI Shared Folder/Teams_C.csv")
teams$row<-c(1:length(teams$PSW.Email))
teams$PSW.Email<-tolower(teams$PSW.Email)
teams$RM<-tolower(teams$RM)
teams$ZM<-tolower(teams$ZM)
teams$AZM<-tolower(teams$AZM)
teams$AM.Email<-tolower(teams$AM.Email)
teams$AAM<-tolower(teams$AAM)
teams$Exp.<-tolower(teams$Exp.)

#teams<-teams[order(teams$row),]

temp <- TeamMerge[c(1:16)]

fn <- paste0("../../BI Shared Folder/Teams_C.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(temp , fn , row.names = T)

################################################### TM Block #############################################

TM<-subset(teams,Level=='TM')

orders<-subset(orders,grepl('ray',tolower(orders$Plan))| grepl('fabric',tolower(orders$Plan)))
orders2<-orders
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','Orders(H)')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`Orders(H)`[is.na(TM$`Orders(H)`)]<-0
TM$`Orders(H)`[TM$Role!='H']<-0

##

temp<-aggregate(amount_in_inr~claimed_TM_email,data=orders,FUN = sum)

colnames(temp)<-c('PSW.Email','Revenue')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$Revenue[is.na(TM$Revenue)]<-0


########################################## Hunting portion

TM$UnitsH<-TM$Revenue
TM$UnitsH[TM$Role!='H']<-0
TM$UnitsH<-(TM$UnitsH/12000)


##

temp <- subset(TeamMerge,TeamMerge$Level == "TM")
temp <- temp[c("PSW.Email","TargetsH")]
#TM$TargetsH <- as.numeric(TeamMerge$Target[match(TeamMerge$PSW.Email,TM$PSW.Email)])
TM <- merge(x=TM,y=temp,by="PSW.Email",all.x=TRUE)

TM$TargetsH <- as.numeric(TM$TargetsH)

##
TM$TargAchH<-TM$UnitsH/TM$TargetsH
TM$TargAchH[is.na(TM$TargAchH)]<-0
TM$TargAchH[is.infinite(TM$TargAchH)]<-0

##

TM$ARPU<-TM$UnitsH/TM$`Orders(H)`
TM$ARPU[is.na(TM$ARPU)]<-0
TM$ARPU[is.infinite(TM$ARPU)]<-0
TM$ARPU<-TM$ARPU*1000

##

TM$AUPWExpH<-as.numeric(TM$UnitsH)
TM$AUPWExpH[TM$Exp.=='new']<-0

##

TM$AUPWNewH<-as.numeric(TM$UnitsH)
TM$AUPWNewH[TM$Exp.=='exp']<-0


################################################## Upselling portion

TM$UnitsU<-TM$Revenue
TM$UnitsU[TM$Role!='U']<-0
TM$UnitsU<-(TM$UnitsU/12000)

##

TM$RevenueU<-TM$UnitsU*12000

##
temp <- subset(TeamMerge,TeamMerge$Level == "TM")
temp <- temp[c("PSW.Email","TargetsU")]
#TM$TargetsH <- as.numeric(TeamMerge$Target[match(TeamMerge$PSW.Email,TM$PSW.Email)])
TM <- merge(x=TM,y=temp,by="PSW.Email",all.x=TRUE)
TM$TargetsU <- as.numeric(TM$TargetsU)

##

TM$TargAchU<-TM$UnitsU/TM$TargetsU
TM$TargAchU[is.na(TM$TargAchU)]<-0
TM$TargAchU[is.infinite(TM$TargAchU)]<-0

##

TM$AUPWExpU<-as.numeric(TM$UnitsU)
TM$AUPWExpU[TM$Exp.=='new']<-0

TM$AUPWNewU<-as.numeric(TM$UnitsU)
TM$AUPWNewU[TM$Exp.=='exp']<-0

##

###################################### Hunting+Upselling portion

TM$UnitsN<-TM$UnitsH+TM$UnitsU

##

TM$RevenueN<-TM$Revenue

##

TM$TargetsN<-as.numeric(TM$TargetsH)+as.numeric(TM$TargetsU)

##

TM$TargAchN<-TM$RevenueN/TM$TargetsN
TM$TargAchN[is.na(TM$TargAchN)]<-0
TM$TargAchN[is.infinite(TM$TargAchN)]<-0

## Total Accounts

orders2<-subset(orders,grepl('ray',tolower(orders$Plan))| grepl('fabric',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','Orders(N)')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`Orders(N)`[is.na(TM$`Orders(N)`)]<-0

## Starter/Basic Accounts

orders2<-subset(orders,grepl('ray',tolower(orders$Plan))& !grepl('pro',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','StarterN')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`StarterN`[is.na(TM$`StarterN`)]<-0

## Pro Accounts

orders2<-subset(orders,grepl('pro',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','ProN')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`ProN`[is.na(TM$`ProN`)]<-0

## Ray-Tab Accounts

orders2<-subset(orders,grepl('ray',tolower(orders$Plan))& grepl('tab',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','TabN')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`TabN`[is.na(TM$`TabN`)]<-0

## EMR Accounts

orders2<-subset(orders,grepl('ray',tolower(orders$Plan))& grepl('d',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','EMRN')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`EMRN`[is.na(TM$`EMRN`)]<-0

## Fabric Accounts

orders2<-subset(orders,grepl('fabric',tolower(orders$Plan)))
temp<-count(orders2$claimed_TM_email)
colnames(temp)<-c('PSW.Email','ReachN')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$`ReachN`[is.na(TM$`ReachN`)]<-0

## Reach Revenue

orders2<-subset(orders,grepl('fabric',tolower(orders$Plan)))

#temp<-aggregate(cost~claimed_TM_email,data=orders2,FUN = sum)
temp<-aggregate(amount_in_inr~claimed_TM_email,data=orders2,FUN = sum)


colnames(temp)<-c('PSW.Email','ReachRevenue')

TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)

TM$ReachRevenue[is.na(TM$ReachRevenue)]<-0

TM$AvgSlotPrice<-TM$ReachRevenue/TM$ReachN
TM$AvgSlotPrice[is.na(TM$AvgSlotPrice)]<-0
TM$AvgSlotPrice[is.infinite(TM$AvgSlotPrice)]<-0

###################################### Teesra Dus
#
# oTD<-subset(orders,as.Date(orders$placed_at)>=as.Date('2015-12-23'))
# temp<-aggregate(amount_in_inr~claimed_TM_email,data=oTD,FUN = sum)
#
# colnames(temp)<-c('PSW.Email','TDRevN')
#
# TM<- merge(x=TM,y=temp,by="PSW.Email",all.x=T,all.y=F)
#
# TM$TDRevN[is.na(TM$TDRevN)]<-0

TM[is.na(TM)]<-0
print("TM Block Done")
################################################ Leadership Block   #############################################

AM<-subset(teams,Level=='AM')
AAM<-subset(teams,Level=='AAM')
AZM<-subset(teams,Level=='AZM')
ZM<-subset(teams,Level=='Zone')
RM<-subset(teams,Level=='Region')
CM<-subset(teams,Level=='Country')

lev<-list()
lev[[1]]<-AM
lev[[2]]<-AAM
lev[[3]]<-AZM
lev[[4]]<-ZM
lev[[5]]<-RM
lev[[6]]<-CM

param<-c("AM.Email","AAM","AZM","ZM","RM","CM")

#### Hunting Aggregator
# temp<-aggregate(cbind(Orders(H),Revenue,UnitsH,TargetsH,TargAchH,UnitsU,RevenueU,TargetsU,UnitsN,RevenueN,TargetsN,Orders(N),StarterN,ProN,TabN,EMRN,ReachN,ReachRevenue, AvgSlotPrice)~AM.Email,data = TM,FUN=sum)

# attr<-c("Orders(H)","Revenue","UnitsH","TargetsH","TargAchH","ARPU","AUPWExpH","AUPWNewH","UnitsU","RevenueU","TargetsU","TargAchU","AUPWExpU","AUPWNewU","UnitsN","RevenueN","TargetsN","TargAchN","Orders(N)","StarterN","ProN","TabN","EMRN","ReachN","ReachRevenue","AvgSlotPrice")

attrs<-c("Orders(H)","Revenue","UnitsH","TargetsH","UnitsU","RevenueU","TargetsU","UnitsN","RevenueN","TargetsN","Orders(N)","StarterN","ProN","TabN","EMRN","ReachN","ReachRevenue")


attrm<- c("AUPWExpH","AUPWNewH","AUPWExpU","AUPWNewU")
attrms<-c("exp","new","exp","new")
attrmr<-c("H","H","U","U")


attrd<-c("TargAchH","ARPU","TargAchU","TargAchN","AvgSlotPrice")

for(j in 1:length(lev)){

  for(i in 1:length(attrs)){
    temp<-aggregate(x=list(TM[[attrs[i]]]),by=list(TM[[param[j]]]),FUN=sum)
    colnames(temp)<-c('PSW.Email',attrs[i])
    lev[[j]]<- merge(x=lev[[j]],y=temp,by="PSW.Email",all.x=T,all.y=F)

  }

  for(i in 1:length(attrm)){
    PM<-TM
    TM<-subset(TM,TM$Exp.==attrms[i])
    TM<-subset(TM,TM$Role==attrmr[i])

    temp<-aggregate(x=list(TM[[attrm[i]]]),by=list(TM[[param[j]]]),FUN=mean)
    colnames(temp)<-c('PSW.Email',attrm[i])
    lev[[j]]<- merge(x=lev[[j]],y=temp,by="PSW.Email",all.x=T,all.y=F)

    TM<-PM
  }

  ########## Ratios

  lev[[j]]$TargAchH<-lev[[j]]$UnitsH/lev[[j]]$TargetsH
  lev[[j]]$TargAchH[is.na(lev[[j]]$TargAchH)]<-0
  lev[[j]]$TargAchH[is.infinite(lev[[j]]$TargAchH)]<-0

  lev[[j]]$ARPU<-lev[[j]]$UnitsH/lev[[j]]$`Orders(H)`*1000
  lev[[j]]$ARPU[is.na(lev[[j]]$ARPU)]<-0
  lev[[j]]$ARPU[is.infinite(lev[[j]]$ARPU)]<-0

  lev[[j]]$TargAchU<-lev[[j]]$UnitsU/lev[[j]]$TargetsU
  lev[[j]]$TargAchU[is.na(lev[[j]]$TargAchU)]<-0
  lev[[j]]$TargAchU[is.infinite(lev[[j]]$TargAchU)]<-0

  lev[[j]]$TargAchN<-lev[[j]]$UnitsN/lev[[j]]$TargetsN
  lev[[j]]$TargAchN[is.na(lev[[j]]$TargAchN)]<-0
  lev[[j]]$TargAchN[is.infinite(lev[[j]]$TargAchN)]<-0

  lev[[j]]$AvgSlotPrice<-lev[[j]]$ReachRevenue/lev[[j]]$ReachN
  lev[[j]]$AvgSlotPrice[is.na(lev[[j]]$AvgSlotPrice)]<-0
  lev[[j]]$AvgSlotPrice[is.infinite(lev[[j]]$AvgSlotPrice)]<-0


  lev[[j]]<-lev[[j]][,names(TM)]

}

AM<-lev[[1]]
AAM<-lev[[2]]
AZM<-lev[[3]]
ZM<-lev[[4]]
RM<-lev[[5]]
CM<-lev[[6]]

print("Aggregation Done")
####################### Metrics Calculation #######################

metrics<-rbind(CM,RM,ZM,AZM,AM,AAM,TM)
metrics<-metrics[order(metrics$row),]
metrics$row<-NULL

metrics$TDRev<-as.numeric(0)
metrics$Conc<-paste0(metrics$PSW.Email,metrics$Level)
#write.csv(temp,"./temp.csv")

# temp<-aggregate(cost~claimed_TM_email,data=orders2,FUN = sum)

o5<-aggregate(amount_in_inr~currency,data=orders,FUN = sum)
metrics$c<-paste0(o5[1,2])

o6<-as.data.frame(orders$claimed_TM_email)


t2<-count(o6[is.na(o6)])
t3<-count(o6[!is.na(o6)])
t3$temp<-'a'

t3<-aggregate(freq~temp,t3,FUN = sum)
t2[1,2]
metrics$d<-t3[1,2]/(t2[1,2]+t3[1,2])
#orders<-orders[orders$city!="NA"]
#orders$city<-NULL

metrics[is.na(metrics)]<-0

###### Gap Creation#####

metrics$Gap1<-metrics$Gap2<-metrics$Gap3<-metrics$Gap4<-metrics$Gap5<-0

metrics<-metrics[,c("PSW.Email","PSW.Name", "Region", "Zone",	"City",	"CM",	"RM",	"ZM",	"AZM", "AM.Email", "AM.Name",	"AAM", "Exp.", "Level",	"Role", "Gap1", "Orders(H)",	"Revenue",
"Gap2", "UnitsH",	"TargetsH",	"TargAchH",	"ARPU",	"AUPWExpH",	"AUPWNewH", "Gap3", "UnitsU",	"RevenueU",	"TargetsU",	"TargAchU",	"AUPWExpU",	"AUPWNewU", "Gap4", "UnitsN",	"RevenueN",	"TargetsN",	"TargAchN",	"Orders(N)",	"StarterN",	"ProN",	"TabN",	"EMRN",	"ReachN",	"ReachRevenue",	"AvgSlotPrice",	"TDRev", "Conc", "c",	"d", "Gap5")]

print("Aggregator Block Done")


########################################## Write Block ##########################################

fn <- paste0("../../Sales Reporting Dashboard/metrics.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(metrics , fn , row.names = F)

fn <- paste0("../../Sales Reporting Dashboard/Orders_",mmm,".csv")
if (file.exists(fn)) file.remove(fn)
write.csv(orders , fn , row.names = F)

fn <- paste0("../../Sales Reporting Dashboard/Orders_Cos.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(orders0 , fn , row.names = F)
# fn <- paste0("../../Sales Reporting Dashboard/Orders_BSD.csv")
# if (file.exists(fn)) file.remove(fn)
# write.csv(orders3 , fn , row.names = F)

# fn <- paste0("../../Sales Reporting Dashboard/Orders_TabBSD.csv")
# if (file.exists(fn)) file.remove(fn)
# write.csv(ordersTab2 , fn , row.names = F)

fn <- paste0("../../Temp/Orders_",mmm,".csv")
if (file.exists(fn)) file.remove(fn)
write.csv(orders , fn , row.names = F)

fn <- paste0("../../Sales Reporting Dashboard/Reach Dashboard/inventory.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(o22 , fn , row.names = F)

fn <- paste0("../../Temp/Order(N).csv")
if (file.exists(fn)) file.remove(fn)
write.csv(ordersN, fn )

