setwd("../fireball/")
t0 <- Sys.time()
source("./R/functions.R")
#!/usr/bin/env Rscript

################ Setting up Libraries
library("RMySQL") #Loads required libraries
library("lubridate")
library("plyr")
print("Initial Libraries loaded.")

############### Connecting to the database through credentials
source("./R/db_creds.R") #All the db connection info
con.fabric <- mysqlConnect(fabricCreds2)
print("Connection to fabric database established.")

############### Reading the required data into dataframes by connecting to the database

# Read practice_doctors table with all the required fields
t1 <- Sys.time()
practices <- dbGetQuery(con.fabric, "select id as practice_id , ray_practice_id ,  name as ClinicName , latitude, longitude ,locality_id from practices")
print("Practices Table Read")
Sys.time()-t1

t1 <- Sys.time()
locality <- dbGetQuery(con.fabric, "select a.id as locality_id , a.name as locality , b.name as city , d.name as country from master_localities a left join master_cities b on a.city_id = b.id left join master_states c on c.id = b.state_id left join master_countries d on d.id = c.country_id")
print("Locality Tables Read")
Sys.time()-t1

t1 <- Sys.time()
practice.doctors <- dbGetQuery(con.fabric, "select id as PracticeDoctor_id, doctor_id, practice_id , status , consultation_fee , resident_doctor , profile_published , created_at from practice_doctors  where deleted_at is NULL")
print("Practice doctors table read.")

Sys.time()-t1

t1 <- Sys.time()
doctors <- dbGetQuery(con.fabric, "select a.id as doctor_id , a.user_id , a.name as DoctorName , a.new_slug as doc_slug , a.gender , practicing_start_year as Experience , dqs from doctors a  where a.deleted_at is NULL")
print("Doctors Table Read")
Sys.time()-t1

t1 <- Sys.time()
doctors_verification <- dbGetQuery(con.fabric, "select doctor_id , verification_status from doctor_verification_statuses ")

#doctors_verification$verification_status = "VERIFIED"

print("Doctor Verification Table Read")
Sys.time()-t1

practice_doctors_main <-  merge(x=practice.doctors , y=practices , by.x = "practice_id", by.y="practice_id" , all.x=T , all.y=F)
practice_doctors_main <-  merge(x=practice_doctors_main , y=locality , by.x = "locality_id", by.y="locality_id" , all.x=T , all.y=F)
practice_doctors_main <-  merge(x=practice_doctors_main , y=doctors , by.x = "doctor_id", by.y="doctor_id" , all.x=T , all.y=F)
practice_doctors_main <-  merge(x=practice_doctors_main , y=doctors_verification , by.x = "doctor_id", by.y="doctor_id" , all.x=T , all.y=F)

#practice_doctors_m <- practice_doctors_main
practice_doctors_m <-  subset(practice_doctors_main, verification_status == 'VERIFIED')
practice_doctors_m$verification_status <- NULL
practice_doctors_m$locality_id <- NULL
#practice_doctors_m$verification_status <-  NULL

last_login <-  dbGetQuery(con.fabric, "select id as user_id , last_login from user_profile where account_id is not NULL")
practice_doctors_m <-  merge(x=practice_doctors_m , y=last_login , by = "user_id" , all.x=T , all.y=F)

# Tabulate the award details by doctor
awards <- dbGetQuery(con.fabric, "select doctor_id, title as award from doctor_awards where deleted_at is NULL ")
awards$award <- gsub("[^A-Za-z0-9():!. ]", "", awards$award)
awards <- aggregate(award~doctor_id, paste,collapse="|", data=awards)

print("Awards table read. Going to read Experience table.")

# Tabulate the experience details in different organizations by doctor
experience_organization <- dbGetQuery(con.fabric, "select doctor_id from doctor_organizations where deleted_at is NULL")
#experience_organization$organizations_experience<- gsub("[^A-Za-z0-9():!. ]", "", experience_organization$organizations_experience)
experience_organization <- as.data.frame(count(experience_organization))
colnames(experience_organization) <- c("doctor_id", "organizations")
print("Experience_organization table read. Going to read Memberships table.")

# Tabulate the membership details by doctor
membership <- dbGetQuery(con.fabric, "select doctor_id , b.name as memberships from doctor_memberships a LEFT JOIN master_doctor_membership_councils b on b.id = membership_council_id")
membership$memberships <- gsub("[^A-Za-z0-9():!. ]", "", membership$memberships)
membership <- aggregate(memberships~doctor_id, paste,collapse="|", data=membership)
print("Memberships table read. Going to read Services table.")

# Tabulate services provided by each doctor
services <- dbGetQuery(con.fabric, "select doctor_id, b.name as services from doctor_services a LEFT JOIN master_doctor_services b on b.id = service_id")
services$services <- gsub("[^A-Za-z0-9():!. ]", "",services$services)
services <- aggregate(services~doctor_id, paste,collapse="|", data=services)
print("Services table read. Going to read Qualifications table.")

# Tabulate the qualifications of each doctor
qualifications <- dbGetQuery(con.fabric, "select doctor_id , b.name as qualifications from doctor_qualifications a LEFT JOIN master_doctor_qualifications b on b.id = qualification_id ")
qualifications$qualifications <-  gsub("[^A-Za-z0-9():!. ]", "",qualifications$qualifications)
qualifications <- aggregate(qualifications~doctor_id, paste,collapse="|", data=qualifications)

print("Qualifications table read. Going to read Clinic_photos table.")

# Tabulate the clinic photos and logos by practice
clinic_photos <- dbGetQuery(con.fabric, "select practice_id, logo as logo_presence from practice_photos where deleted_at is NULL")
clinic_photos2 <- count(clinic_photos$practice_id)
colnames(clinic_photos2) <- c( "practice_id", "number_of_photos")
clinic_photos3 <- aggregate(logo_presence~practice_id , clinic_photos , sum)
clinic_photos <- merge(x= clinic_photos2, y = clinic_photos3 , by = "practice_id", all.x = T , all.y = F)
print("Clinic_photos table read. Going to read Gender table.")

# Tabulate the registration details of each doctor
registration <- dbGetQuery(con.fabric, "select doctor_id , registration_number as registration from doctor_registration_councils where deleted_at is NULL ")
registration$registration = paste0("_",as.character(registration$registration))
print("Registration table read. Going to read Fees table.")

# Tabulate the photo details of each doctor
doctor_photos <- dbGetQuery(con.fabric, "select doctor_id from doctor_photos where deleted_at is NULL and photo_url is not NULL")
doctor_photos <- count(doctor_photos)
colnames(doctor_photos)<- c("doctor_id", "doctor_photos")
print("Doctor_photos table read. Going to read Doctor_specialization table.")

# Tabulate the photo details of each doctor
doctor_specialization <- dbGetQuery(con.fabric, "select doctor_id, speciality as speciality from doctor_specializations a LEFT JOIN master_doctor_subspecialities  b on b.id = a.subspecialization_id Left Join master_doctor_specialities c on c.id = b.speciality_id")
print("Doctor_specialization table read. Going to create master file")

practice_type <- dbGetQuery(con.fabric, "select practice_id, b.name as type from practice_types a LEFT JOIN master_practice_types  b on b.id = a.types_id ")
practice_type <- aggregate(type~practice_id, paste,collapse="|", data=practice_type)


#t1 <- Sys.time()
#diagnostics <- dbGetQuery(con.fabric, "select practice_id , p.name , mc.name , d.published  from diagnostic_practices d left join practices p on p.id = d.practice_id left join master_localities ml on ml.id = p.locality_id left join master_cities mc on mc.id = ml.city_id where d.deleted_at is NULL")
#Sys.time()-t1

#fn <- "../Output/Practo.com Files/Latest Files/diagnostic_practices.csv"
#if (file.exists(fn)) file.remove(fn)
#write.csv(diagnostics,fn, row.names = F)

# Merging all the attributes associated with the doctors and the practices into a single file
master.file <- merge(x = practice_doctors_m, y = awards, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = clinic_photos, by.x = "practice_id", by.y = "practice_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = doctor_photos, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = experience_organization, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = membership, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = qualifications, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = registration, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = services, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = doctor_specialization, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)
master.file <- merge(x = master.file, y = practice_type,  by.x = "practice_id", by.y = "practice_id", all.x = T, all.y = F)

recom <- dbGetQuery(con.fabric  , "select doctor_id , recommendation from doctor_recommendations")
doc_rev <- read.csv("../../Reviews & Recommendations/doc_reviews.csv")
clinic_rev <- read.csv("../../Reviews & Recommendations/clinic_reviews.csv")

ray_customers <- read.csv("../../Ray Customers/Ray_Customers.csv")[,c("ray_practice_id","Customer_Since","EndDate")]
colnames(ray_customers)<- c("ray_practice_id","Ray_Start", "Ray_End")

reach <- read.csv("../../Search Ads Payments Data/practice_customers.csv")
reach[,1]<- NULL
colnames(reach)<- c("ray_practice_id","Reach_Start", "Reach_End")


master.file <- merge(x = master.file, y = recom, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)

master.file <- merge(x = master.file, y = doc_rev, by.x = "doctor_id", by.y = "doctor_id", all.x = T, all.y = F)


master.file <- merge(x = master.file, y = clinic_rev, by.x = "practice_id", by.y = "practice_id", all.x = T, all.y = F)

master.file <- merge(x = master.file, y = ray_customers, by.x = "ray_practice_id", by.y = "ray_practice_id", all.x = T, all.y = F)

master.file <- merge(x = master.file, y = reach, by.x = "ray_practice_id", by.y = "ray_practice_id", all.x = T, all.y = F)


master.file <- master.file[!duplicated(master.file$PracticeDoctor_id),]
print("Master file created by merging all the required files. Going to save the output into a csv file.")




# Saving the output/dataframe into a csv file
fn <- paste0("../Output/Practo.com Files/All Files/doctor_profile_",Sys.Date(),"complete.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(master.file, file = paste0("../Output/Practo.com Files/All Files/doctor_profile_",Sys.Date(),"complete.csv"), row.names = F)

fn <- paste0("../Output/Practo.com Files/Latest Files/Practo Profiles.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(master.file, file = paste0("../Output/Practo.com Files/Latest Files/Practo Profiles.csv"), row.names = F)

file2 <- master.file[, c("PracticeDoctor_id","doctor_id","practice_id" , "ray_practice_id", "speciality" , "locality", "city" , "country" , "number_of_photos")]

fn <- paste0("../Output/Practo.com Files/Latest Files/Profile_Mappings.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(file2, file = paste0("../Output/Practo.com Files/Latest Files/Profile_Mappings.csv"), row.names = F)

singapore <-  subset(master.file, tolower(country) == "singapore")
philippines<- subset(master.file, tolower(country) == "philippines")
indonesia<- subset(master.file, tolower(country) == "indonesia")
malaysia<- subset(master.file, tolower(country) == "malaysia")
international <- subset(master.file, country != "India")

fn <- paste0("../../Philippines Profiles/doctor_profile_philippines",Sys.Date(),"complete.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(philippines, file = paste0("../../Philippines Profiles/doctor_profile_philippines",Sys.Date(),"complete.csv"), row.names = F)

fn <- paste0("../../Singapore Profiles/doctor_profile_singapore",Sys.Date(),"complete.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(singapore, file = paste0("../../Singapore Profiles/doctor_profile_singapore",Sys.Date(),"complete.csv"), row.names = F)

fn <- paste0("../../Indonesia Profiles/doctor_profile_indonesia",Sys.Date(),"complete.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(indonesia, file = paste0("../../Indonesia Profiles/doctor_profile_indonesia",Sys.Date(),"complete.csv"), row.names = F)

fn <- paste0("../../Malaysia/doctor_profile_malaysia",Sys.Date(),"complete.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(malaysia, file = fn, row.names = F)

fn <- paste0("../../International/Linked Files/doctor_profile_international.csv")
if (file.exists(fn)) file.remove(fn)
write.csv(international, file = paste0("../../International/Linked Files/doctor_profile_international.csv"), row.names = F)

#profiles <- master.file
#master.file <- read.csv("../Output/Practo.com Files/Latest Files//Practo Profiles.csv")
profiles_s <- master.file[,c("ray_practice_id", "practice_id","status")]
print("Updating Ray Practice id Map")

ray_id_map_1 <- dbGetQuery(con.fabric, "select   b.ray_practice_id, b.new_slug , ml.name as Locality , mc.name as City from practices b left join master_localities ml on ml.id = b.locality_id left join master_cities mc on mc.id = ml.city_id  where ray_practice_id is not NULL   ;")
ray_id_map_2 <- merge(profiles_s,ray_id_map_1,by="ray_practice_id",all.x=F,all.y=T)
ray_id_map_2 <- ray_id_map_2[!duplicated(ray_id_map_2),]
#colnames(ray_id_map_2)<- c("ray_practice_id","practice_id","Locality","City","new_slug", "status")
ray_id_map_22 <- ray_id_map_2[,c("ray_practice_id","practice_id","new_slug","Locality","City")]

ray_id_map_23 <- subset(ray_id_map_2 , status == "ABS" , ray_practice_id)
ray_id_map_24 <- count(ray_id_map_23)  # abs enabled practices

ray_id_map_25 <- merge(x=ray_id_map_22 , y=ray_id_map_24 , by ="ray_practice_id" , all.x= T , all.y = F)
colnames(ray_id_map_25) <- c("ray_practice_id","practice_id","new_slug","Locality","City","ABS Enabled")

ray_id_map_26 <- master.file[,c("ray_practice_id","speciality")]
ray_id_map_26 <- ray_id_map_26[!duplicated(ray_id_map_26),]
ray_id_map_26 <- aggregate(speciality~ray_practice_id,paste,collapse="|",data=ray_id_map_26)
ray_id_map_27 <- merge(x=ray_id_map_25 , y=ray_id_map_26 , by ="ray_practice_id" , all.x= T , all.y = F)
ray_id_map_27 <- ray_id_map_27[!duplicated(ray_id_map_27$ray_practice_id),]

fn <- "../Output/ray_practice_id_map.csv"
if (file.exists(fn)) file.remove(fn)
write.csv(ray_id_map_27, file="../Output/ray_practice_id_map.csv")

print("All done.")
dbDisconnect(con.fabric)
Sys.time()- t0
print("Sucessfully Executed Practo Profiles.R")

