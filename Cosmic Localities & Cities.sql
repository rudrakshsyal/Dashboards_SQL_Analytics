use darzupractodeathstar;

select * from space_master_cities smc where smc.name rlike 'Rishi';
select smc.id, smc.name
from space_master_cities smc 
where smc.name = 'Mathura'
or smc.name = 'Aligarh'
or smc.name = 'Kota'
or smc.name = 'Jodhpur'
or smc.name = 'Jhansi'
or smc.name = 'Dehradun'
or smc.name = 'Bareilly'
or smc.name = 'Gorakhpur'
or smc.name = 'Haridwar'
group by smc.name;

select * from space_master_cities smc where smc.name = 'Guwahati';
select * from space_master_cities smc where smc.name = 'Ghaziabad';
select * from space_master_cities smc where smc.name rlike 'Ank';

select sml.name from space_master_localities sml where sml.city_id = 398;

select sml.city_id as 'City ID', 
smc.name as 'City Name', 
sml.name as 'Locality Name'
from space_master_localities sml 
left join space_master_cities smc
on sml.city_id = smc.id
where sml.city_id = 667
or sml.city_id = 676
or sml.city_id = 736
or sml.city_id = 737
or sml.city_id = 696
or sml.city_id = 700
or sml.city_id = 610
or sml.city_id = 616
or sml.city_id = 666;


select sml.name as Locality,
c.id as City_ID,
c.name as City,
(case when (es.visit_status is null or es.visit_status in ('PC','NI','WIP','NF')) 
and es.segment_id = 1
and c.id=398
and e.deleted_at is null 
then 1 else 0 end)
as Total_Clinics
from scout_establishments e
left join scout_establishment_segments es
on e.id = es.establishment_id
left join scout_modules m
on e.module_id = m.id
left join space_master_cities c 
on c.id = m.city_id
left join space_master_localities sml
on sml.city_id=c.id
where es.segment_id = 1
and (e.visit_status in ('PC','NI','WIP','NF')
or e.visit_status is null
or es.visit_status in ('PC','NI','WIP','NF'))
and e.deleted_at is null
and c.country_id = 1
and c.id=398;

select * from space_master_states sms where sms.id = 21;

select distinct c.id as City_ID,
c.name as City,
sml.name, 
count(distinct case when (es.visit_status is null or es.visit_status in ('PC','NI','WIP','NF')) 
and es.segment_id = 1
and e.deleted_at is null 
and 
then e.id else 0 end)
as Total_Clinics
from scout_establishments e
left join scout_establishment_segments es
on e.id = es.establishment_id
left join scout_modules m
on e.module_id = m.id
left join space_master_cities c
on c.id = m.city_id
left join space_master_localities sml
on sml.city_id=c.id
where es.segment_id = 1
and c.id=398
group by 3;

desc scout_establishmen;

select mc.name as City,
sml.name as Locality,
count(cp.id) as Clinics
from cosmic_practices cp 
join space_master_localities sml 
on cp.sales_localities_id = sml.id 
join space_master_cities mc 
on mc.id = sml.city_id
where mc.id=398 
group by sml.name, mc.name 
order by sml.name;

select count(cp.id) practice_count, 
csl.`city_id`, 
csl.name 
from space_master_localities csl 
left join cosmic_practices cp 
on csl.id = cp.`sales_localities_id`
group by csl.`name` 
order by practice_count;

select sml.name,
smc.name,
sms.name 
from space_master_localities sml 
left join space_master_cities smc 
on smc.id=sml.city_id 
left join space_master_states sms 
on sms.id=smc.state_id 
where sml.name rlike 'luns'  and sms.name rlike 'Gujarat' order by 1;

select * from space_master_localities limit 100;

select * from space_master_cities limit 100;

select * from cosmic_salesregions where deleted_at is NULL and id = 7860;

select * from cosmic_salesregion_owner where region_id = 7910 limit 100 ;

select * from cosmic_locality_territory_map where locality_id = 144;

select * from space_master_countries limit 100;

select * from cosmic_user_profile limit 100;

select * from cosmic_salesregion_type limit 100;

select * from cosmic_practice_lead_map limit 100;

select * from cosmic_saleslocalities limit 5000;

select * from space_master_localities where name = 'Koramangala';

select id,count(*) from cosmic_leads where source='manual' group by 1 order by 2 desc limit 10;

desc cosmic_leads;

select * from scout_modules where city_id = 2 limit 100;


select c.id as City_ID,
sml.name as Locality,
c.name as City,
sum(case when (es.visit_status is null or es.visit_status in ('PC','NI','WIP','NF')) 
and es.segment_id = 1
and e.deleted_at is null 
then 1 else 0 end)
as Total_Clinics
from scout_establishments e
left join scout_establishment_segments es
on e.id = es.establishment_id
left join scout_modules m
on e.module_id = m.id
left join space_master_cities c 
on c.id = m.city_id
left join space_master_localities sml
on sml.city_id = c.id
where es.segment_id = 1
and (e.visit_status in ('PC','NI','WIP','NF')
or e.visit_status is null
or es.visit_status in ('PC','NI','WIP','NF'))
and e.deleted_at is null
and c.country_id = 1
and c.id=398
group by c.name;


