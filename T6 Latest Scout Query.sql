use darzupractodeathstar;

select c.id as City_ID,
c.name as City,
sum(case when (es.visit_status is null or es.visit_status in ('PC','NI','WIP','NF')) 
and es.segment_id = 1
and e.deleted_at is null 
then 1 else 0 end)
as Total_Clinics,
sum(case when e.visit_status in ('PC')
or es.visit_status = 'PC'
and e.deleted_at is null
and es.segment_id = 1
then 1 else 0 end) as Platinum
from scout_establishments e
left join scout_establishment_segments es
on e.id = es.establishment_id
left join scout_modules m
on e.module_id = m.id
left join space_master_cities c 
on c.id = m.city_id
where es.segment_id = 1
and (e.visit_status in ('PC','NI','WIP','NF')
or e.visit_status is null
or es.visit_status in ('PC','NI','WIP','NF'))
and e.deleted_at is null
and c.id in (1,2,129,716,172,717,171,11,480,497,628,766,484)
group by c.name;

select c.id as City_ID,
c.name as City,
e.id,
e.name,
e.space_id,
e.latitude,
e.longitude,
e.software,
e.status,
e.visit_count,
e.consultant_count,
e.meeting_count,
e.has_computer,
e.has_internet,
e.has_receptionist,
e.has_software,
e.visit_status,
e.patient_count,
e.has_organization
from scout_establishments e
left join scout_establishment_segments es
on e.id = es.establishment_id
left join scout_modules m
on e.module_id = m.id
left join space_master_cities c 
on c.id = m.city_id
where es.segment_id = 1
and e.deleted_at is null
and (e.visit_status in ('PC','NI','WIP','NF')
or e.visit_status is null
or es.visit_status in ('PC','NI','WIP','NF'));