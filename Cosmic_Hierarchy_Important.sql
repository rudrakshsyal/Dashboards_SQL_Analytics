select cso.id, 
cso.region_id as 'Region ID', 
csr.id as 'Region ID', 
csr.name as 'Geo. Location',
cso.user_id as 'Owner ID', 
eu.PK_idEpicenterUser as 'Owner ID', 
eu.Email as 'Owner Email ID',
cso.role as 'Role',
csr.parent_id as 'Parent of Geo. Location',
csr.team_id as 'Team'
from cosmic_salesregion_owner cso 
left join epicenterusers eu
on cso.user_id = eu.PK_idEpicenterUser 
left join cosmic_salesregions csr
on csr.id=cso.region_id
where cso.role = 'SALES' and csr.team_id = 7;

select csr.id, 
csr.salesregion_type_id, 
csr.parent_id, 
csr.name, 
csr.team_id 
from cosmic_salesregions csr 
where csr.team_id = 7;

select cso.id, 
cso.region_id, 
cso.user_id, 
cso.role 
from cosmic_salesregion_owner cso
where cso.role = 'SALES';

select eu.PK_idEpicenterUser, 
eu.Email, 
eu.name, 
eu.City, 
eu.State, 
eu.Country
from epicenterusers eu;