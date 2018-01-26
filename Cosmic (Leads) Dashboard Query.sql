use darzupractodeathstar;

select distinct
	   cltm.territory_id as 'Territory ID', 
       smc.id as 'City ID',
       cso.region_id as 'Region ID',
       cs.parent_id as 'Parent ID',
       cso.user_id as 'User / Manager ID',
       smc.name as 'City Name',
	   sml.name as 'Locality Name',
       cs.name as 'Territory Name',
       epi.name as 'PSW Name',
       epi.Email as 'PSW Email' 
			from cosmic_practice_lead_map cplm
			left join cosmic_practices cp 
					on cplm.practice_id=cp.id
			left join space_master_localities sml 
					on cp.sales_localities_id = sml.id 
            left join cosmic_locality_territory_map cltm 
					on cltm.locality_id = sml.id
			left join space_master_cities smc
					on smc.id = sml.city_id
			left join cosmic_salesregions cs 
					on cs.id = cltm.territory_id
			left join space_master_countries smco
					on smco.id = smc.country_id
			left join cosmic_salesregion_owner cso
					on cso.region_id = cs.id
			left join cosmic_interactions intr 
					on cp.id=intr.practice_id
			left join epicenterusers epi 
					on intr.epicenteruser_id=epi.PK_idEpicenterUser
			where smco.id = 1 and cs.deleted_at is NULL limit 50;
            
            desc cosmic_interactions;
            
use darzupractodeathstar;

select distinct
       smc.id as 'City ID',
       smco.name as 'Country Name',
       smc.name as 'City Name',
	   sml.name as 'Locality Name',
       cs.name as 'Territory Name',
       epi.name as 'PSW Name',
       epi.Email as 'PSW Email' 
			from cosmic_practices cp 
			left join space_master_localities sml 
					on cp.sales_localities_id = sml.id 
            left join cosmic_locality_territory_map cltm 
					on cltm.locality_id = sml.id
			left join space_master_cities smc
					on smc.id = sml.city_id
			left join cosmic_salesregions cs 
					on cs.id = cltm.territory_id
			left join space_master_countries smco
					on smco.id = smc.country_id
			left join cosmic_interactions intr 
					on cp.id=intr.practice_id
			left join epicenterusers epi 
					on intr.epicenteruser_id=epi.PK_idEpicenterUser
			where smco.id = 1 and cs.deleted_at is NULL;
            
