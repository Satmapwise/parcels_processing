# Script to get any layer from any entity
# get_all_layers.py <county> <city> <layer>
# Any blank arguments default to all (if applicable)

import sys

# Define layers, counties, and entities
layers = {
    "flu",
    "zoning",
    "parcel_geometry",
    "streets",
    "address_points",
    "subdivisions",
    "building_footprints",
    "elevation",
    "soils",
    }

counties = {
    "miami-dade",
    "broward",
    "palm_beach",
    "hillsborough",
    "orange",
    "pinellas",
    "duval",
    "lee",
    "polk",
    "brevard",
    "volusia",
    "pasco",
    "seminole",
    "sarasota",
    "manatee",
    "collier",
    "osceola",
    "marion",
    "lake",
    "st_lucie",
    "escambia",
    "leon",
    "alachua",
    "st_johns",
    "clay",
    "okaloosa",
    "hernando",
    "bay",
    "charlotte",
    "santa_rosa",
    "martin",
    "indian_river",
    "citrus",
    "sumter",
    "flagler",
    "highlands",
    "nassau",
    "monroe",
    "putnam",
    "walton",
    "columbia",
    "gadsden",
    "suwannee",
    "jackson",
    "hendry",
    "okeechobee",
    "levy",
    "desoto",
    "wakulla",
    "baker",
    "bradford",
    "hardee",
    "washington",
    "taylor",
    "gilchrist",
    "gulf",
    "union",
    "hamilton",
    "jefferson",
    "lafayette",
    "liberty",
    "madison",
    "glades",
    "calhoun",
    "dixie",
    "franklin"
    }

entities = {
    "miami-dade_incorporated", "miami-dade_unincorporated", 
    "broward_unified", "broward_unincorporated", 
    "palm_beach_unified", 
    "hillsborough_plant_city", "hillsborough_tampa", "hillsborough_temple_terrace", "hillsborough_unincorporated", 
    "orange_apopka", "orange_bay_lake", "orange_belle_isle", "orange_eatonville", "orange_edgewood", "orange_lake_buena_vista", "orange_maitland", "orange_oakland", "orange_ocoee", "orange_orlando", "orange_unincorporated", "orange_windermere", "orange_winter_garden", "orange_winter_park", 
    "pinellas_belleair", "pinellas_belleair_beach", "pinellas_belleair_bluffs", "pinellas_belleair_shore", "pinellas_clearwater", "pinellas_dunedin", "pinellas_gulfport", "pinellas_indian_rocks_beach", "pinellas_indian_shores", "pinellas_kenneth_city", "pinellas_largo", "pinellas_madeira_beach", "pinellas_north_redington_beach", "pinellas_oldsmar", "pinellas_pinellas_park", "pinellas_redington_beach", "pinellas_redington_shores", "pinellas_safety_harbor", "pinellas_seminole", "pinellas_south_pasadena", "pinellas_st_pete_beach", "pinellas_st_petersburg", "pinellas_tarpon_springs", "pinellas_treasure_island", "pinellas_unincorporated", 
    "duval_unified", "lee_unincorporated", "lee_bonita_springs", "lee_cape_coral", "lee_fort_myers", "lee_fort_myers_beach", "lee_sanibel", 
    "polk_unincorporated", "polk_auburndale", "polk_bartow", "polk_davenport", "polk_dundee", "polk_eagle_lake", "polk_fort_meade", "polk_frostproof", "polk_haines_city", "polk_highland_park", "polk_hillcrest_heights", "polk_lake_alfred", "polk_lake_hamilton", "polk_lake_wales", "polk_lakeland", "polk_mulberry", "polk_polk_city", "polk_winter_haven", 
    "brevard_cape_canaveral", "brevard_cocoa", "brevard_cocoa_beach", "brevard_indian_harbour_beach", "brevard_indiatlantic", "brevard_malabar", "brevard_melbourne", "brevard_melbourne_beach", "brevard_melbourne_village", "brevard_palm_bay", "brevard_palm_shores", "brevard_rockledge", "brevard_satellite_beach", "brevard_titusville", "brevard_unincorporated", "brevard_west_melbourne", 
    "volusia_daytona_beach", "volusia_daytona_beach_shores", "volusia_de_bary", "volusia_de_land", "volusia_deltona", "volusia_edgewater", "volusia_flagler_beach", "volusia_holly_hill", "volusia_lake_helen", "volusia_new_smyrna_beach", "volusia_oak_hill", "volusia_orange_city", "volusia_ormond_beach", "volusia_pierson", "volusia_ponce_inlet", "volusia_port_orange", "volusia_south_daytona", "volusia_countywide", "volusia_unincorporated", 
    "pasco_dade_city", "pasco_new_port_richey", "pasco_port_richey", "pasco_san_antonio", "pasco_st_leo", "pasco_unincorporated", "pasco_zephyrhills", 
    "seminole_altamonte_springs", "seminole_casselberry", "seminole_lake_mary", "seminole_longwood", "seminole_oviedo", "seminole_sanford", "seminole_unincorporated", "seminole_winter_springs", 
    "sarasota_unincorporated", "sarasota_longboat_key", "sarasota_north_port", "sarasota_sarasota", "sarasota_venice", 
    "manatee_unincorporated", "manatee_anna_maria", "manatee_bradenton", "manatee_bradenton_beach", "manatee_holmes_beach", "manatee_longboat_key", "manatee_palmetto", 
    "collier_unincorporated", "collier_everglades", "collier_marco_island", "collier_naples", 
    "osceola_st_cloud", "osceola_unincorporated", "osecola_kissimmee", 
    "marion_belleview", "marion_dunnellon", "marion_mcintosh", "marion_ocala", "marion_reddick", "marion_unincorporated", 
    "lake_clermont", "lake_eustis", "lake_fruitland_park", "lake_groveland", "lake_lady_lake", "lake_leesburg", "lake_minneola", "lake_mount_dora", "lake_tavares", "lake_umatilla", "lake_astatula", "lake_howey-in-the-hills", "lake_mascotte", "lake_montverde", "lake_unincorporated", 
    "st_lucie_ft_pierce", "st_lucie_port_st_lucie", "st_lucie_unincorporated", 
    "escambia_century", "escambia_pensacola", "escambia_unincorporated", 
    "leon_unified", "alachua_alachua", "alachua_archer", "alachua_gainesville", "alachua_hawthorne", "alachua_high_springs", "alachua_lacrosse", "alachua_micanopy", "alachua_newberry", "alachua_waldo", "alachua_unincorporated", 
    "st_johns_hastings", "st_johns_marineland", "st_johns_st_augustine", "st_johns_st_augustine_beach", "st_johns_unincorporated", 
    "clay_green_cove_springs", "clay_keystone_heights", "clay_orange_park", "clay_penney_farms", "clay_unincorporated", 
    "okaloosa_cinco_bayou", "okaloosa_crestview", "okaloosa_destin", "okaloosa_fort_walton_beach", "okaloosa_laurel_hill", "okaloosa_mary_esther", "okaloosa_niceville", "okaloosa_shalimar", "okaloosa_unincorporated", "okaloosa_valparaiso", 
    "hernando_brooksville", "hernando_unincorporated", "hernando_weeki_wachee", 
    "bay_callaway", "bay_cedar_grove", "bay_lynn_haven", "bay_mexico_beach", "bay_panama_city", "bay_panama_city_beach", "bay_parker", "bay_springfield", "bay_unincorporated", 
    "charlotte_unincorporated", "charlotte_punta_gorda", 
    "santa_rosa_gulf_breeze", "santa_rosa_jay", "santa_rosa_milton", "santa_rosa_unincorporated", 
    "martin_jupiter_island", "martin_ocean_breeze_park", "martin_sewalls_point", "martin_stuart", "martin_unincorporated", 
    "indian_river_fellsmere", "indian_river_indian_river_shores", "indian_river_orchid", "indian_river_sebastian", "indian_river_unincorporated", "indian_river_vero_beach", 
    "citrus_crystal_river", "citrus_inverness", "citrus_unincorporated", 
    "sumter_bushnell", "sumter_center_hill", "sumter_coleman", "sumter_unincorporated", "sumter_webster", "sumter_wildwood", 
    "flagler_beverly_beach", "flagler_bunnell", "flagler_flagler_beach", "flagler_marineland", "flagler_palm_coast", "flagler_unincorporated", 
    "highlands_unincorporated", "highlands_avon_park", "highlands_lake_placid", "highlands_sebring", 
    "nassau_callahan", "nassau_fernandina_beach", "nassau_hilliard", "nassau_unincorporated", 
    "monroe_islamorada_village_of_islands", "monroe_key_colony_beach", "monroe_key_west", "monroe_layton", "monroe_marathon", "monroe_unincorporated", 
    "putnam_crescent_city", "putnam_interlachen", "putnam_palatka", "putnam_pomona_park", "putnam_unincorporated", "putnam_welaka", 
    "walton_de_funiak_springs", "walton_freeport", "walton_paxton", "walton_unincorporated", 
    "columbia_fort_white", "columbia_lake_city", "columbia_unincorporated", 
    "gadsden_chattahoochee", "gadsden_greensboro", "gadsden_gretna", "gadsden_havana", "gadsden_midway", "gadsden_quincy", "gadsden_unincorporated", 
    "suwannee_branford", "suwannee_live_oak", "suwannee_unincorporated", 
    "jackson_alford", "jackson_bascom", "jackson_campbellton", "jackson_cottondale", "jackson_graceville", "jackson_grand_ridge", "jackson_greenwood", "jackson_jacob_city", "jackson_malone", "jackson_marianna", "jackson_sneads", "jackson_unincorporated", 
    "hendry_unincorporated", "hendry_clewiston", "hendry_labelle", 
    "okeechobee_unincorporated", "okeechobee_okeechobee", 
    "levy_bronson", "levy_cedar_key", "levy_chiefland", "levy_fanning_springs", "levy_inglis", "levy_otter_creek", "levy_unincorporated", "levy_williston", "levy_yankeetown", 
    "desoto_unincorporated", "desoto_arcadia", 
    "wakulla_sopchoppy", "wakulla_st_marks", "wakulla_unincorporated", 
    "baker_glen_st_mary", "baker_macclenny", "baker_unincorporated", 
    "bradford_brooker", "bradford_hampton", "bradford_keystone_heights", "bradford_lawtey", "bradford_starke", "bradford_unincorporated", 
    "hardee_unincorporated", "hardee_wauchula", "hardee_zolfo_springs", "hardee_bowling_green", 
    "washington_caryville", "washington_chipley", "washington_ebro", "washington_unincorporated", "washington_vernon", "washington_wausau", 
    "taylor_perry", "taylor_unincorporated", 
    "gilchrist_bell", "gilchrist_fanning_springs", "gilchrist_trenton", "gilchrist_unincorporated",
    "gulf_port_st_joe", "gulf_unincorporated", "gulf_wewahitchka", 
    "union_lake_butler", "union_raiford", "union_unincorporated", "union_worthington_springs", 
    "hamilton_jasper", "hamilton_jennings", "hamilton_unincorporated", "hamilton_white_springs", 
    "jefferson_monticello", "jefferson_unincorporated", 
    "lafayette_mayo", "lafayette_unincorporated", 
    "liberty_bristol", "liberty_unincorporated", 
    "madison_greenville", "madison_lee", "madison_madison", "madison_unincorporated", 
    "glades_unincorporated", "glades_moore_haven", 
    "calhoun_altha", "calhoun_blountstown", "calhoun_unincorporated", 
    "dixie_cross_city", "dixie_horseshoe_beach", "dixie_unincorporated", 
    "franklin_apalachicola", "franklin_carrabelle", "franklin_unincorporated"
    }

"""
AI PROMPT:

I need to make a script that chains together a series of python scripts to fetch layer data, process it, and upload it to postgres databases. the download step 
varies by layer and entity, while the processing step varies by layer and the upload step is always the same. 

Main routine: 
i've added lists of layers, counties, and city-county entities in the state of Florida. some layers are split by county, while others are split by city. we'll start 
the skeleton of the script by focusing on zoning and flu, both of which are split by entity. first, we need a function to parse the command line input, set the queue, 
then proceed. then, we can wrap downloading and processing together by layer in a download_process_<layer> function. the download_process function will map the entity 
to the correct download function and run it, then run the processing script. then, the main function can proceed to the upload function, which will connect to 2 other 
servers, transfer 2 backup files to each, then run psql commands on each to upload the data (maybe do this in a batch for a list of what download_process returns?). 
after the whole process is done, we can generate a summary .csv that lists the layer, entity, data date, and whether it was successful or not.
the script should be able to run in a loop, with a queue of entities to process.

Additional considerations:
The script needs robust, modular error handling, making use of the logging module. I had an idea for a tiered error handling system, where errors in main are rank 0,
errors in download_process are rank 1, errors in upload are rank 2, and so on. I want to discuss with you to brainstorm how to approach error handling, but I want
to at least use a pointer system to make it modular and easy to edit.


"""