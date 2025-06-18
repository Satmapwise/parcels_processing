# Script to get any layer from any entity
# get_all_layers.py <county> <city> <layer>
# Any blank arguments default to all

import sys

# Define entities by county
# Miami-Dade County
miami_dade_entities = ["miami-dade_incorporated", "miami-dade_unincorporated"]

# Broward County  
broward_entities = ["broward_unified", "broward_unincorporated"]

# Palm Beach County
palm_beach_entities = ["palm_beach_unified"]

# Hillsborough County
hillsborough_entities = ["hillsborough_plant_city", "hillsborough_tampa", "hillsborough_temple_terrace", "hillsborough_unincorporated"]

# Orange County
orange_entities = ["orange_apopka", "orange_bay_lake", "orange_belle_isle", "orange_eatonville", "orange_edgewood", "orange_lake_buena_vista", "orange_maitland", "orange_oakland", "orange_ocoee", "orange_orlando", "orange_unincorporated", "orange_windermere", "orange_winter_garden", "orange_winter_park"]

# Pinellas County
pinellas_entities = ["pinellas_belleair", "pinellas_belleair_beach", "pinellas_belleair_bluffs", "pinellas_belleair_shore", "pinellas_clearwater", "pinellas_dunedin", "pinellas_gulfport", "pinellas_indian_rocks_beach", "pinellas_indian_shores", "pinellas_kenneth_city", "pinellas_largo", "pinellas_madeira_beach", "pinellas_north_redington_beach", "pinellas_oldsmar", "pinellas_pinellas_park", "pinellas_redington_beach", "pinellas_redington_shores", "pinellas_safety_harbor", "pinellas_seminole", "pinellas_south_pasadena", "pinellas_st_pete_beach", "pinellas_st_petersburg", "pinellas_tarpon_springs", "pinellas_treasure_island", "pinellas_unincorporated"]

# Duval County
duval_entities = ["duval_unified"]

# Lee County
lee_entities = ["lee_unincorporated", "lee_bonita_springs", "lee_cape_coral", "lee_fort_myers", "lee_fort_myers_beach", "lee_sanibel"]

# Polk County
polk_entities = ["polk_unincorporated", "polk_auburndale", "polk_bartow", "polk_davenport", "polk_dundee", "polk_eagle_lake", "polk_fort_meade", "polk_frostproof", "polk_haines_city", "polk_highland_park", "polk_hillcrest_heights", "polk_lake_alfred", "polk_lake_hamilton", "polk_lake_wales", "polk_lakeland", "polk_mulberry", "polk_polk_city", "polk_winter_haven"]

# Brevard County
brevard_entities = ["brevard_cape_canaveral", "brevard_cocoa", "brevard_cocoa_beach", "brevard_indian_harbour_beach", "brevard_indiatlantic", "brevard_malabar", "brevard_melbourne", "brevard_melbourne_beach", "brevard_melbourne_village", "brevard_palm_bay", "brevard_palm_shores", "brevard_rockledge", "brevard_satellite_beach", "brevard_titusville", "brevard_unincorporated", "brevard_west_melbourne"]

# Volusia County
volusia_entities = ["volusia_daytona_beach", "volusia_daytona_beach_shores", "volusia_de_bary", "volusia_de_land", "volusia_deltona", "volusia_edgewater", "volusia_flagler_beach", "volusia_holly_hill", "volusia_lake_helen", "volusia_new_smyrna_beach", "volusia_oak_hill", "volusia_orange_city", "volusia_ormond_beach", "volusia_pierson", "volusia_ponce_inlet", "volusia_port_orange", "volusia_south_daytona", "volusia_countywide", "volusia_unincorporated"]

# Pasco County
pasco_entities = ["pasco_dade_city", "pasco_new_port_richey", "pasco_port_richey", "pasco_san_antonio", "pasco_st_leo", "pasco_unincorporated", "pasco_zephyrhills"]

# Seminole County
seminole_entities = ["seminole_altamonte_springs", "seminole_casselberry", "seminole_lake_mary", "seminole_longwood", "seminole_oviedo", "seminole_sanford", "seminole_unincorporated", "seminole_winter_springs"]

# Sarasota County
sarasota_entities = ["sarasota_unincorporated", "sarasota_longboat_key", "sarasota_north_port", "sarasota_sarasota", "sarasota_venice"]

# Manatee County
manatee_entities = ["manatee_unincorporated", "manatee_anna_maria", "manatee_bradenton", "manatee_bradenton_beach", "manatee_holmes_beach", "manatee_longboat_key", "manatee_palmetto"]

# Collier County
collier_entities = ["collier_unincorporated", "collier_everglades", "collier_marco_island", "collier_naples"]

# Osceola County
osceola_entities = ["osceola_st_cloud", "osceola_unincorporated", "osecola_kissimmee"]

# Marion County
marion_entities = ["marion_belleview", "marion_dunnellon", "marion_mcintosh", "marion_ocala", "marion_reddick", "marion_unincorporated"]

# Lake County
lake_entities = ["lake_clermont", "lake_eustis", "lake_fruitland_park", "lake_groveland", "lake_lady_lake", "lake_leesburg", "lake_minneola", "lake_mount_dora", "lake_tavares", "lake_umatilla", "lake_astatula", "lake_howey-in-the-hills", "lake_mascotte", "lake_montverde", "lake_unincorporated"]

# St. Lucie County
st_lucie_entities = ["st_lucie_ft_pierce", "st_lucie_port_st_lucie", "st_lucie_unincorporated"]

# Escambia County
escambia_entities = ["escambia_century", "escambia_pensacola", "escambia_unincorporated"]

# Leon County
leon_entities = ["leon_unified"]

# Alachua County
alachua_entities = ["alachua_alachua", "alachua_archer", "alachua_gainesville", "alachua_hawthorne", "alachua_high_springs", "alachua_lacrosse", "alachua_micanopy", "alachua_newberry", "alachua_waldo", "alachua_unincorporated"]

# St. Johns County
st_johns_entities = ["st_johns_hastings", "st_johns_marineland", "st_johns_st_augustine", "st_johns_st_augustine_beach", "st_johns_unincorporated"]

# Clay County
clay_entities = ["clay_green_cove_springs", "clay_keystone_heights", "clay_orange_park", "clay_penney_farms", "clay_unincorporated"]

# Okaloosa County
okaloosa_entities = ["okaloosa_cinco_bayou", "okaloosa_crestview", "okaloosa_destin", "okaloosa_fort_walton_beach", "okaloosa_laurel_hill", "okaloosa_mary_esther", "okaloosa_niceville", "okaloosa_shalimar", "okaloosa_unincorporated", "okaloosa_valparaiso"]

# Hernando County
hernando_entities = ["hernando_brooksville", "hernando_unincorporated", "hernando_weeki_wachee"]

# Bay County
bay_entities = ["bay_callaway", "bay_cedar_grove", "bay_lynn_haven", "bay_mexico_beach", "bay_panama_city", "bay_panama_city_beach", "bay_parker", "bay_springfield", "bay_unincorporated"]

# Charlotte County
charlotte_entities = ["charlotte_unincorporated", "charlotte_punta_gorda"]

# Santa Rosa County
santa_rosa_entities = ["santa_rosa_gulf_breeze", "santa_rosa_jay", "santa_rosa_milton", "santa_rosa_unincorporated"]

# Martin County
martin_entities = ["martin_jupiter_island", "martin_ocean_breeze_park", "martin_sewalls_point", "martin_stuart", "martin_unincorporated"]

# Indian River County
indian_river_entities = ["indian_river_fellsmere", "indian_river_indian_river_shores", "indian_river_orchid", "indian_river_sebastian", "indian_river_unincorporated", "indian_river_vero_beach"]

# Citrus County
citrus_entities = ["citrus_crystal_river", "citrus_inverness", "citrus_unincorporated"]

# Sumter County
sumter_entities = ["sumter_bushnell", "sumter_center_hill", "sumter_coleman", "sumter_unincorporated", "sumter_webster", "sumter_wildwood"]

# Flagler County
flagler_entities = ["flagler_beverly_beach", "flagler_bunnell", "flagler_flagler_beach", "flagler_marineland", "flagler_palm_coast", "flagler_unincorporated"]

# Highlands County
highlands_entities = ["highlands_unincorporated", "highlands_avon_park", "highlands_lake_placid", "highlands_sebring"]

# Nassau County
nassau_entities = ["nassau_callahan", "nassau_fernandina_beach", "nassau_hilliard", "nassau_unincorporated"]

# Monroe County
monroe_entities = ["monroe_islamorada_village_of_islands", "monroe_key_colony_beach", "monroe_key_west", "monroe_layton", "monroe_marathon", "monroe_unincorporated"]

# Putnam County
putnam_entities = ["putnam_crescent_city", "putnam_interlachen", "putnam_palatka", "putnam_pomona_park", "putnam_unincorporated", "putnam_welaka"]

# Walton County
walton_entities = ["walton_de_funiak_springs", "walton_freeport", "walton_paxton", "walton_unincorporated"]

# Columbia County
columbia_entities = ["columbia_fort_white", "columbia_lake_city", "columbia_unincorporated"]

# Gadsden County
gadsden_entities = ["gadsden_chattahoochee", "gadsden_greensboro", "gadsden_gretna", "gadsden_havana", "gadsden_midway", "gadsden_quincy", "gadsden_unincorporated"]

# Suwannee County
suwannee_entities = ["suwannee_branford", "suwannee_live_oak", "suwannee_unincorporated"]

# Jackson County
jackson_entities = ["jackson_alford", "jackson_bascom", "jackson_campbellton", "jackson_cottondale", "jackson_graceville", "jackson_grand_ridge", "jackson_greenwood", "jackson_jacob_city", "jackson_malone", "jackson_marianna", "jackson_sneads", "jackson_unincorporated"]

# Hendry County
hendry_entities = ["hendry_unincorporated", "hendry_clewiston", "hendry_labelle"]

# Okeechobee County
okeechobee_entities = ["okeechobee_unincorporated", "okeechobee_okeechobee"]

# Levy County
levy_entities = ["levy_bronson", "levy_cedar_key", "levy_chiefland", "levy_fanning_springs", "levy_inglis", "levy_otter_creek", "levy_unincorporated", "levy_williston", "levy_yankeetown"]

# DeSoto County
desoto_entities = ["desoto_unincorporated", "desoto_arcadia"]

# Wakulla County
wakulla_entities = ["wakulla_sopchoppy", "wakulla_st_marks", "wakulla_unincorporated"]

# Baker County
baker_entities = ["baker_glen_st_mary", "baker_macclenny", "baker_unincorporated"]

# Bradford County
bradford_entities = ["bradford_brooker", "bradford_hampton", "bradford_keystone_heights", "bradford_lawtey", "bradford_starke", "bradford_unincorporated"]

# Hardee County
hardee_entities = ["hardee_unincorporated", "hardee_wauchula", "hardee_zolfo_springs", "hardee_bowling_green"]

# Washington County
washington_entities = ["washington_caryville", "washington_chipley", "washington_ebro", "washington_unincorporated", "washington_vernon", "washington_wausau"]

# Taylor County
taylor_entities = ["taylor_perry", "taylor_unincorporated"]

# Gilchrist County
gilchrist_entities = ["gilchrist_bell", "gilchrist_fanning_springs", "gilchrist_trenton", "gilchrist_unincorporated"]

# Gulf County
gulf_entities = ["gulf_port_st_joe", "gulf_unincorporated", "gulf_wewahitchka"]

# Union County
union_entities = ["union_lake_butler", "union_raiford", "union_unincorporated", "union_worthington_springs"]

# Hamilton County
hamilton_entities = ["hamilton_jasper", "hamilton_jennings", "hamilton_unincorporated", "hamilton_white_springs"]

# Jefferson County
jefferson_entities = ["jefferson_monticello", "jefferson_unincorporated"]

# Lafayette County
lafayette_entities = ["lafayette_mayo", "lafayette_unincorporated"]

# Liberty County
liberty_entities = ["liberty_bristol", "liberty_unincorporated"]

# Madison County
madison_entities = ["madison_greenville", "madison_lee", "madison_madison", "madison_unincorporated"]

# Glades County
glades_entities = ["glades_unincorporated", "glades_moore_haven"]

# Calhoun County
calhoun_entities = ["calhoun_altha", "calhoun_blountstown", "calhoun_unincorporated"]

# Dixie County
dixie_entities = ["dixie_cross_city", "dixie_horseshoe_beach", "dixie_unincorporated"]

# Franklin County
franklin_entities = ["franklin_apalachicola", "franklin_carrabelle", "franklin_unincorporated"]

all_entities = {"miami-dade_incorporated", "miami-dade_unincorporated", 
                "broward_unified", "broward_unincorporated", 
                "palm_beach_unified", 
                "hillsborough_plant_city", "hillsborough_tampa", "hillsborough_temple_terrace", "hillsborough_unincorporated", 
                "orange_apopka", "orange_bay_lake", "orange_belle_isle", "orange_eatonville", "orange_edgewood", "orange_lake_buena_vista", "orange_maitland", "orange_oakland", "orange_ocoee", "orange_orlando", "orange_unincorporated", "orange_windermere", "orange_winter_garden", "orange_winter_park", 
                "pinellas_belleair", "pinellas_belleair_beach", "pinellas_belleair_bluffs", "pinellas_belleair_shore", "pinellas_clearwater", "pinellas_dunedin", "pinellas_gulfport", "pinellas_indian_rocks_beach", "pinellas_indian_shores", "pinellas_kenneth_city", "pinellas_largo", "pinellas_madeira_beach", "pinellas_north_redington_beach", "pinellas_oldsmar", "pinellas_pinellas_park", "pinellas_redington_beach", "pinellas_redington_shores", "pinellas_safety_harbor", "pinellas_seminole", "pinellas_south_pasadena", "pinellas_st_pete_beach", "pinellas_st_petersburg", "pinellas_tarpon_springs", "pinellas_treasure_island", "pinellas_unincorporated", 
                "duval_unified", "lee_unincorporated", "lee_bonita_springs", "lee_cape_coral", "lee_fort_myers", "lee_fort_myers_beach", "lee_sanibel", 
                "polk_unincorporated", "polk_auburndale", "polk_bartow", "polk_davenport", "polk_dundee", "polk_eagle_lake", "polk_fort_meade", "polk_frostproof", "polk_haines_city", "polk_highland_park", "polk_hillcrest_heights", "polk_lake_alfred", "polk_lake_hamilton", "polk_lake_wales", "polk_lakeland", "polk_mulberry", "polk_polk_city", "polk_winter_haven", 
                "brevard_cape_canaveral", "brevard_cocoa", "brevard_cocoa_beach", "brevard_indian_harbour_beach", "brevard_indiatlantic", "brevard_malabar", "brevard_melbourne", "brevard_melbourne_beach", "brevard_melbourne_village", "brevard_palm_bay", "brevard_palm_shores", "brevard_rockledge", "brevard_satellite_beach", "brevard_titusville", "brevard_unincorporated", "brevard_west_melbourne", "volusia_daytona_beach", "volusia_daytona_beach_shores", "volusia_de_bary", "volusia_de_land", "volusia_deltona", "volusia_edgewater", "volusia_flagler_beach", "volusia_holly_hill", "volusia_lake_helen", "volusia_new_smyrna_beach", "volusia_oak_hill", "volusia_orange_city", "volusia_ormond_beach", "volusia_pierson", "volusia_ponce_inlet", "volusia_port_orange", "volusia_south_daytona", "volusia_countywide", "volusia_unincorporated", "pasco_dade_city", "pasco_new_port_richey", "pasco_port_richey", "pasco_san_antonio", "pasco_st_leo", "pasco_unincorporated", "pasco_zephyrhills", "seminole_altamonte_springs", "seminole_casselberry", "seminole_lake_mary", "seminole_longwood", "seminole_oviedo", "seminole_sanford", "seminole_unincorporated", "seminole_winter_springs", "sarasota_unincorporated", "sarasota_longboat_key", "sarasota_north_port", "sarasota_sarasota", "sarasota_venice", "manatee_unincorporated", "manatee_anna_maria", "manatee_bradenton", "manatee_bradenton_beach", "manatee_holmes_beach", "manatee_longboat_key", "manatee_palmetto", "collier_unincorporated", "collier_everglades", "collier_marco_island", "collier_naples", "osceola_st_cloud", "osceola_unincorporated", "osecola_kissimmee", "marion_belleview", "marion_dunnellon", "marion_mcintosh", "marion_ocala", "marion_reddick", "marion_unincorporated", "lake_clermont", "lake_eustis", "lake_fruitland_park", "lake_groveland", "lake_lady_lake", "lake_leesburg", "lake_minneola", "lake_mount_dora", "lake_tavares", "lake_umatilla", "lake_astatula", "lake_howey-in-the-hills", "lake_mascotte", "lake_montverde", "lake_unincorporated", "st_lucie_ft_pierce", "st_lucie_port_st_lucie", "st_lucie_unincorporated", "escambia_century", "escambia_pensacola", "escambia_unincorporated", "leon_unified", "alachua_alachua", "alachua_archer", "alachua_gainesville", "alachua_hawthorne", "alachua_high_springs", "alachua_lacrosse", "alachua_micanopy", "alachua_newberry", "alachua_waldo", "alachua_unincorporated", "st_johns_hastings", "st_johns_marineland", "st_johns_st_augustine", "st_johns_st_augustine_beach", "st_johns_unincorporated", "clay_green_cove_springs", "clay_keystone_heights", "clay_orange_park", "clay_penney_farms", "clay_unincorporated", "okaloosa_cinco_bayou", "okaloosa_crestview", "okaloosa_destin", "okaloosa_fort_walton_beach", "okaloosa_laurel_hill", "okaloosa_mary_esther", "okaloosa_niceville", "okaloosa_shalimar", "okaloosa_unincorporated", "okaloosa_valparaiso", "hernando_brooksville", "hernando_unincorporated", "hernando_weeki_wachee", "bay_callaway", "bay_cedar_grove", "bay_lynn_haven", "bay_mexico_beach", "bay_panama_city", "bay_panama_city_beach", "bay_parker", "bay_springfield", "bay_unincorporated", "charlotte_unincorporated", "charlotte_punta_gorda", "santa_rosa_gulf_breeze", "santa_rosa_jay", "santa_rosa_milton", "santa_rosa_unincorporated", "martin_jupiter_island", "martin_ocean_breeze_park", "martin_sewalls_point", "martin_stuart", "martin_unincorporated", "indian_river_fellsmere", "indian_river_indian_river_shores", "indian_river_orchid", "indian_river_sebastian", "indian_river_unincorporated", "indian_river_vero_beach", "citrus_crystal_river", "citrus_inverness", "citrus_unincorporated", "sumter_bushnell", "sumter_center_hill", "sumter_coleman", "sumter_unincorporated", "sumter_webster", "sumter_wildwood", "flagler_beverly_beach", "flagler_bunnell", "flagler_flagler_beach", "flagler_marineland", "flagler_palm_coast", "flagler_unincorporated", "highlands_unincorporated", "highlands_avon_park", "highlands_lake_placid", "highlands_sebring", "nassau_callahan", "nassau_fernandina_beach", "nassau_hilliard", "nassau_unincorporated", "monroe_islamorada_village_of_islands", "monroe_key_colony_beach", "monroe_key_west", "monroe_layton", "monroe_marathon", "monroe_unincorporated", "putnam_crescent_city", "putnam_interlachen", "putnam_palatka", "putnam_pomona_park", "putnam_unincorporated", "putnam_welaka", "walton_de_funiak_springs", "walton_freeport", "walton_paxton", "walton_unincorporated", "columbia_fort_white", "columbia_lake_city", "columbia_unincorporated", "gadsden_chattahoochee", "gadsden_greensboro", "gadsden_gretna", "gadsden_havana", "gadsden_midway", "gadsden_quincy", "gadsden_unincorporated", "suwannee_branford", "suwannee_live_oak", "suwannee_unincorporated", "jackson_alford", "jackson_bascom", "jackson_campbellton", "jackson_cottondale", "jackson_graceville", "jackson_grand_ridge", "jackson_greenwood", "jackson_jacob_city", "jackson_malone", "jackson_marianna", "jackson_sneads", "jackson_unincorporated", "hendry_unincorporated", "hendry_clewiston", "hendry_labelle", "okeechobee_unincorporated", "okeechobee_okeechobee", "levy_bronson", "levy_cedar_key", "levy_chiefland", "levy_fanning_springs", "levy_inglis", "levy_otter_creek", "levy_unincorporated", "levy_williston", "levy_yankeetown", "desoto_unincorporated", "desoto_arcadia", "wakulla_sopchoppy", "wakulla_st_marks", "wakulla_unincorporated", "baker_glen_st_mary", "baker_macclenny", "baker_unincorporated", "bradford_brooker", "bradford_hampton", "bradford_keystone_heights", "bradford_lawtey", "bradford_starke", "bradford_unincorporated", "hardee_unincorporated", "hardee_wauchula", "hardee_zolfo_springs", "hardee_bowling_green", "washington_caryville", "washington_chipley", "washington_ebro", "washington_unincorporated", "washington_vernon", "washington_wausau", "taylor_perry", "taylor_unincorporated", "gilchrist_bell", "gilchrist_fanning_springs", "gilchrist_trenton", "gulf_port_st_joe", "gulf_unincorporated", "gulf_wewahitchka", "union_lake_butler", "union_raiford", "union_unincorporated", "union_worthington_springs", "hamilton_jasper", "hamilton_jennings", "hamilton_unincorporated", "hamilton_white_springs", "jefferson_monticello", "jefferson_unincorporated", "lafayette_mayo", "lafayette_unincorporated", "liberty_bristol", "liberty_unincorporated", "madison_greenville", "madison_lee", "madison_madison", "madison_unincorporated", "glades_unincorporated", "glades_moore_haven", "calhoun_altha", "calhoun_blountstown", "calhoun_unincorporated", "dixie_cross_city", "dixie_horseshoe_beach", "dixie_unincorporated", "franklin_apalachicola", "franklin_carrabelle", "franklin_unincorporated", "gilchrist_unincorporated"}

"""
AI PROMPT:

I need to make a script that chains together a series of python scripts to fetch layer data, process it, and upload it to postgres databases. all layers have the
same steps except for downloading; the script will map out the correct tools to use based on the layer and entity (specified by county and city), and call these. 
in addition to this, when the layer is flu or zoning, an additional step in processing will be needed. to start with the skeleton of the script, map 

"""

# Create a dictionary mapping counties to their entities for easy lookup
county_entity_map = {
    "miami-dade": miami_dade_entities,
    "broward": broward_entities,
    "palm_beach": palm_beach_entities,
    "hillsborough": hillsborough_entities,
    "orange": orange_entities,
    "pinellas": pinellas_entities,
    "duval": duval_entities,
    "lee": lee_entities,
    "polk": polk_entities,
    "brevard": brevard_entities,
    "volusia": volusia_entities,
    "pasco": pasco_entities,
    "seminole": seminole_entities,
    "sarasota": sarasota_entities,
    "manatee": manatee_entities,
    "collier": collier_entities,
    "osceola": osceola_entities,
    "marion": marion_entities,
    "lake": lake_entities,
    "st_lucie": st_lucie_entities,
    "escambia": escambia_entities,
    "leon": leon_entities,
    "alachua": alachua_entities,
    "st_johns": st_johns_entities,
    "clay": clay_entities,
    "okaloosa": okaloosa_entities,
    "hernando": hernando_entities,
    "bay": bay_entities,
    "charlotte": charlotte_entities,
    "santa_rosa": santa_rosa_entities,
    "martin": martin_entities,
    "indian_river": indian_river_entities,
    "citrus": citrus_entities,
    "sumter": sumter_entities,
    "flagler": flagler_entities,
    "highlands": highlands_entities,
    "nassau": nassau_entities,
    "monroe": monroe_entities,
    "putnam": putnam_entities,
    "walton": walton_entities,
    "columbia": columbia_entities,
    "gadsden": gadsden_entities,
    "suwannee": suwannee_entities,
    "jackson": jackson_entities,
    "hendry": hendry_entities,
    "okeechobee": okeechobee_entities,
    "levy": levy_entities,
    "desoto": desoto_entities,
    "wakulla": wakulla_entities,
    "baker": baker_entities,
    "bradford": bradford_entities,
    "hardee": hardee_entities,
    "washington": washington_entities,
    "taylor": taylor_entities,
    "gilchrist": gilchrist_entities,
    "gulf": gulf_entities,
    "union": union_entities,
    "hamilton": hamilton_entities,
    "jefferson": jefferson_entities,
    "lafayette": lafayette_entities,
    "liberty": liberty_entities,
    "madison": madison_entities,
    "glades": glades_entities,
    "calhoun": calhoun_entities,
    "dixie": dixie_entities,
    "franklin": franklin_entities
}