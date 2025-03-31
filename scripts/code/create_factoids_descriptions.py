import json
import re
from rdflib import Graph, Literal, URIRef, Namespace, XSD
from namespaces import NameSpaces
import file_management as fm
import multi_sources_processing as msp
import graphrdf as gr
import str_processing as sp
import geom_processing as gp

np = NameSpaces()

################################################## Generate descriptions ######################################################

def create_landmark_version_description(lm_id, lm_label, lm_type:str, lang:str, lm_attributes:dict, lm_provenance:dict, time_description:dict):
    """
    Create a landmark version description
    """

    description = {
        "id": lm_id,
        "label": lm_label,
        "type": lm_type,
        "lang": lang,
        "attributes": lm_attributes,
        "provenance": lm_provenance,
        "time": time_description
    }

    return description

def create_landmark_relation_description(lr_id, lr_type:str, locatum_id:str, relatum_ids:list[str], lm_provenance:dict, time_description:dict):
    """
    Create a landmark relation description
    """
    description = {
        "id": lr_id,
        "type": lr_type,
        "locatum": locatum_id,
        "relatum": relatum_ids,
        "provenance": lm_provenance,
        "time": time_description
    }

    return description

def create_address_description(addr_uuid:str, addr_label:str, lang:str, target_uuid:str, segment_uuids:list[str], lm_provenance:dict):
    """
    Create an address description
    """
    description = {
        "id": addr_uuid,
        "label": addr_label,
        "lang": lang,
        "target": target_uuid,
        "segments": segment_uuids,
        "provenance": lm_provenance,
    }

    return description

##################################################### BAN ##########################################################

def create_state_description_for_ban(ban_file:str, valid_time:dict, lang:str, ban_ns:Namespace):
    landmarks_desc = []
    relations_desc = []
    addresses_desc = []
    thoroughfares = {} # {"Rue Gérard":"12345678-1234-5678-1234-567812345678"}
    arrdts = {} # {"Paris 1er Arrondissement":"12345678-1234-5678-1234-567812345678"}
    cps = {} # {"75001":"12345678-1234-5678-1234-567812345678"}
    
    ## BAN file columns
    hn_id_col, hn_number_col, hn_rep_col, hn_lon_col, hn_lat_col = "id", "numero", "rep", "lon", "lat"
    th_name_col, th_fantoir_col = "nom_voie",  "id_fantoir"
    cp_number_col = "code_postal"
    arrdt_name_col, arrdt_insee_col = "nom_commune", "code_insee"

    content = fm.read_csv_file_as_dict(ban_file, id_col=hn_id_col, delimiter=";", encoding='utf-8-sig')
    for value in content.values():
        hn, th, arrdt, cp = create_landmarks_descriptions_from_ban_line(value, valid_time, lang, ban_ns,
                                                                       hn_id_col, hn_number_col, hn_rep_col, hn_lon_col, hn_lat_col,
                                                                       th_name_col, th_fantoir_col, cp_number_col,
                                                                       arrdt_name_col, arrdt_insee_col,
                                                                       thoroughfares, arrdts, cps)
        hn_label = value.get(hn_number_col) + value.get(hn_rep_col)
        th_label = value.get(th_name_col)
        arrdt_label = value.get(arrdt_name_col)
        cp_label = value.get(cp_number_col)

        # Add descriptions in landmarks_desc
        landmarks_desc.append(hn[0])
        if th[0] is not None:
            landmarks_desc.append(th[0])
            thoroughfares[th_label] = th[1]
        if arrdt[0] is not None:
            landmarks_desc.append(arrdt[0])
            arrdts[arrdt_label] = arrdt[1]
        if cp[0] is not None:
            landmarks_desc.append(cp[0])
            cps[cp_label] = cp[1]

        # Get URI for landmark relation provenance
        hn_id = value.get(hn_id_col)
        provenance_uri = str(ban_ns[hn_id])

        # Create landmark relation descriptions
        lr_descs, lr_uuids = create_landmark_relations_descriptions_from_ban_line(hn[1], th[1], arrdt[1], cp[1], provenance_uri, valid_time)
        relations_desc += lr_descs  

        # Create address description
        addr_label = f"{hn_label} {th_label}, {cp_label} {arrdt_label}"
        addr_desc = create_address_description_from_ban_line(addr_label, lang, hn[1], lr_uuids, {"uri":provenance_uri})
        addresses_desc.append(addr_desc)      

    return {"landmarks":landmarks_desc, "relations":relations_desc, "addresses":addresses_desc}

def create_address_description_from_ban_line(label:str, lang:str, target_uuid:str, segment_uuids:list[URIRef], lm_provenance:dict):
    addr_uuid = gr.generate_uuid()
    addr_desc = create_address_description(addr_uuid, label, lang, target_uuid, segment_uuids, lm_provenance)
    return addr_desc

def create_landmarks_descriptions_from_ban_line(value, valid_time, lang, ban_ns,
                                               hn_id_col, hn_number_col, hn_rep_col, hn_lon_col, hn_lat_col,
                                               th_name_col, th_fantoir_col, cp_number_col,
                                               arrdt_name_col, arrdt_insee_col,
                                               thoroughfares, arrdts, cps):
    
    # Create house number description
    hn_id = value.get(hn_id_col)
    hn_label = value.get(hn_number_col) + value.get(hn_rep_col)
    hn_geom = "POINT (" + value.get(hn_lon_col) + " " + value.get(hn_lat_col) + ")"        
    hn_uuid, hn_desc = create_house_number_description_for_ban(hn_label, hn_geom, hn_id, lang, valid_time, ban_ns)

    # Create thoroughfare description (if not exists)
    th_label = value.get(th_name_col)
    th_id = value.get(th_fantoir_col)
    th_uuid, th_desc = thoroughfares.get(th_label), None
    if th_uuid is None:
        th_uuid, th_desc = create_thoroughfare_description_for_ban(th_label, th_id, lang, valid_time, ban_ns)

    arrdt_label = value.get(arrdt_name_col)
    arrdt_id = value.get(arrdt_insee_col)
    arrdt_uuid, arrdt_desc = arrdts.get(arrdt_label), None
    if arrdt_uuid is None:
        arrdt_uuid, arrdt_desc = create_arrondissement_description_for_ban(arrdt_label, arrdt_id, lang, valid_time, ban_ns)

    cp_label = value.get(cp_number_col)
    cp_uuid, cp_desc = cps.get(cp_label), None
    if cp_uuid is None:
        cp_uuid, cp_desc = create_postal_code_area_description_for_ban(cp_label, cp_label, lang, valid_time, ban_ns)

    return [hn_desc, hn_uuid], [th_desc, th_uuid], [arrdt_desc, arrdt_uuid], [cp_desc, cp_uuid]

def create_landmark_relations_descriptions_from_ban_line(hn_uuid, th_uuid, arrdt_uuid, cp_uuid, provenance_uri, valid_time:dict):
    lr_uuid_1, lr_uuid_2, lr_uuid_3 = gr.generate_uuid(), gr.generate_uuid(), gr.generate_uuid()
    lr_desc_1 = create_landmark_relation_description(lr_uuid_1, "belongs", hn_uuid, [th_uuid], {"uri":provenance_uri}, valid_time)
    lr_desc_2 = create_landmark_relation_description(lr_uuid_2, "within", hn_uuid, [arrdt_uuid], {"uri":provenance_uri}, valid_time)
    lr_desc_3 = create_landmark_relation_description(lr_uuid_3, "within", hn_uuid, [cp_uuid], {"uri":provenance_uri}, valid_time)
    return [lr_desc_1, lr_desc_2, lr_desc_3], [lr_uuid_1, lr_uuid_2, lr_uuid_3]


def create_house_number_description_for_ban(hn_label:str, hn_geom:str, hn_id:str, lang:str, valid_time:dict, ban_ns:Namespace):
    hn_uuid = gr.generate_uuid()
    hn_type = "street_number"
    hn_attrs = {"name":{"value":hn_label}, "geometry": {"value":hn_geom, "datatype":"wkt_literal"}}
    hn_provenance = {"uri":ban_ns[hn_id]}
    hn_desc = create_landmark_version_description(hn_uuid, hn_label, hn_type, lang, hn_attrs, hn_provenance, valid_time)

    return hn_uuid, hn_desc

def create_thoroughfare_description_for_ban(th_label:str, th_id:str, lang:str, valid_time:dict, ban_ns:Namespace):
    th_uuid = gr.generate_uuid()
    th_type = "thoroughfare"
    th_attrs = {"name":{"value":th_label, "lang":lang}}
    th_provenance = {"uri":ban_ns[th_id]}
    th_desc = create_landmark_version_description(th_uuid, th_label, th_type, lang, th_attrs, th_provenance, valid_time)

    return th_uuid, th_desc

def create_arrondissement_description_for_ban(arrdt_label:str, arrdt_id:str, lang:str, valid_time:dict, ban_ns:Namespace):
    arrdt_uuid = gr.generate_uuid()
    arrdt_type = "district"
    arrdt_attrs = {"name":{"value":arrdt_label, "lang":lang}, "insee_code":{"value":arrdt_id}}
    arrdt_provenance = {"uri":ban_ns[arrdt_id]}
    arrdt_desc = create_landmark_version_description(arrdt_uuid, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance, valid_time)

    return arrdt_uuid, arrdt_desc

def create_postal_code_area_description_for_ban(cp_label:str, cp_id:str, lang:str, valid_time:dict, ban_ns:Namespace):
    cp_uuid = gr.generate_uuid()
    cp_type = "postal_code_area"
    cp_attrs = {"name":{"value":cp_label}}
    cp_provenance = {"uri":ban_ns[cp_id]}
    cp_desc = create_landmark_version_description(cp_uuid, cp_label, cp_type, lang, cp_attrs, cp_provenance, valid_time)

    return cp_uuid, cp_desc

##################################################### OSM ##########################################################

def create_state_description_for_osm(osm_file:str, osm_hn_file:str, valid_time:dict, lang:str, ban_ns:Namespace):
    landmarks_desc = []
    relations_desc = []
    osm_relations = [] # ["https://www.openstreetmap.org/relation/11832935", "https://www.openstreetmap.org/relation/11832936"]
    
    ## OSM file columns
    hn_id_col, hn_number_col, hn_geom_col = "houseNumberId", "houseNumberLabel", "houseNumberGeomWKT"
    th_id_col, th_name_col = "streetId",  "streetName"
    arrdt_id_col, arrdt_name_col, arrdt_insee_col = "arrdtId", "arrdtName", "arrdtInsee"

    # Read the two files and merge their content
    content = merge_content_of_osm_files(osm_file, osm_hn_file, hn_id_col)

    for value in content.values():
        hn, th, arrdt = create_landmarks_descriptions_from_osm_line(value, valid_time, lang, ban_ns,
                                                                    hn_id_col, hn_number_col, hn_geom_col, th_id_col, th_name_col,
                                                                    arrdt_id_col, arrdt_name_col, arrdt_insee_col,
                                                                    osm_relations)
        
        # Add descriptions in landmarks_desc
        landmarks_desc.append(hn[0])
        if th[0] is not None:
            landmarks_desc.append(th[0])
            osm_relations.append(th[1])
        if arrdt[0] is not None:
            landmarks_desc.append(arrdt[0])
            osm_relations.append(arrdt[1])

        # Create landmark relation descriptions
        lr_descs = create_landmark_relations_descriptions_from_osm_line(hn[1], th[1], arrdt[1], valid_time)
        relations_desc += lr_descs

    return {"landmarks":landmarks_desc, "relations":relations_desc}
        
def merge_content_of_osm_files(osm_file, osm_hn_file, hn_id_col):
    # Read the two files
    content_osm = fm.read_csv_file_as_dict(osm_file, id_col=hn_id_col, delimiter=",", encoding='utf-8-sig')
    content_osm_hn = fm.read_csv_file_as_dict(osm_hn_file, id_col=hn_id_col, delimiter=",", encoding='utf-8-sig')

    # Merge the two contents
    content = {}
    for key_osm, value_osm in content_osm.items():
        value_osm_hn = content_osm_hn.get(key_osm)
        value = {**value_osm, **value_osm_hn}
        content[key_osm] = value

    return content

def create_landmarks_descriptions_from_osm_line(value, valid_time, lang, osm_ns,
                                                hn_id_col, hn_number_col, hn_geom_col, th_id_col, th_name_col,
                                                arrdt_id_col, arrdt_name_col, arrdt_insee_col,
                                                osm_relations):
    # Create house number description
    hn_id = value.get(hn_id_col)
    hn_geom = value.get(hn_geom_col)
    hn_label = value.get(hn_number_col)      
    hn_desc = create_house_number_description_for_osm(hn_label, hn_geom, hn_id, lang, valid_time)

    # Create thoroughfare description (if not exists)
    th_id = value.get(th_id_col)
    th_label = value.get(th_name_col)
    th_desc = None
    if th_id not in osm_relations:
        th_desc = create_thoroughfare_description_for_osm(th_label, th_id, lang, valid_time)

    arrdt_id = value.get(arrdt_id_col)
    arrdt_label = value.get(arrdt_name_col)
    arrdt_insee = value.get(arrdt_insee_col)
    arrdt_desc = None
    if arrdt_id not in osm_relations:
        arrdt_desc = create_arrondissement_description_for_osm(arrdt_label, arrdt_id, arrdt_insee, lang, valid_time)

    return [hn_desc, hn_id], [th_desc, th_id], [arrdt_desc, arrdt_id]

def create_landmark_relations_descriptions_from_osm_line(hn_uuid, th_uuid, arrdt_uuid, valid_time:dict):
    lr_uuid_1, lr_uuid_2 = gr.generate_uuid(), gr.generate_uuid()
    lr_desc_1 = create_landmark_relation_description(lr_uuid_1, "belongs", hn_uuid, [th_uuid], {"uri":th_uuid}, valid_time)
    lr_desc_2 = create_landmark_relation_description(lr_uuid_2, "within", hn_uuid, [arrdt_uuid], {"uri":arrdt_uuid}, valid_time)
    return [lr_desc_1, lr_desc_2]

def create_house_number_description_for_osm(hn_label:str, hn_geom:str, hn_id:str, lang:str, valid_time:dict):
    hn_type = "street_number"
    hn_attrs = {"name":{"value":hn_label}, "geometry": {"value":hn_geom, "datatype":"wkt_literal"}}
    hn_provenance = {"uri":hn_id}
    hn_desc = create_landmark_version_description(hn_id, hn_label, hn_type, lang, hn_attrs, hn_provenance, valid_time)
    return hn_desc

def create_thoroughfare_description_for_osm(th_label:str, th_id:str, lang:str, valid_time:dict):
    th_type = "thoroughfare"
    th_attrs = {"name":{"value":th_label, "lang":lang}}
    th_provenance = {"uri":th_id}
    th_desc = create_landmark_version_description(th_id, th_label, th_type, lang, th_attrs, th_provenance, valid_time)
    return th_desc

def create_arrondissement_description_for_osm(arrdt_label:str, arrdt_id:str, arrdt_insee:str, lang:str, valid_time:dict):
    arrdt_type = "district"
    arrdt_attrs = {"name":{"value":arrdt_label, "lang":lang}, "insee_code":{"value":arrdt_insee}}
    arrdt_provenance = {"uri":arrdt_id}
    arrdt_desc = create_landmark_version_description(arrdt_id, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance, valid_time)
    return arrdt_desc

##################################################### Ville de Paris ##########################################################

def create_state_description_for_ville_paris_actuelles(vpa_file, valid_time, lang, vpa_ns):
    events_desc = []
    landmarks_desc = []
    relations_desc = []
    districts = {} # {"Buttes-aux-Cailles":"12345678-1234-5678-1234-685544777"}
    arrdts = {} # {"13e":"12345678-1234-5678-1234-567812345678"}

    # File columns
    id_col = "Identifiant"
    name_col = "Dénomination complète minuscule"
    start_time_col = "Date de l'arrété"
    arrdt_col = "Arrondissement"
    district_col = "Quartier"
    geom_col = "geo_shape"

    content = fm.read_csv_file_as_dict(vpa_file, id_col=id_col, delimiter=";", encoding='utf-8-sig')

    for value in content.values():
        th, th_districts, th_arrdts = create_landmarks_descriptions_for_ville_paris_actuelles_line(value, valid_time, lang, vpa_ns,
                                                                                                   id_col, name_col, arrdt_col, district_col, geom_col,
                                                                                                   districts, arrdts)
 
        add_descriptions_in_landmarks_desc_for_ville_paris_actuelles_line(landmarks_desc, th, th_districts, th_arrdts, districts, arrdts)
        district_and_arrdt_uris = [x[1] for x in th_districts + th_arrdts]
        lr_descs = create_landmark_relations_descriptions_for_ville_paris_actuelles_line(th[1], district_and_arrdt_uris, valid_time)
        relations_desc += lr_descs

    return {"landmarks":landmarks_desc, "relations":relations_desc}

def add_descriptions_in_landmarks_desc_for_ville_paris_actuelles_line(landmarks_desc, th, th_districts, th_arrdts, districts, arrdts):
    landmarks_desc.append(th[0])

    for district in th_districts:
        if district[0] is not None:
            districts[district[2]] = district[1]
            landmarks_desc.append(district[0])
    for arrdt in th_arrdts:
        if arrdt[0] is not None:
            arrdts[arrdt[2]] = arrdt[1]
            landmarks_desc.append(arrdt[0])

def create_landmarks_descriptions_for_ville_paris_actuelles_line(value, valid_time, lang, vpa_ns,
                                                                  id_col, name_col, arrdt_col, district_col, geom_col,
                                                                  districts, arrdts):
    th_id = value.get(id_col)
    th_label = value.get(name_col)
    th_geom = value.get(geom_col)
    th_arrdt_labels = sp.split_cell_content(value.get(arrdt_col), sep=",")
    th_district_labels = sp.split_cell_content(value.get(district_col), sep=",")

    th_desc = create_thoroughfare_description_for_ville_paris_actuelles(th_label, th_id, th_geom, lang, valid_time, vpa_ns)

    th_districts, th_arrdts = [], []

    for lab in th_district_labels:
        district_uuid, district_desc = districts.get(lab), None
        if lab is not None:
            district_uuid, district_desc = create_district_description_for_ville_paris_actuelles(lab, lang, valid_time, vpa_ns)
            th_districts.append([district_desc, district_uuid, lab])

    for lab in th_arrdt_labels:
        arrdt_uuid, arrdt_desc = arrdts.get(lab), None
        if lab is not None:
            arrdt_uuid, arrdt_desc = create_arrondissement_description_for_ville_paris_actuelles(lab, lang, valid_time, vpa_ns)
            th_arrdts.append([arrdt_desc, arrdt_uuid, lab])

    return [th_desc, th_id], th_districts, th_arrdts

def create_landmark_relations_descriptions_for_ville_paris_actuelles_line(th_uuid, district_and_arrdt_uuids, valid_time:dict):
    lr_descs = []
    for uuid in district_and_arrdt_uuids:
        lr_uuid = gr.generate_uuid()
        lr_desc = create_landmark_relation_description(lr_uuid, "within", th_uuid, [uuid], {"uri":th_uuid}, valid_time)
        lr_descs.append(lr_desc)

    return lr_descs

def create_thoroughfare_description_for_ville_paris_actuelles(th_label:str, th_id:str, th_geom:str, lang:str, valid_time:dict, vpa_ns:Namespace):
    th_type = "thoroughfare"
    th_wkt_geom = gp.from_geojson_to_wkt(json.loads(th_geom))
    th_attrs = {"name":{"value":th_label, "lang":lang}, "geometry": {"value":th_wkt_geom, "datatype":"wkt_literal"}}
    th_provenance = {"uri":str(vpa_ns[th_id])}
    th_desc = create_landmark_version_description(th_id, th_label, th_type, lang, th_attrs, th_provenance, valid_time)
    return th_desc

def create_district_description_for_ville_paris_actuelles(district_label:str, lang:str, valid_time:dict, vpa_ns:Namespace):
    district_uuid = gr.generate_uuid()
    district_type = "district"
    district_attrs = {"name":{"value":district_label, "lang":lang}}
    district_provenance = {"uri":str(vpa_ns)}
    district_desc = create_landmark_version_description(district_uuid, district_label, district_type, lang, district_attrs, district_provenance, valid_time)
    return district_uuid, district_desc

def create_arrondissement_description_for_ville_paris_actuelles(arrdt_label:str, lang:str, valid_time:dict, vpa_ns:Namespace):
    arrdt_uuid = gr.generate_uuid()
    arrdt_type = "district"
    arrdt_label = re.sub("^0", "", arrdt_label.replace("01e", "01er")) + " arrondissement de Paris"
    arrdt_attrs = {"name":{"value":arrdt_label, "lang":lang}}
    arrdt_provenance = {"uri":str(vpa_ns)}
    arrdt_desc = create_landmark_version_description(arrdt_uuid, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance, valid_time)
    return arrdt_uuid, arrdt_desc
