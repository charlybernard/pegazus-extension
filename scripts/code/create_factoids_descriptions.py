import json
import re
from rdflib import Graph, Literal, URIRef, Namespace, XSD
from namespaces import NameSpaces
import file_management as fm
import multi_sources_processing as msp
import graphrdf as gr
import str_processing as sp
import geom_processing as gp
import description_initialisation as di
np = NameSpaces()

##################################################### BAN ##########################################################

def create_state_description_for_ban(ban_file:str, valid_time:dict, source:dict, lang:str, ban_ns:Namespace):
    landmarks_desc = []
    relations_desc = []
    addresses_desc = []
    thoroughfares = {} # {"Rue Gérard":"12345678-1234-5678-1234-567812345678"}
    arrdts = {} # {"Paris 1er Arrondissement":"12345678-1234-5678-1234-567812345678"}
    cps = {} # {"75001":"12345678-1234-5678-1234-567812345678"}
    
    ## BAN file columns
    sn_id_col, sn_number_col, sn_rep_col, sn_lon_col, sn_lat_col = "id", "numero", "rep", "lon", "lat"
    th_name_col, th_fantoir_col = "nom_voie",  "id_fantoir"
    cp_number_col = "code_postal"
    arrdt_name_col, arrdt_insee_col = "nom_commune", "code_insee"

    content = fm.read_csv_file_as_dict(ban_file, id_col=sn_id_col, delimiter=";", encoding='utf-8-sig')
    for value in content.values():
        hn, th, arrdt, cp = create_landmarks_descriptions_from_ban_line(value, lang, ban_ns,
                                                                       sn_id_col, sn_number_col, sn_rep_col, sn_lon_col, sn_lat_col,
                                                                       th_name_col, th_fantoir_col, cp_number_col,
                                                                       arrdt_name_col, arrdt_insee_col,
                                                                       thoroughfares, arrdts, cps)
        sn_label = value.get(sn_number_col) + value.get(sn_rep_col)
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
        sn_id = value.get(sn_id_col)
        provenance_uri = str(ban_ns[sn_id])

        # Create landmark relation descriptions
        lr_descs, lr_uuids = create_landmark_relations_descriptions_from_ban_line(hn[1], th[1], arrdt[1], cp[1], provenance_uri)
        relations_desc += lr_descs  

        # Create address description
        addr_label = f"{sn_label} {th_label}, {cp_label} {arrdt_label}"
        addr_prov_desc = {"uri":provenance_uri}
        addr_desc = create_address_description_from_ban_line(addr_label, lang, hn[1], lr_uuids, addr_prov_desc)
        addresses_desc.append(addr_desc)      

    description = {"landmarks":landmarks_desc, "relations":relations_desc, "addresses":addresses_desc}
    if isinstance(valid_time, dict):
        description["time"] = valid_time
    if isinstance(source, dict):
        description["source"] = source

    return description

def create_address_description_from_ban_line(label:str, lang:str, target_uuid:str, segment_uuids:list[URIRef], lm_provenance:dict):
    addr_uuid = gr.generate_uuid()
    addr_desc = di.create_address_description(addr_uuid, label, lang, target_uuid, segment_uuids, lm_provenance)
    return addr_desc

def create_landmarks_descriptions_from_ban_line(value, lang, ban_ns,
                                               sn_id_col, sn_number_col, sn_rep_col, sn_lon_col, sn_lat_col,
                                               th_name_col, th_fantoir_col, cp_number_col,
                                               arrdt_name_col, arrdt_insee_col,
                                               thoroughfares, arrdts, cps):
    
    # Create street number description
    sn_id = value.get(sn_id_col)
    sn_label = value.get(sn_number_col) + value.get(sn_rep_col)
    sn_geom = "POINT (" + value.get(sn_lon_col) + " " + value.get(sn_lat_col) + ")"        
    sn_uuid, sn_desc = create_streetnumber_description_for_ban(sn_label, sn_geom, sn_id, ban_ns)

    # Create thoroughfare description (if not exists)
    th_label = value.get(th_name_col)
    th_id = value.get(th_fantoir_col)
    th_uuid, th_desc = thoroughfares.get(th_label), None
    if th_uuid is None:
        th_uuid, th_desc = create_thoroughfare_description_for_ban(th_label, th_id, lang, ban_ns)

    arrdt_label = value.get(arrdt_name_col)
    arrdt_id = value.get(arrdt_insee_col)
    arrdt_uuid, arrdt_desc = arrdts.get(arrdt_label), None
    if arrdt_uuid is None:
        arrdt_uuid, arrdt_desc = create_arrondissement_description_for_ban(arrdt_label, arrdt_id, lang, ban_ns)

    cp_label = value.get(cp_number_col)
    cp_uuid, cp_desc = cps.get(cp_label), None
    if cp_uuid is None:
        cp_uuid, cp_desc = create_postal_code_area_description_for_ban(cp_label, cp_label, lang, ban_ns)

    return [sn_desc, sn_uuid], [th_desc, th_uuid], [arrdt_desc, arrdt_uuid], [cp_desc, cp_uuid]

def create_landmark_relations_descriptions_from_ban_line(sn_uuid, th_uuid, arrdt_uuid, cp_uuid, provenance_uri):
    lr_uuid_1, lr_uuid_2, lr_uuid_3 = gr.generate_uuid(), gr.generate_uuid(), gr.generate_uuid()
    lr_desc_1 = di.create_landmark_relation_version_description(lr_uuid_1, "belongs", sn_uuid, [th_uuid], {"uri":provenance_uri})
    lr_desc_2 = di.create_landmark_relation_version_description(lr_uuid_2, "within", sn_uuid, [arrdt_uuid], {"uri":provenance_uri})
    lr_desc_3 = di.create_landmark_relation_version_description(lr_uuid_3, "within", sn_uuid, [cp_uuid], {"uri":provenance_uri})
    return [lr_desc_1, lr_desc_2, lr_desc_3], [lr_uuid_1, lr_uuid_2, lr_uuid_3]


def create_streetnumber_description_for_ban(sn_label:str, sn_geom:str, sn_id:str, ban_ns:Namespace):
    sn_uuid = gr.generate_uuid()
    sn_type = "street_number"
    sn_attrs = {"name":{"value":sn_label}, "geometry": {"value":sn_geom, "datatype":"wkt_literal"}}
    sn_provenance = {"uri":ban_ns[sn_id]}
    sn_desc = di.create_landmark_version_description(sn_uuid, sn_label, sn_type, None, sn_attrs, sn_provenance)

    return sn_uuid, sn_desc

def create_thoroughfare_description_for_ban(th_label:str, th_id:str, lang:str, ban_ns:Namespace):
    th_uuid = gr.generate_uuid()
    th_type = "thoroughfare"
    th_attrs = {"name":{"value":th_label, "lang":lang}}
    th_provenance = {"uri":ban_ns[th_id]}
    th_desc = di.create_landmark_version_description(th_uuid, th_label, th_type, lang, th_attrs, th_provenance)

    return th_uuid, th_desc

def create_arrondissement_description_for_ban(arrdt_label:str, arrdt_id:str, lang:str, ban_ns:Namespace):
    arrdt_uuid = gr.generate_uuid()
    arrdt_type = "district"
    arrdt_attrs = {"name":{"value":arrdt_label, "lang":lang}, "insee_code":{"value":arrdt_id}}
    arrdt_provenance = {"uri":ban_ns[arrdt_id]}
    arrdt_desc = di.create_landmark_version_description(arrdt_uuid, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance)

    return arrdt_uuid, arrdt_desc

def create_postal_code_area_description_for_ban(cp_label:str, cp_id:str, lang:str, ban_ns:Namespace):
    cp_uuid = gr.generate_uuid()
    cp_type = "postal_code_area"
    cp_attrs = {"name":{"value":cp_label}}
    cp_provenance = {"uri":ban_ns[cp_id]}
    cp_desc = di.create_landmark_version_description(cp_uuid, cp_label, cp_type, lang, cp_attrs, cp_provenance)

    return cp_uuid, cp_desc

##################################################### OSM ##########################################################

def create_state_description_for_osm(osm_file:str, osm_sn_file:str, valid_time:dict, source:dict, lang:str, ban_ns:Namespace):
    landmarks_desc = []
    relations_desc = []
    osm_relations = [] # ["https://www.openstreetmap.org/relation/11832935", "https://www.openstreetmap.org/relation/11832936"]
    
    ## OSM file columns
    sn_id_col, sn_number_col, sn_geom_col = "houseNumberId", "houseNumberLabel", "houseNumberGeomWKT"
    th_id_col, th_name_col = "streetId",  "streetName"
    arrdt_id_col, arrdt_name_col, arrdt_insee_col = "arrdtId", "arrdtName", "arrdtInsee"

    # Read the two files and merge their content
    content = merge_content_of_osm_files(osm_file, osm_sn_file, sn_id_col)

    for value in content.values():
        hn, th, arrdt = create_landmarks_descriptions_from_osm_line(value, lang,
                                                                    sn_id_col, sn_number_col, sn_geom_col, th_id_col, th_name_col,
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
        lr_descs = create_landmark_relations_descriptions_from_osm_line(hn[1], th[1], arrdt[1])
        relations_desc += lr_descs

    description = {"landmarks":landmarks_desc, "relations":relations_desc}
    if isinstance(valid_time, dict):
        description["time"] = valid_time
    if isinstance(source, dict):
        description["source"] = source

    return description
        
def merge_content_of_osm_files(osm_file, osm_sn_file, sn_id_col):
    # Read the two files
    content_osm = fm.read_csv_file_as_dict(osm_file, id_col=sn_id_col, delimiter=",", encoding='utf-8-sig')
    content_osm_hn = fm.read_csv_file_as_dict(osm_sn_file, id_col=sn_id_col, delimiter=",", encoding='utf-8-sig')

    # Merge the two contents
    content = {}
    for key_osm, value_osm in content_osm.items():
        value_osm_hn = content_osm_hn.get(key_osm)
        value = {**value_osm, **value_osm_hn}
        content[key_osm] = value

    return content

def create_landmarks_descriptions_from_osm_line(value, lang,
                                                sn_id_col, sn_number_col, sn_geom_col, th_id_col, th_name_col,
                                                arrdt_id_col, arrdt_name_col, arrdt_insee_col,
                                                osm_relations):
    # Create house number description
    sn_id = value.get(sn_id_col)
    sn_geom = value.get(sn_geom_col)
    sn_label = value.get(sn_number_col)      
    sn_desc = create_streetnumber_description_for_osm(sn_label, sn_geom, sn_id)

    # Create thoroughfare description (if not exists)
    th_id = value.get(th_id_col)
    th_label = value.get(th_name_col)
    th_desc = None
    if th_id not in osm_relations:
        th_desc = create_thoroughfare_description_for_osm(th_label, th_id, lang)

    arrdt_id = value.get(arrdt_id_col)
    arrdt_label = value.get(arrdt_name_col)
    arrdt_insee = value.get(arrdt_insee_col)
    arrdt_desc = None
    if arrdt_id not in osm_relations:
        arrdt_desc = create_arrondissement_description_for_osm(arrdt_label, arrdt_id, arrdt_insee, lang)

    return [sn_desc, sn_id], [th_desc, th_id], [arrdt_desc, arrdt_id]

def create_landmark_relations_descriptions_from_osm_line(sn_uuid, th_uuid, arrdt_uuid):
    lr_uuid_1, lr_uuid_2 = gr.generate_uuid(), gr.generate_uuid()
    lr_desc_1 = di.create_landmark_relation_version_description(lr_uuid_1, "belongs", sn_uuid, [th_uuid], {"uri":th_uuid})
    lr_desc_2 = di.create_landmark_relation_version_description(lr_uuid_2, "within", sn_uuid, [arrdt_uuid], {"uri":arrdt_uuid})
    return [lr_desc_1, lr_desc_2]

def create_streetnumber_description_for_osm(sn_label:str, sn_geom:str, sn_id:str):
    sn_type = "street_number"
    sn_attrs = {"name":{"value":sn_label}, "geometry": {"value":sn_geom, "datatype":"wkt_literal"}}
    sn_provenance = {"uri":sn_id}
    sn_desc = di.create_landmark_version_description(sn_id, sn_label, sn_type, None, sn_attrs, sn_provenance)
    return sn_desc

def create_thoroughfare_description_for_osm(th_label:str, th_id:str, lang:str):
    th_type = "thoroughfare"
    th_attrs = {"name":{"value":th_label, "lang":lang}}
    th_provenance = {"uri":th_id}
    th_desc = di.create_landmark_version_description(th_id, th_label, th_type, lang, th_attrs, th_provenance)
    return th_desc

def create_arrondissement_description_for_osm(arrdt_label:str, arrdt_id:str, arrdt_insee:str, lang:str):
    arrdt_type = "district"
    arrdt_attrs = {"name":{"value":arrdt_label, "lang":lang}, "insee_code":{"value":arrdt_insee}}
    arrdt_provenance = {"uri":arrdt_id}
    arrdt_desc = di.create_landmark_version_description(arrdt_id, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance)
    return arrdt_desc

##################################################### Ville de Paris ##########################################################

def create_state_and_event_description_for_ville_paris_actuelles(vpa_file, valid_time:dict, source:dict, lang, vp_ns:Namespace):
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
        th, th_districts, th_arrdts = create_landmarks_descriptions_for_ville_paris_actuelles_line(value, lang, vp_ns,
                                                                                                   id_col, name_col, arrdt_col, district_col, geom_col,
                                                                                                   districts, arrdts)
        start_time_stamp = value.get(start_time_col)
        if start_time_stamp is not None and start_time_stamp != "":
            th_label = value.get(name_col)
            provenance = {"uri":str(vp_ns[value.get(id_col)])}
            ev = create_landmark_appearance_event_for_ville_paris(th_label, lang, provenance, start_time_stamp)
            events_desc.append(ev)
    
        add_descriptions_in_landmarks_desc_for_ville_paris_actuelles_line(landmarks_desc, th, th_districts, th_arrdts, districts, arrdts)
        district_and_arrdt_uris = [x[1] for x in th_districts + th_arrdts]
        lr_descs = create_landmark_relations_descriptions_for_ville_paris_line(th[1], district_and_arrdt_uris, vp_ns)
        relations_desc += lr_descs

    states_description = {"landmarks":landmarks_desc, "relations":relations_desc}
    events_description = {"events":events_desc}
    if isinstance(valid_time, dict):
        states_description["time"] = valid_time
    if isinstance(source, dict):
        states_description["source"] = source
        events_description["source"] = source

    return states_description, events_description

def create_event_description_for_ville_paris_caduques(vpc_file:str, source:dict, lang:str, vp_ns:Namespace):
    events_desc = []

    # File columns
    id_col = "Identifiant"
    name_col = "Dénomination complète minuscule"
    start_time_col = "Date de l'arrêté"
    end_time_col = "Date de caducité"
    arrdt_col = "Arrondissement"
    district_col = "Quartier"

    content = fm.read_csv_file_as_dict(vpc_file, id_col=id_col, delimiter=";", encoding='utf-8-sig')

    # Create events descriptions
    for value in content.values():
        # :warning: if start_time_stamp and end_time_stamp do not exist, no event will not be created
        lm_label = value.get(name_col)
        provenance = {"uri":str(vp_ns[value.get(id_col)])}
        start_time_stamp = value.get(start_time_col)
        end_time_stamp = value.get(end_time_col)
        if start_time_stamp is not None and start_time_stamp != "":
            ev_desc_app = create_landmark_appearance_event_for_ville_paris(lm_label, lang, provenance, start_time_stamp)
            events_desc.append(ev_desc_app)
        if end_time_stamp is not None and end_time_stamp != "":
            ev_desc_dis = create_landmark_disappearance_event_for_ville_paris(lm_label, lang, provenance, end_time_stamp)
            events_desc.append(ev_desc_dis)

    description = {"events":events_desc}
    if isinstance(source, dict):
        description["source"] = source

    return description

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

def create_landmarks_descriptions_for_ville_paris_actuelles_line(value, lang, vp_ns,
                                                                id_col, name_col, arrdt_col, district_col, geom_col,
                                                                districts, arrdts):
    th_id = value.get(id_col)
    th_label = value.get(name_col)
    th_geom = value.get(geom_col)
    th_arrdt_labels = sp.split_cell_content(value.get(arrdt_col), sep=",")
    th_district_labels = sp.split_cell_content(value.get(district_col), sep=",")

    th_desc = create_thoroughfare_description_for_ville_paris(th_label, th_id, th_geom, lang, vp_ns)

    th_districts, th_arrdts = [], []

    for lab in th_district_labels:
        district_uuid, district_desc = districts.get(lab), None
        if district_uuid is None:
            district_uuid, district_desc = create_district_description_for_ville_paris(lab, lang, vp_ns)
        th_districts.append([district_desc, district_uuid, lab])

    for lab in th_arrdt_labels:
        arrdt_uuid, arrdt_desc = arrdts.get(lab), None
        if arrdt_uuid is None:
            arrdt_uuid, arrdt_desc = create_arrondissement_description_for_ville_paris(lab, lang, vp_ns)
        th_arrdts.append([arrdt_desc, arrdt_uuid, lab])

    return [th_desc, th_id], th_districts, th_arrdts

def create_landmark_relations_descriptions_for_ville_paris_line(th_uuid, district_and_arrdt_uuids, vp_ns:Namespace):
    lr_descs = []
    for uuid in district_and_arrdt_uuids:
        lr_uuid = gr.generate_uuid()
        lr_provenance = {"uri":str(vp_ns[th_uuid])}
        lr_desc = di.create_landmark_relation_version_description(lr_uuid, "within", th_uuid, [uuid], lr_provenance)
        lr_descs.append(lr_desc)

    return lr_descs

def create_thoroughfare_description_for_ville_paris(th_label:str, th_id:str, th_geom:str, lang:str, vp_ns:Namespace):
    th_type = "thoroughfare"
    th_attrs = {"name":di.create_landmark_attribute_version_description(th_label, lang=lang)}
    if th_geom is not None:
        th_wkt_geom = gp.from_geojson_to_wkt(json.loads(th_geom))
        th_attrs["geometry"] = di.create_landmark_attribute_version_description(th_wkt_geom, datatype="wkt_literal")
    th_provenance = {"uri":str(vp_ns[th_id])}
    th_desc = di.create_landmark_version_description(th_id, th_label, th_type, lang, th_attrs, th_provenance)
    return th_desc

def create_district_description_for_ville_paris(district_label:str, lang:str, vp_ns:Namespace):
    district_uuid = gr.generate_uuid()
    district_type = "district"
    district_attrs = {"name":di.create_landmark_attribute_version_description(district_label, lang=lang)}
    district_provenance = {"uri":str(vp_ns)}
    district_desc = di.create_landmark_version_description(district_uuid, district_label, district_type, lang, district_attrs, district_provenance)
    return district_uuid, district_desc

def create_arrondissement_description_for_ville_paris(arrdt_label:str, lang:str, vp_ns:Namespace):
    arrdt_uuid = gr.generate_uuid()
    arrdt_type = "district"
    arrdt_label = re.sub("^0", "", arrdt_label.replace("01e", "01er")) + " arrondissement de Paris"
    arrdt_attrs = {"name":di.create_landmark_attribute_version_description(arrdt_label, lang=lang)}
    arrdt_provenance = {"uri":str(vp_ns)}
    arrdt_desc = di.create_landmark_version_description(arrdt_uuid, arrdt_label, arrdt_type, lang, arrdt_attrs, arrdt_provenance)
    return arrdt_uuid, arrdt_desc

def create_landmark_appearance_event_for_ville_paris(lm_label:str, lm_lang:str, provenance:dict, time_stamp:str):
    time_description = get_time_description_for_ville_paris(time_stamp)
    makes_effective = [di.create_landmark_attribute_version_description(lm_label, lang=lm_lang)]
    name_attr_cg = di.create_landmark_attribute_change_event_description("name", makes_effective=makes_effective)
    lm_cg = di.create_landmark_change_event_description("appearance")
    lm = di.create_landmark_event_description(1, "thoroughfare", lm_label, lm_lang, changes=[lm_cg, name_attr_cg])
    ev_desc = di.create_event_description(None, lm_lang, [lm], [], provenance, time_description)
    return ev_desc

def create_landmark_disappearance_event_for_ville_paris(lm_label:str, lm_lang:str, provenance:dict, time_stamp:str):
    time_description = get_time_description_for_ville_paris(time_stamp)
    outdates = [di.create_landmark_attribute_version_description(lm_label, lang=lm_lang)]
    name_attr_cg = di.create_landmark_attribute_change_event_description("name", outdates=outdates)
    lm_cg = di.create_landmark_change_event_description("disappearance")
    lm = di.create_landmark_event_description(1, "thoroughfare", lm_label, lm_lang, changes=[lm_cg, name_attr_cg])
    ev_desc = di.create_event_description(None, lm_lang, [lm], [], provenance, time_description)
    return ev_desc

def get_time_description_for_ville_paris(time_stamp:str):
    return {"stamp":time_stamp, "calendar":"gregorian", "precision":"day"}

##################################################### Geojson states ##########################################################

def create_state_description_for_geojson_states(geojson_file:str, landmark_type:str, identity_property:str, name_attribute:str, lang:str=None,
                                                time_description:dict={}, source_description:dict={}):
    """
    `identity_property` is the property used to identify the identity of landmark in the geojson file.
    `name_attribute` is the property used to identify the name of landmark in the geojson file.
    """
    feature_collection = fm.read_json_file(geojson_file)
    landmarks = get_merged_landmarks_from_geojson_states(feature_collection, identity_property)
    state_desc = create_landmarks_descriptions_for_geojson_states(landmarks, landmark_type, name_attribute, lang, time_description, source_description)

    return state_desc

def get_merged_landmarks_from_geojson_states(feature_collection:dict, identity_property:str):
    """
    `identity_property` is the property used to identify the identity of landmark in the geojson file.
    If idetity_property is `name`, all features with the same value for the property `name` will be merged.
    """

    landmarks_to_merge, landmarks = {}, []

    features = feature_collection.get("features")
    geojson_crs = feature_collection.get("crs")
    srs_iri = gp.get_srs_iri_from_geojson_feature_collection(geojson_crs)

    for feature in features:
        if isinstance(feature.get("properties"), dict):
            feature_name = feature.get("properties").get(identity_property)
        else:
            feature_name = None

        geometry, properties = feature.get("geometry"), feature.get("properties")
        if feature_name not in landmarks_to_merge.keys():
            landmarks_to_merge[feature_name] = {"properties": [], "geometry": []}

        landmarks_to_merge[feature_name]["properties"].append(properties) if properties is not None else None
        landmarks_to_merge[feature_name]["geometry"].append(geometry) if geometry is not None else None

    for lm in landmarks_to_merge.values():
        merged_geometry = gp.get_wkt_union_of_geojson_geometries(lm["geometry"], srs_iri)
        merged_properties = lm["properties"][0] if len(lm["properties"]) > 0 else {}
        landmark = {"type":"Feature", "properties": merged_properties, "geometry": merged_geometry}
        landmarks.append(landmark)
    
    return landmarks
 

def create_landmarks_descriptions_for_geojson_states(landmarks:list, landmark_type:str, name_attribute:str, lang:str=None,
                                                          time_description:dict=None, source_description:dict=None):
    """
    Create a state description for a list of landmarks
    """
    landmarks_desc = []

    for landmark in landmarks:
        lm_desc = create_state_description_for_geojson_landmark_state(landmark, landmark_type, name_attribute, lang)
        landmarks_desc.append(lm_desc)

    description = {"landmarks":landmarks_desc}

    if isinstance(time_description, dict):
        description["time"] = time_description
    if isinstance(source_description, dict):
        description["source"] = source_description

    return description

def create_state_description_for_geojson_landmark_state(landmark:dict, landmark_type:str, name_attribute:str, lang:str=None):
    lm_uuid = gr.generate_uuid()
    lm_label = landmark.get("properties").get(name_attribute)

    # Get the geometry and properties of the landmark
    geometry = landmark.get("geometry")

    # Create the attributes of the landmark description
    name_attr_desc = di.create_landmark_attribute_version_description(lm_label, lang=lang)
    geom_attr_desc = di.create_landmark_attribute_version_description(geometry, datatype="wkt_literal")
    attributes = {}
    if name_attr_desc is not None:
        attributes["name"] = name_attr_desc
    if geom_attr_desc is not None:
        attributes["geometry"] = geom_attr_desc

    # Create the landmark description
    lm_desc = di.create_landmark_version_description(lm_uuid, lm_label, landmark_type, lang, attributes, {})
    return lm_desc
    

def create_state_description_for_geojson_states_of_streetnumbers(geojson_file:str, identity_property:str, name_attribute:str,
                                                                lang:str=None, time_description:dict=None, source_description:dict=None):
    """
    `identity_property` is the property used to identify the identity of landmark in the geojson file.
    `name_attribute` is the property used to identify the name of landmark in the geojson file.
    """
    feature_collection = fm.read_json_file(geojson_file)
    features = feature_collection.get("features")
    geojson_crs = feature_collection.get("crs")
    srs_iri = gp.get_srs_iri_from_geojson_feature_collection(geojson_crs)
    
    lm_descs, lr_descs = [], []
    thoroughfares = {}

    for feature in features:
        sn_desc, [th_uuid, th_desc, th_label], lr_desc = create_state_description_for_geojson_streetnumber_state(feature, thoroughfares, srs_iri, name_attribute, lang)
        if sn_desc is not None:
            lm_descs.append(sn_desc)
            if th_desc is not None:
                lm_descs.append(th_desc)
                thoroughfares[th_label] = th_uuid
            lr_descs.append(lr_desc)

    descriptions = {"landmarks":lm_descs, "relations":lr_descs}
    if isinstance(time_description, dict):
        descriptions["time"] = time_description
    if isinstance(source_description, dict):
        descriptions["source"] = source_description

    return descriptions

def create_state_description_for_geojson_streetnumber_state(streetnumber:dict, thoroughfares:dict, srs_iri:str, name_attribute:str, lang:str=None):
    address_label = streetnumber.get("properties").get(name_attribute)
    if address_label is None:
        return None, [None, None, None], None
        
    sn_label, th_label = sp.split_french_address(address_label)
    geometries = [streetnumber["geometry"]]
    geometry_value = gp.get_wkt_union_of_geojson_geometries(geometries, srs_iri)

    name_attr_desc = di.create_landmark_attribute_version_description(sn_label)
    geom_attr_desc = di.create_landmark_attribute_version_description(geometry_value, datatype="wkt_literal")
    attributes = {}
    if name_attr_desc is not None:
        attributes["name"] = name_attr_desc
    if geom_attr_desc is not None:
        attributes["geometry"] = geom_attr_desc

    sn_uuid = gr.generate_uuid()
    sn_desc = di.create_landmark_version_description(sn_uuid, sn_label, "street_number", None, attributes)

    th_uuid, th_desc = thoroughfares.get(th_label), None
    if th_uuid is None:
        th_uuid = gr.generate_uuid()
        th_desc = di.create_landmark_version_description(th_uuid, th_label, "thoroughfare", lang, attributes)

    lr_desc = di.create_landmark_relation_version_description(gr.generate_uuid(), "belongs", sn_uuid, [th_uuid])

    return sn_desc, [th_uuid, th_desc, th_label], lr_desc
    
    
