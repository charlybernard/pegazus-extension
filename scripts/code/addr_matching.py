import json
import geojson
import str_processing as sp
import geom_processing as gp
from shapely.geometry import LineString

def get_streetnumbers(json_file, sn_attr:str=None, th_attr:str=None, addr_attr:str=None):
    """
    Returns the features from the two datasets.
    """

    data = json.load(open(json_file, "r", encoding="utf-8"))
    features = {}

    for feature in data.get("features"):
        th_label = feature.get("properties").get(th_attr)
        sn_label = feature.get("properties").get(sn_attr)
        addr_label = feature.get("properties").get(addr_attr)

        if addr_attr is not None:
            sn_label, th_label = sp.split_french_address(addr_label)
        if th_label is None or th_label == "":
            th_label = None
        else:
            th_label = sp.simplify_french_name_version(str(th_label), "thoroughfare")
        if sn_label is None or sn_label == "":
            sn_label = None
        else:
            sn_label = sp.simplify_french_name_version(str(sn_label), "number")
  
        if th_label is not None and sn_label is not None:
            label = th_label + "&" + sn_label
            if label in features:
                features[label].append(feature)
            else:
                features[label] = [feature]

    return features

def create_links_between_similar_features(features_1, features_2,
                                          features_1_id_attr:str=None, features_2_id_attr:str=None,
                                          features_1_epsg_code=None, feature_2_epsg_code=None, default_epsg_code=None, max_distance=None):
    """
    Create links between similar features from two datasets.
    """

    # Créer une liste pour stocker les liens
    links = []

    # Parcourir les features du premier dataset
    for key in features_1.keys():
        values1 = features_1[key]
        if key in features_2:
            values2 = features_2[key]
            for value1 in values1:
                for value2 in values2:
                        link = get_feature_link_between_two_features(value1, value2,
                                                                    features_1_id_attr, features_2_id_attr,
                                                                    features_1_epsg_code, feature_2_epsg_code, default_epsg_code, max_distance)
                        # Ajouter le lien entre les deux features (si il existe)
                        if link is not None:
                            links.append(link)

    return links

########################################################## Functions for feature matching ##########################################################

def get_linestring_link_between_two_features(g1:dict, g2:dict, g1_epsg:str, g2_epsg:str, default_epsg:str=None, max_distance:float=None):
    geom1 = gp.from_geojson_to_shape(g1)
    geom2 = gp.from_geojson_to_shape(g2)
    centroid_1 = geom1.centroid
    centroid_2 = geom2.centroid

    if g1_epsg != default_epsg and max_distance is not None:
        centroid_1 = gp.transform_geometry_crs(centroid_1, g1_epsg, default_epsg)
    if g2_epsg != default_epsg and max_distance is not None:
        centroid_2 = gp.transform_geometry_crs(centroid_2, g2_epsg, default_epsg)

    distance = centroid_1.distance(centroid_2)

    if max_distance is not None and distance <= max_distance :
        if g1_epsg != default_epsg and max_distance is None:
            centroid_1 = gp.transform_geometry_crs(centroid_1, g1_epsg, default_epsg)
        if g2_epsg != default_epsg and max_distance is None:
            centroid_2 = gp.transform_geometry_crs(centroid_2, g2_epsg, default_epsg)
        geom_line = LineString([centroid_1, centroid_2])
        geom_line_geojson = geojson.loads(geojson.dumps(geom_line))
        return geom_line_geojson
    
    else:
        return None
    
def get_feature_link_between_two_features(feature_1:dict, feature_2:dict,
                                          feature_1_id_attr:str, feature_2_id_attr:str,
                                          feature_1_epsg:str, feature_2_epsg:str, default_epsg:str=None,
                                          max_distance:float=None):
    """
    Get the link between two features
    """

    g1 = feature_1.get("geometry")
    g2 = feature_2.get("geometry")
    # Get the linestring link of the two geometries
    if max_distance is not None:
        max_distance = float(max_distance)
    geom_line_geojson = get_linestring_link_between_two_features(g1, g2, feature_1_epsg, feature_2_epsg, default_epsg, max_distance)
    if geom_line_geojson is None:
        return None
    
    # Create a new feature with the link
    link = {
        "geometry": geom_line_geojson,
        "properties": {
            "id_1": feature_1.get("properties").get(feature_1_id_attr),
            "id_2": feature_2.get("properties").get(feature_2_id_attr),
        }
    }

    return link

def create_links_between_similar_features_in_two_files(file_1_settings:dict, file_2_settings:dict,
                                                       out_links_file, default_epsg_code=None, max_distance=None):

    # Lecture des réglages pour le fichier 1
    f1_set_name = file_1_settings.get("file")
    f1_set_addr_attr = file_1_settings.get("addr_attr")
    f1_set_th_attr = file_1_settings.get("th_attr")
    f1_set_sn_attr = file_1_settings.get("sn_attr")
    f1_set_id_attr = file_1_settings.get("id_attr")
    f1_set_epsg_code = file_1_settings.get("epsg_code")

    # Lecture des réglages pour le fichier 2
    f2_set_name = file_2_settings.get("file")
    f2_set_addr_attr = file_2_settings.get("addr_attr")
    f2_set_th_attr = file_2_settings.get("th_attr")
    f2_set_sn_attr = file_2_settings.get("sn_attr")
    f2_set_id_attr = file_2_settings.get("id_attr")
    f2_set_epsg_code = file_2_settings.get("epsg_code")

    # Lister les features dans les deux fichiers
    if f1_set_addr_attr is not None:
        f1 = get_streetnumbers(f1_set_name, None, None, f1_set_addr_attr)
    else:
        f1 = get_streetnumbers(f1_set_name, f1_set_sn_attr, f1_set_th_attr)

    if f2_set_addr_attr is not None:
        f2 = get_streetnumbers(f2_set_name, None, None, f2_set_addr_attr)
    else:
        f2 = get_streetnumbers(f2_set_name, f2_set_sn_attr, f2_set_th_attr)

    # Créer les liens entre les features similaires des deux fichiers
    links = create_links_between_similar_features(f1, f2, f1_set_id_attr, f2_set_id_attr,
                                                  f1_set_epsg_code, f2_set_epsg_code, default_epsg_code, max_distance)
    
    # Exporter les liens vers un fichier geojson
    export_features_to_geojson(links, out_links_file, "links", default_epsg_code)

def export_features_to_geojson(features:list, out_file:str, name:str=None, epsg_code:str=None):
    """
    Export the features to a geojson file.
    """

    out_data = {
        "type": "FeatureCollection",
        "features": features,
    }

    if name is not None:
        out_data["name"] = name
    
    if epsg_code is not None:
        out_data["crs"] = { "type": "name", "properties": { "name": f"urn:ogc:def:crs:{epsg_code.replace(':','::')}" } }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    data_folder = "../../data/"
    tmp_folder = "../../tmp_files/"

    default_epsg_code = "EPSG:2154"
    max_distance = 10  # in meters (must be a float)
    
    cadastre_paris_1807_adresses_settings = {
        "file": data_folder + "cadastre_paris_1807_adresses.geojson",
        "th_attr": "NOM_SAISI",
        "sn_attr": "NUMERO TXT",
        "id_attr": "id",
        "epsg_code": "EPSG:2154"
    }

    atlas_vasserot_1810_adresses_settings = {
        "file": data_folder + "atlas_vasserot_1810_adresses.geojson",
        "th_attr": "nom_entier",
        "sn_attr": "num_voies",
        "id_attr": "id",
        "epsg_code": "EPSG:4326"
    }

    atlas_jacoubet_1836_adresses_settings = {
        "file": data_folder + "atlas_jacoubet_1836_adresses.geojson",
        "th_attr": "nom_entier",
        "sn_attr": "num_voies",
        "id_attr": "fid",
        "epsg_code": "EPSG:2154"
    }

    atlas_municipal_1888_adresses_settings = {
        "file": data_folder + "atlas_municipal_1888_adresses.geojson",
        "th_attr": "normalised",
        "sn_attr": "numbers_va",
        "id_attr": "fid",
        "epsg_code": "EPSG:2154"
    }

    out_file_1 = tmp_folder + "1_links_cadastre_paris_1807_adresses_atlas_vasserot_adresses_1810.geojson"
    out_file_2 = tmp_folder + "2_links_atlas_vasserot_1810_adresses_atlas_jacoubet_1836_adresses.geojson"

    out_file_3 = tmp_folder + "3_links_atlas_jacoubet_1836_adresses_atlas_municipal_1888_adresses.geojson"

    create_links_between_similar_features_in_two_files(cadastre_paris_1807_adresses_settings, atlas_vasserot_1810_adresses_settings, out_file_1, default_epsg_code, max_distance)
    create_links_between_similar_features_in_two_files(atlas_vasserot_1810_adresses_settings, atlas_jacoubet_1836_adresses_settings, out_file_2, default_epsg_code, max_distance)
    create_links_between_similar_features_in_two_files(atlas_jacoubet_1836_adresses_settings, atlas_municipal_1888_adresses_settings, out_file_3, default_epsg_code, max_distance)