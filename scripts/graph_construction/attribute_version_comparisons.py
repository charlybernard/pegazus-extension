from rdflib import URIRef, Graph, Literal
from scripts.graph_construction.namespaces import NameSpaces
from scripts.utils import geom_processing as gp
from scripts.utils import str_processing as sp
from scripts.graph_construction import graphdb as gd
from scripts.graph_construction import graphrdf as gr

np = NameSpaces()

def compare_attribute_versions(graphdb_url:URIRef, repository_name:str, comp_named_graph_uri:URIRef, comp_tmp_file:str, comparison_settings:dict={}):
    # Get versions which have to be compared
    results = get_attribute_versions_to_compare(graphdb_url, repository_name)
    bindings = results.get("results").get("bindings")

    # Creation of a RDFLib graph (it will be exported as a TTL file at the end of the process)
    g = get_processed_attribute_version_values(bindings, comparison_settings)
    gr.add_namespaces_to_graph(g, np.namespaces_with_prefixes)
    g.serialize(destination=comp_tmp_file)
    
    # Import the TTL file in GraphDB
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, comp_tmp_file, named_graph_uri=comp_named_graph_uri)

def get_attribute_versions_to_compare(graphdb_url:URIRef, repository_name:str):
    """
    Get the attribute versions which have to be compared.
    """

    query = np.query_prefixes  + f"""
        SELECT DISTINCT ?ltype ?attrType ?attrVers1 ?attrVers2 ?versVal1 ?versVal2 WHERE {{
            ?rootLm a addr:Landmark ; addr:hasTrace ?lm1, ?lm2.
            ?lm1 addr:hasAttribute [addr:isAttributeType ?attrType ; addr:hasAttributeVersion ?attrVers1] ; addr:isLandmarkType ?ltype .
            ?lm2 addr:hasAttribute [addr:isAttributeType ?attrType ; addr:hasAttributeVersion ?attrVers2] .
            ?attrVers1 addr:versionValue ?versVal1 .
            ?attrVers2 addr:versionValue ?versVal2 .
            FILTER(!sameTerm(?lm1, ?lm2))
            MINUS {{
                ?attrVers1 ?p ?attrVers2 .
                FILTER(?p IN (addr:sameVersionValueAs, addr:differentVersionValueFrom))
            }}
        }}
    """

    results = gd.select_query_to_json(query, graphdb_url, repository_name)

    return results

def get_processed_attribute_version_values(bindings:dict,  comparison_settings:dict={}):
    """
    Get the processed attribute version values (geometry or name) according the type of attribute version.
    """

    g = Graph()
    
    # Dictionary which defines properties to used according comparison outputs.
    val_comp_dict = {True:np.ADDR["sameVersionValueAs"], False:np.ADDR["differentVersionValueFrom"], None:None}

    # processed_values = {"http://rdf.geohistoricaldata.org/id/address/factoids/AV_ff60c9d04eb342499f45387e34f9d992": "rueausterlitz"}
    processed_values = {}

    # Dictionary which defines properties to used according comparison outputs.
    crs_uri = comparison_settings.get("geom_crs_uri")
    epsg_code = gp.get_epsg_code_from_opengis_epsg_uri(crs_uri, True)
    geom_transformers = gp.get_useful_transformers_for_to_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
    comparison_settings["geom_transformers"] = geom_transformers

    for binding in bindings:
        is_same_value, attr_vers_1, attr_vers_2, processed_vers_val_1, processed_vers_val_2 = are_two_attribute_versions_similar(binding, processed_values, comparison_settings)
        processed_values[attr_vers_1] = processed_vers_val_1
        processed_values[attr_vers_2] = processed_vers_val_2

        # Get the property to be used to compare versions according the result of comparison
        # Add the triple in the graph
        comp_pred = val_comp_dict.get(is_same_value)
        if comp_pred is not None:
            g.add((attr_vers_1, comp_pred, attr_vers_2))
    
    return g

def are_two_attribute_versions_similar(binding:dict, processed_values:dict, comparison_settings:dict={}):
    # Get URIs (attribute and attribute versions)
    lm_type = gr.convert_result_elem_to_rdflib_elem(binding.get('ltype'))
    attr_type = gr.convert_result_elem_to_rdflib_elem(binding.get('attrType'))
    attr_vers_1 = gr.convert_result_elem_to_rdflib_elem(binding.get('attrVers1'))
    attr_vers_2 = gr.convert_result_elem_to_rdflib_elem(binding.get('attrVers2'))
    vers_val_1 = gr.convert_result_elem_to_rdflib_elem(binding.get('versVal1'))
    vers_val_2 = gr.convert_result_elem_to_rdflib_elem(binding.get('versVal2'))

    processed_vers_val_1 = processed_values.get(attr_vers_1)
    processed_vers_val_2 = processed_values.get(attr_vers_2)

    if processed_vers_val_1 is None:
        processed_vers_val_1 = get_processed_attribute_version_value(vers_val_1, attr_type, lm_type, comparison_settings)
    if processed_vers_val_2 is None:
        processed_vers_val_2 = get_processed_attribute_version_value(vers_val_2, attr_type, lm_type, comparison_settings)

    is_same_value = are_similar_versions(processed_vers_val_1, processed_vers_val_2, attr_type, lm_type, comparison_settings)

    return is_same_value, attr_vers_1, attr_vers_2, processed_vers_val_1, processed_vers_val_2

def are_similar_versions(vers_val_1, vers_val_2, attr_type:URIRef, lm_type:URIRef, comparison_settings:dict={}):
    """
    Returns True if `vers_val_1` is similar to `vers_val_2`, False else.
    Similarity depends on type of landmark (`lm_type`) and coefficient of similarity (`similarity_coef`).
    To comparable geometries, they must have the same shape (point, linestring, polygon) so buffer radius (`buffer_radius`) is used for linestring to be converted as polygon if needed.
    Besides, for geometries to have same coordinated reference system (`crs_uri`).
    """

    is_same_value = None

    if attr_type == np.ATYPE["Name"]:
        is_same_value = are_similar_name_versions(vers_val_1, vers_val_2)

    elif attr_type == np.ATYPE["Geometry"]:
        similarity_coef = comparison_settings.get("geom_similarity_coef")
        max_distance_for_points = comparison_settings.get("geom_buffer_radius")
        is_same_value = are_similar_geom_versions(lm_type, vers_val_1, vers_val_2, similarity_coef, max_distance_for_points)
    
    elif attr_type == np.ATYPE["InseeCode"]:
        is_same_value = are_similar_name_versions(vers_val_1, vers_val_2)

    return is_same_value

def get_processed_attribute_version_value(vers_val:Literal, attr_type:URIRef, lm_type:URIRef, comparison_settings:dict={}):
    """
    Get the processed attribute version value (geometry or name) according the type of attribute version.
    """

    # Get settings for geometry processing
    crs_uri = comparison_settings.get("geom_crs_uri")
    buffer_radius = comparison_settings.get("geom_buffer_radius")
    transformers = comparison_settings.get("geom_transformers")
    
    if attr_type == np.ATYPE["Name"]:
        name_type = get_name_type_according_landmark_type(lm_type)
        _, processed_value = sp.normalize_and_simplify_name_version(vers_val.strip(), name_type, name_lang=vers_val.language)

    elif attr_type == np.ATYPE["Geometry"]:
        # Get the suitable shape type of geometries (point, linestring, polygon) to compare them according landmark type
        geom_type = get_geom_type_according_landmark_type(lm_type)
        # Extract wkt literal and cri uri from vers_val Literal and modify wkt literal if needed (add buffer and change CRS)
        geom_wkt, geom_srid_uri = gp.get_wkt_geom_from_geosparql_wktliteral(vers_val.strip())
        # Get the transformer to convert geometry from its original CRS to the one used in the comparison
        # If the geometry is already in the right CRS, no transformation is needed
        processed_value = gp.get_processed_geometry(geom_wkt, geom_type, geom_srid_uri, crs_uri, buffer_radius, transformers)

    elif attr_type == np.ATYPE["InseeCode"]:
        processed_value = vers_val.strip()

    return processed_value


def get_name_type_according_landmark_type(rel_lm_type:URIRef):
    """
    According landmark type, return a value (housenumber, thoroughfare, area)
    """
    if rel_lm_type in [np.LTYPE["HouseNumber"], np.LTYPE["StreetNumber"], np.LTYPE["DistrictNumber"]]:
        return "housenumber"
    elif rel_lm_type in [np.LTYPE["Thoroughfare"]]:
        return "thoroughfare"
    elif rel_lm_type in [np.LTYPE["District"], np.LTYPE["Municipality"]]:
        return "area"
    else:
        return ""

def get_geom_type_according_landmark_type(rel_lm_type:URIRef):
    """
    According landmark type, return a shape of geometry.
    """
    
    if rel_lm_type in [np.LTYPE["HouseNumber"], np.LTYPE["StreetNumber"], np.LTYPE["DistrictNumber"]]:
        return "point"
    elif rel_lm_type in [np.LTYPE["Thoroughfare"], np.LTYPE["District"], np.LTYPE["Municipality"]]:
        return "polygon"
    else:
        return "polygon"

def are_similar_name_versions(vers_val_1, vers_val_2):
    if vers_val_1 == vers_val_2:
        return True
    else:
        return False

def are_similar_geom_versions(lm_type, vers_val_1, vers_val_2, similarity_coef, max_distance_for_points) -> bool:
    """
    Returns True if `vers_val_1` is similar to `vers_val_2`, False else.
    Similarity depends on type of landmark (`lm_type`) and coefficient of similarity (`similarity_coef`).
    To comparable geometries, they must have the same shape (point, linestring, polygon) so buffer radius (`buffer_radius`) is used for linestring to be converted as polygon if needed.
    Besides, for geometries to have same coordinated reference system (`crs_uri`).
    """

    # Get the suitable shape type of geometries (point, linestring, polygon) to compare them according landmark type
    geom_type = get_geom_type_according_landmark_type(lm_type)

    return gp.are_similar_geometries(vers_val_1, vers_val_2, geom_type, similarity_coef, max_dist=max_distance_for_points)
    
# if __name__ == "__main__":
#     binding = {
#         "ltype": {"type": "uri", "value": "http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/Thoroughfare"},
#         "attrType": {"type": "uri", "value": "http://rdf.geohistoricaldata.org/id/codes/address/attributeType/Geometry"},
#         "attrVers1": {"type": "uri", "value": "http://rdf.geohistoricaldata.org/id/address/factoids/AV_ff60c9d04eb342499f45387e34f9d992"},
#         "attrVers2": {"type": "uri", "value": "http://rdf.geohistoricaldata.org/id/address/factoids/AV_4a6c3b8e7f2b4a0b8d5a0c5c7a4f8c3"},
#         "versVal1": {"type": "literal", "value": "<http://www.opengis.net/def/crs/EPSG/0/2154> LINESTRING (654162.457422 6861451.622655, 654213.2102 6861541.193389)", "datatype": "http://www.opengis.net/ont/geosparql#wktLiteral"},
#         "versVal2": {"type": "literal", "value": "POLYGON ((2.375453 48.851553, 2.375414 48.851563, 2.375481 48.851649, 2.375492 48.85168, 2.375509 48.851747, 2.375512 48.851748, 2.37552 48.851758, 2.375553 48.851746, 2.375598 48.851798, 2.375882 48.852127, 2.375884 48.85213, 2.375893 48.852127, 2.376004 48.852256, 2.376015 48.852271, 2.376043 48.852289, 2.37607 48.852281, 2.376037 48.852243, 2.375997 48.852197, 2.376021 48.852187, 2.376018 48.852184, 2.37601 48.852187, 2.375986 48.852159, 2.375991 48.852151, 2.375964 48.852121, 2.37595 48.852106, 2.375672 48.851792, 2.375661 48.851795, 2.375558 48.85167, 2.375569 48.851666, 2.375567 48.851661, 2.375569 48.851656, 2.375572 48.851653, 2.375577 48.85165, 2.375561 48.85163, 2.375533 48.851637, 2.375521 48.85164, 2.375453 48.851553))", "datatype": "http://www.opengis.net/ont/geosparql#wktLiteral"}
#         }
#     comparison_settings = {
#         "geom_crs_uri": "http://www.opengis.net/def/crs/EPSG/0/2154",
#         "geom_similarity_coef": 0.85,
#         "geom_buffer_radius": 10
#     }
#     crs_uri = comparison_settings.get("geom_crs_uri")
#     epsg_code = gp.get_epsg_code_from_opengis_epsg_uri(crs_uri, True)
#     geom_transformers = gp.get_useful_transformers_for_to_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
#     comparison_settings["geom_transformers"] = geom_transformers

#     are_similar = are_two_attribute_versions_similar(binding, {}, comparison_settings)
#     print(are_similar[0])
#     print(are_similar[4].wkt)