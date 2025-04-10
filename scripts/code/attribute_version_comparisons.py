from namespaces import NameSpaces
import geom_processing as gp
import str_processing as sp
import graphdb as gd
import graphrdf as gr
from rdflib import URIRef, Graph, Literal

np = NameSpaces()

import time

# def compare_attribute_versions(graphdb_url:URIRef, repository_name:str, comp_named_graph_name:str, comp_tmp_file:str, comparison_settings:dict={}):
#     """
#     Compare versions related to the same attribute. The way of versions are compared are set by `comparison_settings`.
#     """

#     t1 = time.time()
#     # Get versions which have to be compared
#     results = get_attribute_versions_to_compare(graphdb_url, repository_name)
#     elements = results.get("results").get("bindings")
#     t2 = time.time()
#     print(f"Time to get attribute versions to compare: {t2-t1} seconds")
#     # Initialisation of a RDFLib graph (it will be exported as a TTL file at the end of the process)
#     g = Graph() 

#     # Dictionary which defines properties to used according comparison outputs.
#     val_comp_dict = {True:np.ADDR["sameVersionValueAs"], False:np.ADDR["differentVersionValueFrom"], None:None}
    
#     similarity_coef = comparison_settings.get("geom_similarity_coef")
#     buffer_radius = comparison_settings.get("geom_buffer_radius")
#     crs_uri = comparison_settings.get("geom_crs_uri")
#     epsg_code = gp.get_epsg_code_from_opengis_epsg_uri(crs_uri, True)
#     transformers = gp.get_useful_transformers_for_from_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])

#     for elem in elements:
#         # Get URIs (attribute and attribute versions)
#         attr_type = gr.convert_result_elem_to_rdflib_elem(elem.get('attrType'))
#         lm_type = gr.convert_result_elem_to_rdflib_elem(elem.get('ltype'))
#         attr_vers_1 = gr.convert_result_elem_to_rdflib_elem(elem.get('attrVers1'))
#         attr_vers_2 = gr.convert_result_elem_to_rdflib_elem(elem.get('attrVers2'))
#         vers_val_1 = gr.convert_result_elem_to_rdflib_elem(elem.get('versVal1'))
#         vers_val_2 = gr.convert_result_elem_to_rdflib_elem(elem.get('versVal2'))

#         # If the attribute describes a name, comparison is done thanks to `are_similar_name_versions()`.
#         # This comparison depends on the type of landmark (`lm_type`)
#         if attr_type == np.ATYPE["Name"]:
#             is_same_value = are_similar_name_versions(lm_type, vers_val_1, vers_val_2)
  
#         # If the attribute describes a geometry, comparison is done thanks to `are_similar_geom_versions()`.
#         # This comparison depends on the type of landmark (`lm_type`)
#         elif attr_type == np.ATYPE["Geometry"]:
#             is_same_value = are_similar_geom_versions(lm_type, vers_val_1, vers_val_2, similarity_coef, buffer_radius, crs_uri, transformers)
        
#         elif attr_type == np.ATYPE["InseeCode"]:
#             # For INSEE code, comparison is done with the help of `are_similar_name_versions()`
#             # but the result is always True
#             is_same_value = are_similar_name_versions(lm_type, vers_val_1, vers_val_2)
#         else:
#             is_same_value = None
        
#         # Get the property to be used to compare versions according the result of comparison
#         # Add the triple in the graph
#         comp_pred = val_comp_dict.get(is_same_value)
#         if comp_pred is not None:
#             g.add((attr_vers_1, comp_pred, attr_vers_2))
    
#     t3 = time.time()
#     print(f"Time to compare attribute versions: {t3-t2} seconds")
#     # Export the graph to of TTL file
#     g.serialize(destination=comp_tmp_file)
#     t4 = time.time()
#     print(f"Time to export graph to TTL file: {t4-t3} seconds")
    
#     # Import the TTL file in GraphDB
#     gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, comp_tmp_file, named_graph_name=comp_named_graph_name)
#     t5 = time.time()
#     print(f"Time to import TTL file in GraphDB: {t5-t4} seconds")
        
# def get_geom_type_according_landmark_type(rel_lm_type:URIRef):
#     """
#     According landmark type, return a shape of geometry.
#     """
    
#     if rel_lm_type in [np.LTYPE["HouseNumber"], np.LTYPE["StreetNumber"], np.LTYPE["DistrictNumber"]]:
#         return "point"
#     elif rel_lm_type in [np.LTYPE["Thoroughfare"], np.LTYPE["District"], np.LTYPE["Municipality"]]:
#         return "polygon"
#     else:
#         return "polygon"

# def get_name_type_according_landmark_type(rel_lm_type:URIRef):
#     """
#     According landmark type, return a value (housenumber, thoroughfare, area)
#     """
#     if rel_lm_type in [np.LTYPE["HouseNumber"], np.LTYPE["StreetNumber"], np.LTYPE["DistrictNumber"]]:
#         return "housenumber"
#     elif rel_lm_type in [np.LTYPE["Thoroughfare"]]:
#         return "thoroughfare"
#     elif rel_lm_type in [np.LTYPE["District"], np.LTYPE["Municipality"]]:
#         return "area"
#     else:
#         return ""

# def are_similar_geom_versions(lm_type, vers_val_1, vers_val_2, similarity_coef, buffer_radius, crs_uri, transformers) -> bool:
#     """
#     Returns True if `vers_val_1` is similar to `vers_val_2`, False else.
#     Similarity depends on type of landmark (`lm_type`) and coefficient of similarity (`similarity_coef`).
#     To comparable geometries, they must have the same shape (point, linestring, polygon) so buffer radius (`buffer_radius`) is used for linestring to be converted as polygon if needed.
#     Besides, for geometries to have same coordinated reference system (`crs_uri`).
#     """

#     # Get the suitable shape type of geometries (point, linestring, polygon) to compare them according landmark type
#     geom_type = get_geom_type_according_landmark_type(lm_type)

#     # Extract wkt literal and cri uri from vers_val Literal and modify wkt literal if needed (add buffer and change CRS)
#     geom_wkt_1, geom_srid_uri_1 = gp.get_wkt_geom_from_geosparql_wktliteral(vers_val_1.strip())
#     geom_1 = gp.get_processed_geometry(geom_wkt_1, geom_srid_uri_1, geom_type, crs_uri, buffer_radius, transformers)
#     geom_wkt_2, geom_srid_uri_2 = gp.get_wkt_geom_from_geosparql_wktliteral(vers_val_2.strip())
#     geom_2 = gp.get_processed_geometry(geom_wkt_2, geom_srid_uri_2, geom_type, crs_uri, buffer_radius, transformers)

#     return gp.are_similar_geometries(geom_1, geom_2, geom_type, similarity_coef, max_dist=buffer_radius)

# def are_similar_name_versions(lm_type, vers_val_1, vers_val_2):
#     name_type = get_name_type_according_landmark_type(lm_type)
#     _, simplified_name_1 = sp.normalize_and_simplify_name_version(vers_val_1.strip(), name_type, name_lang=vers_val_1.language)
#     _, simplified_name_2 = sp.normalize_and_simplify_name_version(vers_val_2.strip(), name_type, name_lang=vers_val_2.language)

#     if simplified_name_1 == simplified_name_2:
#         return True
#     else:
#         return False

def compare_attribute_versions(graphdb_url:URIRef, repository_name:str, comp_named_graph_name:str, comp_tmp_file:str, comparison_settings:dict={}):
    # Get versions which have to be compared
    results = get_attribute_versions_to_compare(graphdb_url, repository_name)
    bindings = results.get("results").get("bindings")

    # Creation of a RDFLib graph (it will be exported as a TTL file at the end of the process)
    g = get_processed_attribute_version_values(bindings, comparison_settings)
    g.serialize(destination=comp_tmp_file)
    
    # Import the TTL file in GraphDB
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, comp_tmp_file, named_graph_name=comp_named_graph_name)

def get_attribute_versions_to_compare(graphdb_url:URIRef, repository_name:str):
    """
    Get the attribute versions which have to be compared.
    """

    query = np.query_prefixes  + f"""
        SELECT DISTINCT ?ltype ?attrType ?attrVers1 ?attrVers2 ?versVal1 ?versVal2 WHERE {{
            ?rootLm a addr:Landmark ; addr:isRootOf ?lm1, ?lm2.
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
    geom_transformers = gp.get_useful_transformers_for_from_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
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

    # Get the suitable shape type of geometries (point, linestring, polygon) to compare them according landmark type
    geom_type = get_geom_type_according_landmark_type(lm_type)

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
        processed_value = gp.get_processed_geometry(geom_wkt, geom_srid_uri, geom_type, crs_uri, buffer_radius, transformers)

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
    