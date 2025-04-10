import re
import json
import geojson
import pyproj
import shapely
from shapely import wkt
from shapely.geometry import shape
from shapely.ops import transform
from uuid import uuid4
from rdflib import URIRef, Literal, Namespace

def get_crs_dict():
    crs_dict = {
    "EPSG:4326": URIRef("http://www.opengis.net/def/crs/EPSG/0/4326"),
    "EPSG:2154": URIRef("http://www.opengis.net/def/crs/EPSG/0/2154"),
    "urn:ogc:def:crs:OGC:1.3:CRS84": URIRef("http://www.opengis.net/def/crs/EPSG/0/4326"),
    "urn:ogc:def:crs:EPSG::2154": URIRef("http://www.opengis.net/def/crs/EPSG/0/2154"),
    }

    return crs_dict

def get_srs_iri_from_geojson_feature_collection(geojson_crs:dict):
    crs_dict = get_crs_dict()
    try:
        crs_name = geojson_crs.get("properties").get("name")
        srs_iri = crs_dict.get(crs_name)
        return srs_iri
    except:
        return None

def from_geojson_to_shape(geojson_obj:dict):
    """
    Convert a geojson object to a shapely shape
    """

    # Convert the geojson object to a string
    geojson_str = json.dumps(geojson_obj)

    # Load the geojson string into a shapely shape
    geom = shape(geojson.loads(geojson_str))

    return geom

def from_geojson_to_wkt(geojson_obj:dict):
    geom = from_geojson_to_shape(geojson_obj)
    return geom.wkt

def merge_geojson_features_from_one_property(feature_collection:dict, property_name:str):
    """
    Merge all features of a geojson object which have the same property (name for instance)
    """

    new_geojson_features = []
    features_to_merge = {}
    
    features_key = "features"
    crs_key = "crs"

    for feat in feature_collection.get(features_key):
        # Get property value for the feature
        property_value = feat.get("properties").get(property_name)

        # If the value is blank or does not exist, generate an uuid
        if property_value in [None, ""]:
            empty_value = True
            property_value = uuid4().hex
            feature_template = {"type":"Feature", "properties":{}}
        else:
            empty_value = False
            feature_template = {"type":"Feature", "properties":{property_name:property_value}}

        features_to_merge_key = features_to_merge.get(property_value)

        if features_to_merge_key is None:
            features_to_merge[property_value] = [feature_template, [feat]]
        else:
            features_to_merge[property_value][1].append(feat)

    for elem in features_to_merge.values():
        template, feature = elem

        geom_collection_list = []
        for portion in feature:
            geom_collection_list.append(portion.get("geometry"))
    
        geom_collection = {"type": "GeometryCollection", "geometries": geom_collection_list}
        template["geometry"] = geom_collection
        new_geojson_features.append(template)

    new_geojson = {"type":"FeatureCollection", features_key:new_geojson_features}

    crs_value = feature_collection.get(crs_key)
    if crs_value is not None :
        new_geojson[crs_key] = crs_value

    return new_geojson

def get_union_of_geosparql_wktliterals(wkt_literal_list:list[Literal]):
    GEO = Namespace("http://www.opengis.net/ont/geosparql#")
    geom_list = []
    for wkt_literal in wkt_literal_list:
        wkt_geom_value, wkt_geom_srid = get_wkt_geom_from_geosparql_wktliteral(wkt_literal)
        geom = wkt.loads(wkt_geom_value)
        geom_list.append(geom)

    geom_union = shapely.union_all(geom_list)
    geom_union_wkt = shapely.to_wkt(geom_union)
    wkt_literal_union = Literal(f"{wkt_geom_srid.n3()} {geom_union_wkt}", datatype=GEO.wktLiteral)

    return wkt_literal_union

def get_union_of_geojson_geometries(geojson_geoms_list:list[dict]):
    geom_list = []
    for geojson_geom in geojson_geoms_list:
        geom = shape(geojson_geom)
        geom_list.append(geom)

    geom_union = shapely.union_all(geom_list)
    
    return geom_union

def get_wkt_union_of_geojson_geometries(geojson_geoms_list:list[dict], wkt_geom_srid:URIRef):
    # GEO = Namespace("http://www.opengis.net/ont/geosparql#")
    geom_union = get_union_of_geojson_geometries(geojson_geoms_list)
    geom_union_wkt = shapely.to_wkt(geom_union)
    if wkt_geom_srid is not None:
        wkt_union = f"{wkt_geom_srid.n3()} {geom_union_wkt}"
    else:
        wkt_union = geom_union_wkt

    return wkt_union

def get_wkt_geom_from_geosparql_wktliteral(wktliteral:str):
    """
    Extract the WKT and SRID URI of the geometry if indicated
    """

    wkt_srid_pattern = "<(.{0,})>"
    wkt_value_pattern = "<.{0,}> {1,}"
    wkt_geom_srid_match = re.match(wkt_srid_pattern, wktliteral)
    
    epsg_4326_uri = URIRef("http://www.opengis.net/def/crs/EPSG/0/4326")
    crs84_uri = URIRef("http://www.opengis.net/def/crs/OGC/1.3/CRS84")

    if wkt_geom_srid_match is not None:
        wkt_geom_srid = URIRef(wkt_geom_srid_match.group(1))
    else:
        wkt_geom_srid = epsg_4326_uri

    if wkt_geom_srid == crs84_uri:
        wkt_geom_srid = epsg_4326_uri
    wkt_geom_value = re.sub(wkt_value_pattern, "", wktliteral)

    return wkt_geom_value, wkt_geom_srid

def transform_geometry_crs(geom, crs_from, crs_to):
    """
    Obtain geometry defined in the `from_crs` coordinate system to the `to_crs` coordinate system.
    """

    project = get_crs_transformer(crs_from, crs_to)
    return transform(project, geom)

def get_crs_transformer(crs_from:str, crs_to:str):
    project = pyproj.Transformer.from_crs(crs_from, crs_to, always_xy=True).transform
    return project

def get_pyproj_crs_from_opengis_epsg_uri(opengis_epsg_uri:URIRef):
    """
    Extract EPSG code from `opengis_epsg_uri` to return a pyproj.CRS object
    """
    
    epsg_code = get_epsg_code_from_opengis_epsg_uri(opengis_epsg_uri)
    if epsg_code is not None:
        return pyproj.CRS.from_epsg(epsg_code)
    else :
        return None

def get_epsg_code_from_opengis_epsg_uri(opengis_epsg_uri:URIRef, with_epsg:bool=False):
    """
    Extract EPSG code from `opengis_epsg_uri` to return a pyproj.CRS object
    """
    pattern = "http://www.opengis.net/def/crs/EPSG/0/([0-9]{1,})"
    try :
        epsg_code = re.match(pattern, opengis_epsg_uri.strip()).group(1)
        if with_epsg:
            epsg_code = f'EPSG:{epsg_code}'
    except :
        epsg_code = None

    return epsg_code

def are_similar_geometries(geom_1, geom_2, geom_type:str, coef_min:float=0.8, max_dist=10) -> bool:
    """
    The function determines whether two geometries are similar:
    `coef_min` is in [0,1] and defines the minimum value for considering `geom_1` and `geom_2` to be similar
    `geom_type` defines the type of geometry to be taken into account (`point`, `linestring`, `polygon`)
    """

    if geom_type == "polygon":
        return are_similar_polygons(geom_1, geom_2, coef_min)
    elif geom_type == "point":
        return are_similar_points(geom_1, geom_2, max_dist)
    return None

    
def are_similar_points(geom_1, geom_2, max_dist):
    """
    We want to know if two points are similar. They are similar if the distance between them is less than `max_dist`.
    """

    dist = geom_1.distance(geom_2)

    if dist <= max_dist:
        return True
    else:
        return False


def are_similar_polygons(geom_1, geom_2, coef_min:float):
    """
    Technique for determining whether polygons are similar:
    * build a bounding bbox for each polygon
    * analyse the overlap between the union of the bboxes and the intersection.

    If the overlap rate is greater than `coef_min`, the polygons are similar
    """

    geom_intersection = geom_1.envelope.intersection(geom_2.envelope)
    geom_union = geom_1.envelope.union(geom_2.envelope)
    coef = geom_intersection.area/geom_union.area

    if coef >= coef_min:
        return True
    else:
        return False
    

def get_projected_geometry(geom, geom_srid_uri:URIRef, crs_uri:URIRef, transformers:dict[str, pyproj.Transformer]={}):
    """
    Obtain geometry defined in the `geom_srid_uri` coordinate system to the `crs_uri` coordinate system.
    The `transformers` dictionary is used to store transformers for each coordinate system.
    """

    # Getting the EPSG code from the OpenGIS URI
    crs_from = get_epsg_code_from_opengis_epsg_uri(geom_srid_uri, True)
    crs_to = get_epsg_code_from_opengis_epsg_uri(crs_uri, True)

    # transformers dictionary is used to store transformers for each coordinate system
    transformer = transformers.get(crs_to)

    if transformer is None and crs_from != crs_to:
        # If the transformer is not already in the dictionary, create it
        transformer = get_crs_transformer(crs_from, crs_to)

    # Converting geometry to the target coordinate system
    if crs_from != crs_to:
        geom = transform(transformer, geom)

    return geom

def get_processed_geometry(geom_wkt:str, geom_srid_uri:URIRef, geom_type:str, crs_uri:URIRef, buffer_radius:float, transformers:dict[str, pyproj.Transformer]={}):
    """
    Obtaining a geometry so that it can be compared with others:
    * its coordinates will be expressed in the reference frame linked to `crs_uri`.
    * if the geometry is a line or a point (area=0.0) and we want to have a polygon as geometry (`geom_type == ‘polygon’`), then we retrieve a buffer zone whose buffer is given by `buffer_radius`.
    """

    geom = wkt.loads(geom_wkt)
    geom = get_projected_geometry(geom, geom_srid_uri, crs_uri, transformers)
    
    # Add a `meter_buffer` if it's not a polygon
    if geom.area == 0.0 and geom_type == "polygon":
        geom = geom.buffer(buffer_radius)

    return geom

def get_useful_transformers_for_from_crs(from_crs:str, to_crs_list:list[str]):
    """
    Get a list of transformers to be used for converting geometries from `from_crs` to each of the `to_crs_list` coordinate systems.
    """

    transformers = {}
    for to_crs in to_crs_list:
        transformer = get_crs_transformer(from_crs, to_crs)
        transformers[to_crs] = transformer

    return transformers