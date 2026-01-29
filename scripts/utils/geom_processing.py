import random
import math
import re
import json
import geojson
import pyproj
import shapely
from shapely import wkt
from shapely.geometry import shape, Point
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

def transform_geometry(geom, transformer):
    """
    Obtain geometry defined in the `from_crs` coordinate system to the `to_crs` coordinate system.
    """

    return transform(transformer.transform, geom)

def transform_geometry_crs(geom, crs_from, crs_to):
    """
    Obtain geometry defined in the `from_crs` coordinate system to the `to_crs` coordinate system.
    """

    transformer = get_crs_transformer(crs_from, crs_to)
    return transform(transformer.transform, geom)

def get_crs_transformer(crs_from:str, crs_to:str):
    transformer = pyproj.Transformer.from_crs(crs_from, crs_to, always_xy=True)
    return transformer

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
    

def get_projected_geometry(geom, crs_from_uri:URIRef, crs_to_uri:URIRef, transformers:dict[str, pyproj.Transformer]={}):
    """
    Obtain geometry defined in the `geom_srid_uri` coordinate system to the `crs_uri` coordinate system.
    The `transformers` dictionary is used to store transformers for each coordinate system.
    """

    # Getting the EPSG code from the OpenGIS URI
    crs_from = get_epsg_code_from_opengis_epsg_uri(crs_from_uri, True)
    crs_to = get_epsg_code_from_opengis_epsg_uri(crs_to_uri, True)

    # transformers dictionary is used to store transformers for each coordinate system
    transformer = transformers.get(crs_from)

    if transformer is None and crs_from != crs_to:
        # If the transformer is not already in the dictionary, create it
        transformer = get_crs_transformer(crs_from, crs_to)

    # Converting geometry to the target coordinate system
    if crs_from != crs_to:
        geom = transform(transformer.transform, geom)

    return geom

def get_processed_geometry(geom_wkt:str, geom_type:str, geom_srid_uri:URIRef, crs_uri:URIRef, buffer_radius:float, transformers:dict[str, pyproj.Transformer]={}):
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

def get_useful_transformers_for_to_crs(to_crs:str, from_crs_list:list[str]):
    """
    Get a list of transformers to be used for converting geometries from each of the `from_crs_list` coordinate systems to `to_crs`.
    """

    transformers = {}
    for from_crs in from_crs_list:
        transformer = get_crs_transformer(from_crs, to_crs)
        transformers[from_crs] = transformer

    return transformers

def wkt_to_shapely(wkt_literal, crs_to_uri:URIRef, transformers:dict[str, pyproj.Transformer]={}):
    wkt_geom_value, wkt_geom_srid = get_wkt_geom_from_geosparql_wktliteral(wkt_literal)
    crs_from_uri = URIRef(wkt_geom_srid)
    geom = wkt.loads(wkt_geom_value)
    geom = get_projected_geometry(geom, crs_from_uri, crs_to_uri, transformers)
    return geom

def get_centroid_of_union_of_geosparql_wktliterals(wkt_literal_list:list[Literal], crs_to_uri:URIRef, transformers:dict[str, pyproj.Transformer]={}):
    geom_list = []
    for wkt_literal in wkt_literal_list:
        geom = wkt_to_shapely(wkt_literal, crs_to_uri, transformers)
        geom_list.append(geom)

    geom_union = shapely.union_all(geom_list)

    return geom_union.centroid


def get_point_around_wkt_literal_geoms(
    wkt_literal_list: list[Literal],
    crs_to_uri: URIRef,
    transformers: dict[str, pyproj.Transformer] = {},
    max_distance: float = 5
):
    """
    Generate a point geometry spatially similar to a set of geometries
    described as WKT literals.

    The input WKT literals are converted into Shapely geometries using the
    target CRS. A representative point is then generated so that it lies
    within a spatial tolerance defined by ``max_distance`` around the
    original geometries.

    This function is mainly used to simulate spatial uncertainty or
    fragmentary geometric knowledge when generating synthetic factoids.

    Parameters
    ----------
    wkt_literal_list : list[Literal]
        A list of RDF literals encoded as GeoSPARQL WKT literals describing
        point geometries.
    crs_to_uri : URIRef
        URI of the target coordinate reference system (CRS) used to interpret
        the WKT literals.
    transformers : dict[str, pyproj.Transformer], optional
        A dictionary of CRS transformers used to convert geometries into the
        target CRS. Default is an empty dictionary.
    max_distance : float, optional
        Maximum distance (in CRS units) used to define spatial similarity
        between geometries. Default is 5.

    Returns
    -------
    Literal
        A GeoSPARQL WKT literal representing a point geometry spatially
        similar to the input geometries.
    """

    # GeoSPARQL namespace
    GEO = Namespace("http://www.opengis.net/ont/geosparql#")

    # Convert WKT literals to Shapely point geometries
    points = [
        wkt_to_shapely(wkt_literal, crs_to_uri, transformers)
        for wkt_literal in wkt_literal_list
    ]

    # Generate a representative point within the spatial tolerance
    new_point = generate_similar_point(points, max_distance)

    # Convert the generated point back to a WKT literal
    new_point_wkt = shapely.to_wkt(new_point)
    wkt_out_geom = Literal(
        f"{crs_to_uri.n3()} {new_point_wkt}",
        datatype=GEO.wktLiteral
    )

    return wkt_out_geom


#####################################################################

def generate_similar_point(points, max_distance=5):
    """
    Generate a point that is spatially similar to a set of input points.

    If only one point is provided, a new point is generated in the vicinity
    of the original geometry. If multiple points are provided, the function
    computes the intersection of buffers around each point and returns a
    representative point located within this intersection.

    Parameters
    ----------
    points : list
        A list of point geometries (e.g. coordinate tuples or Point-compatible
        geometries).
    max_distance : float, optional
        Radius (in the same unit as the input coordinates) used to define
        spatial similarity. Default is 5.

    Returns
    -------
    Point
        A point geometry that is spatially similar to the input points.
        If no common intersection exists, the first input point is returned.
    """

    # If only one point is provided, generate a nearby point
    if len(points) == 1:
        return get_new_point_near_geom(points[0], max_distance=max_distance)

    # ------------------------------------------------------------------
    # Step 1: generate buffer zones around each point
    # ------------------------------------------------------------------
    buffers = [Point(p).buffer(max_distance) for p in points]

    # ------------------------------------------------------------------
    # Step 2: compute the intersection of all buffers
    # ------------------------------------------------------------------
    intersection = buffers[0]
    for buf in buffers[1:]:
        intersection = intersection.intersection(buf)

        # If there is no common intersection, fall back to the first point
        if intersection.is_empty:
            return points[0]

    # ------------------------------------------------------------------
    # Step 3: generate a representative point inside the intersection
    # ------------------------------------------------------------------
    # The centroid is used as a simple and robust representative geometry
    return intersection.centroid


def get_new_point_near_geom(geom, max_distance=5):
    """
    Generates a random point within a `max_distance` radius of the `geom` geometry.
    The geometry must be in projected coordinates (e.g. EPSG:2154, in metres).
    Parameters
    ----------
    geom : shapely.geometry
        The reference geometry around which to generate a new point.
    max_distance : float
        The maximum distance (in the same unit as the geometry's coordinates)
        from the reference geometry to the new point.
    Returns
    -------
    shapely.geometry.Point
        A new point geometry located within `max_distance` of the input geometry.
    """

    centroid = geom.centroid

    # Random angle between 0 and 2 * pi (in radians)
    angle = random.uniform(0, 2 * math.pi)

    # Random distance between 0 and max_distance
    distance = random.uniform(0, max_distance)

    # Coordinates of the new point
    dx = distance * math.cos(angle)
    dy = distance * math.sin(angle)

    return Point(centroid.x + dx, centroid.y + dy)