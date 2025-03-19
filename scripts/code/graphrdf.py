from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import XSD
from namespaces import NameSpaces
from uuid import uuid4
import re

np = NameSpaces()

def get_literal_without_option(value:str):
    return Literal(value)

def get_literal_with_lang(value:str, lang:str):
    return Literal(value, lang=lang)

def get_literal_with_datatype(value:str, datatype:URIRef):
    return Literal(value, datatype=datatype)

def get_geometry_wkt_literal(geom_wkt:str):
    return get_literal_with_datatype(geom_wkt, np.GEO.wktLiteral)

def get_name_literal(label:str, lang:str=None):
    return get_literal_with_lang(label, lang)

def get_insee_literal(insee_num:str):
    return get_literal_without_option(insee_num)

def convert_result_elem_to_rdflib_elem(result_elem:dict):
    """
    From a dictionary describing an element of a query result, convert it into an element of a graph triplet (URIRef, Literal, Bnode)
    """
    
    if result_elem is None:
        return None
    
    res_type = result_elem.get("type")
    res_value = result_elem.get("value")
    
    if res_type == "uri":
        return URIRef(res_value)
    elif res_type == "literal":
        res_lang = result_elem.get("xml:lang")
        res_datatype = result_elem.get("datatype")
        return Literal(res_value, lang=res_lang, datatype=res_datatype)
    elif res_type == "bnode":
        return BNode(res_value)
    else:
        return None
    
def generate_uri(namespace:Namespace=None, prefix:str=None):
    if prefix:
        return namespace[f"{prefix}_{uuid4().hex}"]
    else:
        return namespace[uuid4().hex]
    
def generate_uuid():
    return uuid4().hex

def add_namespaces_to_graph(g:Graph, namespaces:dict):
    for prefix, namespace in namespaces.items():
        g.bind(prefix, namespace)

def is_valid_uri(uri_str:str):
    regex = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return uri_str is not None and regex.search(uri_str)

def get_valid_uri(uri_str:str):
    if is_valid_uri(uri_str):
        return URIRef(uri_str)
    else:
        return None

def get_boolean_value(boolean:Literal):
    """
    Get python bool from RDFLib bool
    It returns None is `boolean` is not a boolean
    """

    boolean_val = boolean.strip()
    if boolean.datatype != XSD.boolean:
        return None
    
    if boolean_val == "false":
        return False
    elif boolean_val == "true":
        return True
    
    return None