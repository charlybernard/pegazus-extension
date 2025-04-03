from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import XSD
from namespaces import NameSpaces
from uuid import uuid4
import re

np = NameSpaces()

def get_literal_without_option(value:str):
    """
    Create a Literal object without any language or datatype option.

    Parameters:
    - value (str): The string value to be turned into a Literal.

    Returns:
    - Literal: A Literal object containing the provided value.

    Example usage:
    ```python
    literal = get_literal_without_option("Hello World")
    ```
    """
    if value is None:
        return None
    return Literal(value)

def get_literal_with_lang(value:str, lang:str):
    """
    Create a Literal object with a specified language.

    Parameters:
    - value (str): The string value to be turned into a Literal.
    - lang (str): The language tag to associate with the Literal.

    Returns:
    - Literal: A Literal object containing the provided value and language.

    Example usage:
    ```python
    literal = get_literal_with_lang("Bonjour", "fr")
    ```
    """
    if value is None:
        return None
    return Literal(value, lang=lang)

def get_literal_with_datatype(value:str, datatype:URIRef):
    """
    Create a Literal object with a specified datatype.

    Parameters:
    - value (str): The string value to be turned into a Literal.
    - datatype (URIRef): The URIRef representing the datatype for the Literal.

    Returns:
    - Literal: A Literal object containing the provided value and datatype.

    Example usage:
    ```python
    literal = get_literal_with_datatype("123", XSD.integer)
    ```
    """
    return Literal(value, datatype=datatype)

def get_geometry_wkt_literal(geom_wkt:str):
    """
    Create a Literal object for a geometry in WKT (Well-Known Text) format.

    Parameters:
    - geom_wkt (str): The WKT representation of the geometry.

    Returns:
    - Literal: A Literal object containing the WKT geometry with the appropriate datatype.

    Example usage:
    ```python
    literal = get_geometry_wkt_literal("POINT(1 1)")
    ```
    """
    return get_literal_with_datatype(geom_wkt, np.GEO.wktLiteral)

def get_name_literal(label:str, lang:str=None):
    """
    Create a Literal object representing a name with an optional language tag.

    Parameters:
    - label (str): The name to be represented as a Literal.
    - lang (str, optional): The language tag to associate with the Literal (default is None).

    Returns:
    - Literal: A Literal object containing the name and language (if provided).

    Example usage:
    ```python
    literal = get_name_literal("John Doe", "en")
    ```
    """
    return get_literal_with_lang(label, lang)

def get_insee_literal(insee_num:str):
    """
    Create a Literal object for an INSEE number (a French national identification number).

    Parameters:
    - insee_num (str): The INSEE number to be represented as a Literal.

    Returns:
    - Literal: A Literal object containing the INSEE number.

    Example usage:
    ```python
    literal = get_insee_literal("01343")
    ```
    """
    return get_literal_without_option(insee_num)

def convert_result_elem_to_rdflib_elem(result_elem:dict):
    """
    Convert a dictionary describing an element of a query result into an RDFLib element (URIRef, Literal, or BNode).

    Parameters:
    - result_elem (dict): A dictionary representing an element, typically from a SPARQL query result.
      It should contain keys such as 'type', 'value', 'xml:lang', and 'datatype'.

    Returns:
    - URIRef, Literal, or BNode: The RDFLib element corresponding to the dictionary description.

    Example usage:
    ```python
    rdflib_elem = convert_result_elem_to_rdflib_elem({"type": "uri", "value": "http://example.org"})
    ```
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
    """
    Generate a new URIRef based on the provided namespace and an optional prefix.

    Parameters:
    - namespace (Namespace, optional): The namespace to generate the URIRef from (default is None).
    - prefix (str, optional): A prefix to be used as part of the URI (default is None).

    Returns:
    - URIRef: A newly generated URIRef.

    Example usage:
    ```python
    uri = generate_uri(np.EX, "example")
    ```
    """
    if prefix:
        return namespace[f"{prefix}_{uuid4().hex}"]
    else:
        return namespace[uuid4().hex]

def generate_uuid():
    """
    Generate a random UUID string.

    Returns:
    - str: A random UUID in hexadecimal string format.

    Example usage:
    ```python
    unique_id = generate_uuid()
    ```
    """
    return uuid4().hex

def add_namespaces_to_graph(g:Graph, namespaces:dict):
    """
    Add a set of namespaces to an RDFLib graph.

    Parameters:
    - g (Graph): The RDFLib Graph object to which the namespaces will be added.
    - namespaces (dict): A dictionary where the keys are prefix strings and the values are Namespace objects.

    Returns:
    - None: The namespaces are added to the graph in-place.

    Example usage:
    ```python
    add_namespaces_to_graph(g, {"ex": np.EX, "geo": np.GEO})
    ```
    """
    for prefix, namespace in namespaces.items():
        g.bind(prefix, namespace)

def is_valid_uri(uri_str:str):
    """
    Check whether a given string is a valid URI.

    Parameters:
    - uri_str (str): The string to check.

    Returns:
    - bool: True if the string is a valid URI, False otherwise.

    Example usage:
    ```python
    is_valid = is_valid_uri("http://example.org")
    ```
    """
    regex = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return uri_str is not None and regex.search(uri_str)

def get_valid_uri(uri_str:str):
    """
    Convert a valid URI string into a URIRef object, or return None if the string is not valid.

    Parameters:
    - uri_str (str): The string representing the URI to validate and convert.

    Returns:
    - URIRef or None: A URIRef if the string is valid, or None if the string is not a valid URI.

    Example usage:
    ```python
    uri_ref = get_valid_uri("http://example.org")
    ```
    """
    if is_valid_uri(uri_str):
        return URIRef(uri_str)
    else:
        return None

def get_boolean_value(boolean:Literal):
    """
    Convert an RDFLib boolean Literal to a Python boolean (True or False), or None if the Literal is not a boolean.

    Parameters:
    - boolean (Literal): A Literal object representing a boolean value in RDFLib.

    Returns:
    - bool or None: The corresponding Python boolean value (True or False), or None if the input is not a valid boolean.

    Example usage:
    ```python
    python_bool = get_boolean_value(Literal("true", datatype=XSD.boolean))
    ```
    """
    boolean_val = boolean.strip()
    if boolean.datatype != XSD.boolean:
        return None
    
    if boolean_val == "false":
        return False
    elif boolean_val == "true":
        return True
    
    return None
