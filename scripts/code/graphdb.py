import filemanagement as fm
from rdflib import Graph, Namespace, Literal, BNode, URIRef
from rdflib.namespace import RDF
import requests

## Build uris from `graphdb_url` and `repository_name`

def get_repository_uri_from_name(graphdb_url:URIRef, repository_name:str) -> URIRef:
    """Get uri of the repository from its name and graphdb_url"""
    return graphdb_url + "/repositories/" + repository_name

def get_repository_namespaces_uri_from_name(graphdb_url:URIRef, repository_name:str) -> URIRef:
    return graphdb_url + "/repositories/" + repository_name + "/namespaces"

def get_named_graph_uri_from_name(graphdb_url:URIRef, repository_name:str, named_graph_name:str) -> URIRef:
    return graphdb_url + "/repositories/" + repository_name + "/rdf-graphs/" + named_graph_name

def get_repository_uri_statements_from_name(graphdb_url:URIRef, repository_name:str) -> URIRef:
    return graphdb_url + "/repositories/" + repository_name + "/statements"

def get_rest_repository_uri_from_name(graphdb_url:URIRef, repository_name:str) -> URIRef:
    return graphdb_url + "/rest/repositories/" + repository_name

def get_rest_respositories_uri(graphdb_url:URIRef) -> URIRef:
    return graphdb_url + "/rest/repositories"

## Create repository

def create_repository(graphdb_url:URIRef, repository_name:str, repository_config_file:str, ruleset_file:str=None, ruleset_name:str=None, disable_same_as:bool=False, check_for_inconsistencies:bool=False):
    # Create a configuration file for the repository
    # Here `ruleset_name` is None as ruleset will be defined after having created the repository
    create_config_local_repository_file(repository_config_file, repository_name, ruleset_name=None, disable_same_as=disable_same_as, check_for_inconsistencies=check_for_inconsistencies)
    
    # Thanks to configuration file, create the repository
    create_repository_from_config_file(graphdb_url, repository_config_file)

    if ruleset_file is not None:
        ruleset_name = "perso_rules"
        add_ruleset_from_file(graphdb_url, repository_name, ruleset_file, ruleset_name)
    elif ruleset_name is not None:
        add_ruleset_from_name(graphdb_url, repository_name, ruleset_name)

    change_ruleset(graphdb_url, repository_name, ruleset_name)

def create_repository_from_config_file(graphdb_url:URIRef, local_config_file:str):
    url = get_rest_respositories_uri(graphdb_url)
    files = {"config":open(local_config_file,'rb')}
    r = requests.post(url, files=files)
    return r

def create_config_local_repository_file(config_repository_file:str, repository_name:str, ruleset_name:str="rdfsplus-optimized", disable_same_as:bool=True, check_for_inconsistencies:bool=False):
    """
    Create a configuration file for a repository which has to be created.
    * `config_repository_file` is the path of the configuration file ;
    * `repository_name` is the name of the repository which has to be created ;
    * `ruleset_name` is the name of the ruleset. It is `rdfsplus-optimized` by default
    * `disable_same_as` is a boolean to know if `owl:sameAs` links have to be propagated
    * `check_for_inconsistencies` is a boolean to know if the repository has to check for inconsistencies
    """

    # Define namespaces of the configuration
    rep = Namespace("http://www.openrdf.org/config/repository#")
    sr = Namespace("http://www.openrdf.org/config/repository/sail#")
    sail = Namespace("http://www.openrdf.org/config/sail#")
    graph_db = Namespace("http://www.ontotext.com/config/graphdb#")
    g = Graph() # Initialize a graph

    elem = BNode()
    repository_impl = BNode()
    sail_impl = BNode()
    
    disable_same_as_str = str(disable_same_as).lower()

    # ruleset_name can be the name of built-in ruleset ("owl-max", rdfsplus-optimized"...) of the path of a ruleset file
    if ruleset_name is None:
        ruleset_name = "empty"
        
    g.add((elem, RDF.type, rep.Repository))
    g.add((elem, rep.repositoryID, Literal(repository_name)))
    g.add((elem, rep.repositoryImpl, repository_impl))
    g.add((repository_impl, rep.repositoryType, Literal("graphdb:SailRepository")))
    g.add((repository_impl, sr.sailImpl, sail_impl))
    g.add((sail_impl, sail.sailType, Literal("graphdb:Sail")))
    g.add((sail_impl, graph_db["base-URL"], Literal("http://example.org/owlim#")))
    g.add((sail_impl, graph_db["defaultNS"], Literal("")))
    g.add((sail_impl, graph_db["entity-index-size"], Literal("10000000")))
    g.add((sail_impl, graph_db["entity-id-size"], Literal("32")))
    g.add((sail_impl, graph_db["imports"], Literal("")))
    g.add((sail_impl, graph_db["repository-type"], Literal("file-repository")))
    g.add((sail_impl, graph_db["ruleset"], Literal(ruleset_name)))
    g.add((sail_impl, graph_db["storage-folder"], Literal("storage")))
    g.add((sail_impl, graph_db["enable-context-index"], Literal("false")))
    g.add((sail_impl, graph_db["enablePredicateList"], Literal("true")))
    g.add((sail_impl, graph_db["in-memory-literal-properties"], Literal("true")))
    g.add((sail_impl, graph_db["enable-literal-index"], Literal("true")))
    g.add((sail_impl, graph_db["check-for-inconsistencies"], Literal(check_for_inconsistencies)))
    g.add((sail_impl, graph_db["disable-sameAs"], Literal(disable_same_as_str)))
    g.add((sail_impl, graph_db["query-timeout"], Literal("0")))
    g.add((sail_impl, graph_db["query-limit-results"], Literal("0")))
    g.add((sail_impl, graph_db["throw-QueryEvaluationException-on-timeout"], Literal("false")))
    g.add((sail_impl, graph_db["read-only"], Literal("false")))
    
    # Export created graph with the configuration file
    g.serialize(destination=config_repository_file)
    
def load_ontologies(graphdb_url:URIRef, repository_name:str, ont_files:list[str]=[], ontology_named_graph_name:str="ontology"):
    """Import all ontologies in a named graph in the given repository"""
    for ont_file in ont_files:
        import_ttl_file_in_graphdb(graphdb_url, repository_name, ont_file, ontology_named_graph_name)

## Delete and/or clean repository

def clear_repository(graphdb_url:URIRef, repository_name:str):
    """Remove all contents from repository. /!\ The repository still exists after this process."""

    url = get_repository_uri_statements_from_name(graphdb_url, repository_name)
    r = requests.delete(url)
    return r

def remove_repository(graphdb_url:URIRef, repository_name:str):
    """Remove a repository defined by its name"""

    url = get_repository_uri_from_name(graphdb_url, repository_name)
    r = requests.delete(url)
    return r

def reinitialize_repository(graphdb_url:URIRef, repository_name:str, repository_config_file:str,
                            ruleset_file:str=None, ruleset_name:str=None, disable_same_as:bool=False,
                            check_for_inconsistencies:bool=False, allow_removal:bool=True):
    """
    Reinitialize a repository by removing it and recreating it again
    """

    # This action is done only if the repository already exists
    if get_repository_existence(graphdb_url, repository_name):
        # Remove the repository
        # `allow_removal` is an option as, sometimes, removing a repository does not work
        # Else, just clear the repository
        if allow_removal:
            remove_repository(graphdb_url, repository_name)
        else:
            clear_repository(graphdb_url, repository_name)

    create_repository(graphdb_url, repository_name, repository_config_file, ruleset_file, ruleset_name, disable_same_as, check_for_inconsistencies)

### Remove named graphs with different ways

def remove_named_graph(graphdb_url:URIRef, repository_name:str, named_graph_name:str):
    url = get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name).strip()
    r = requests.delete(url)
    return r

def remove_named_graph_from_uri(named_graph_uri:URIRef):
    r = requests.delete(named_graph_uri)
    return r

def remove_named_graphs(graphdb_url:URIRef, repository_name:str, named_graph_name_list:list[str]):
    for named_graph_name in named_graph_name_list:
        remove_named_graph(graphdb_url, repository_name, named_graph_name)

def remove_named_graphs_from_uris(named_graph_uris_list:list[URIRef]):
    for g in named_graph_uris_list:
        remove_named_graph_from_uri(g)

def remove_named_graph_from_query(graphdb_url:URIRef, repository_name:str, named_graph_name:str):
    graph_uri = get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name)

    query = f"""
    DELETE {{
        GRAPH ?g {{ ?s ?p ?o }}
    }}
    WHERE {{
        BIND ({graph_uri.n3()} AS ?g)
        GRAPH ?g {{ ?s ?p ?o }}
    }}
    """
    
    update_query(query, graphdb_url, repository_name)

def remove_named_graphs_from_query(graphdb_url:URIRef, repository_name:str, named_graph_names_list:list[str]):
    named_graph_uris_list, selected_named_graphs = [], ""
    for named_graph_name in named_graph_names_list:
        named_graph_uris_list.append(get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name).n3())

    selected_named_graphs = ",".join(named_graph_uris_list)

    query = f"""
    DELETE {{
        GRAPH ?g {{ ?s ?p ?o }}
    }}
    WHERE {{
        GRAPH ?g {{ ?s ?p ?o }}
        FILTER (?g in ({selected_named_graphs}))
    }}
    """

    update_query(query, graphdb_url, repository_name)

## Select or extract data within graph

def export_data_from_repository(graphdb_url:URIRef, repository_name:str, out_ttl_file:str, named_graph_name:str=None, named_graph_uri:URIRef=None):
    """
    Export data from a repository whose name is given `repository_name` to a turtle file whose path is given by `out_ttl_file`.
    If you only want to extract data from a specified named graph within the repository, you can select this named graph by giving its name (`named_graph_name`) or its URI (`named_graph_uri`).
    """

    url = get_repository_uri_statements_from_name(graphdb_url, repository_name).strip()
    headers = get_http_headers_dictionary(content_type="application/x-www-form-urlencoded", accept="text/turtle")
    params = {}

    if named_graph_uri is not None:
        params["context"] = named_graph_uri.n3()
    elif named_graph_name is not None:
        named_graph_uri = get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name)
        params["context"] = named_graph_uri.n3()

    r = requests.get(url, params=params, headers=headers)
    fm.write_file(r.text, out_ttl_file)

def select_query_to_txt_file(query:str, graphdb_url:URIRef, repository_name:str, res_query_file:str):
    """Export data from repository whose name is given `repository_name` with a SELECT query to a text file whose path is given by `res_query_file`."""

    url = get_repository_uri_from_name(graphdb_url, repository_name).strip()
    headers = get_http_headers_dictionary(content_type="application/x-www-form-urlencoded")
    data = {"query":query}
    r = requests.post(url, data=data, headers=headers)
    fm.write_file(r.text, res_query_file)

def select_query_to_json(query:str, graphdb_url:URIRef, repository_name:str):
    """Return data within a JSON object from repository whose name is given `repository_name` with a SELECT query."""
    url = get_repository_uri_from_name(graphdb_url, repository_name).strip()
    headers = get_http_headers_dictionary(content_type="application/x-www-form-urlencoded", accept="application/json")
    data = {"query":query}
    r = requests.post(url, data=data, headers=headers)
    return r.json()

## Update graph with query or ttl file

def update_query(query:str, graphdb_url:URIRef, repository_name:str):
    """Send an update query (INSERT, INSERT DATA, DELETE, DELETE DATA) to a repository to update it"""

    url = get_repository_uri_statements_from_name(graphdb_url, repository_name).strip()
    headers = get_http_headers_dictionary(content_type="application/x-www-form-urlencoded")
    data = {"update":query}
    r = requests.post(url, data=data, headers=headers)
    return r

def import_ttl_file_in_graphdb(graphdb_url:URIRef, repository_name:str, ttl_file:str, named_graph_name:str=None, named_graph_uri:URIRef=None):
    """
    Import data from turtle file whose path is `ttl_file` to a repository whose name is given `repository_name`. 
    By default, data is imported default named graph but you can specify a named graph by giving its name (`named_graph_name`) or its URI (`named_graph_uri`).
    """

    if named_graph_uri is not None:
        urlref = named_graph_uri
    elif named_graph_name is not None:
        urlref = get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name)
    else:
        urlref = get_repository_uri_statements_from_name(graphdb_url, repository_name)
    
    url = urlref.strip()
    headers = get_http_headers_dictionary(content_type="application/x-turtle")
        
    with open(ttl_file, 'rb') as f:
        data = f.read()
    
    r = requests.post(url, data=data, headers=headers)
    return r

## Manage namespaces

def get_repository_namespaces(graphdb_url:URIRef, repository_name:str):
    """Get all stored namespaces (with their related prefix) of a repository within a dictionary."""

    url = get_repository_namespaces_uri_from_name(graphdb_url, repository_name).strip()
    headers = get_http_headers_dictionary(content_type="application/x-www-form-urlencoded", accept="application/json")
    r = requests.get(url, headers=headers)

    namespaces = {}
    for elem in r.json().get("results").get("bindings"):
        prefix = elem["prefix"]["value"]
        uri = elem["namespace"]["value"]
        namespaces[prefix] = uri

    return namespaces

def add_prefix_to_repository(graphdb_url:URIRef, repository_name:str, namespace:Namespace, prefix:str):
    url = get_repository_namespaces_uri_from_name(graphdb_url, repository_name).strip() + "/" + prefix
    headers = get_http_headers_dictionary(content_type="text/plain")
    data = namespace.strip()
    r = requests.put(url, headers=headers, data=data)
    return r

def add_prefixes_to_repository(graphdb_url:URIRef, repository_name:str, namespace_prefixes:dict):
    for prefix, namespace in namespace_prefixes.items():
        add_prefix_to_repository(graphdb_url, repository_name, namespace, prefix)

def get_repository_prefixes(graphdb_url:URIRef, repository_name:str, perso_namespaces:dict=None):
    """
    perso_namespaces is a dictionnary which stores personalised namespaces to add of overwrite repository namespaces.
    keys are prefixes and values are URIs
    Ex: `{"geo":Namespace("http://data.ign.fr/def/geofla")}`
    """

    namespaces = get_repository_namespaces(graphdb_url, repository_name)
    if perso_namespaces is not None:
        namespaces.update(perso_namespaces)

    prefixes = ""
    for prefix, uri in namespaces.items():
        str_uri = uri[""].n3()
        prefixes += f"PREFIX {prefix}: {str_uri}\n"
        
    return prefixes

## Manage repository inference and rulesets

def reinfer_repository(graphdb_url:URIRef, repository_name:str):
    """
    According to GraphDB : 'Statements are inferred only when you insert new statements. So, if reconnected to a repository with a different ruleset, it does not take effect immediately.'
    This function reinfers repository
    """
    
    query = """
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA { [] sys:reinfer [] }
    """

    update_query(query, graphdb_url, repository_name)

def turn_inference_off(graphdb_url:URIRef, repository_name:str):
    query = """
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA { [] sys:turnInferenceOff [] }
    """

    update_query(query, graphdb_url, repository_name)


def turn_inference_on(graphdb_url:URIRef, repository_name:str):
    query = """
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA { [] sys:turnInferenceOn [] }
    """

    update_query(query, graphdb_url, repository_name)

def add_ruleset_from_file(graphdb_url, repository_name, ruleset_file, ruleset_name):
    query  = f"""
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA {{
        <_:{ruleset_name}> sys:addRuleset <file:{ruleset_file}>
    }}
    """

    update_query(query, graphdb_url, repository_name)

def add_ruleset_from_name(graphdb_url:URIRef, repository_name:str, ruleset_name:str):
    query  = f"""
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA {{
        _:b sys:addRuleset "{ruleset_name}"
    }}
    """

    update_query(query, graphdb_url, repository_name)

def change_ruleset(graphdb_url:URIRef, repository_name:str, ruleset_name:str):
    query = f"""
    prefix sys: <http://www.ontotext.com/owlim/system#>
    INSERT DATA {{
        _:b sys:defaultRuleset "{ruleset_name}"
    }}
    """

    update_query(query, graphdb_url, repository_name)

## Auxiliary functions

def get_repository_existence(graphdb_url:URIRef, repository_name:str):
    """
    Get a boolean to know if the repository already exists (True if yes, False else)
    """

    url = get_rest_repository_uri_from_name(graphdb_url, repository_name)
    headers = get_http_headers_dictionary(content_type="application/x-turtle")
    r = requests.get(url, headers=headers)

    if r.text == "":
        return False
    else:
        return True
    

def get_http_headers_dictionary(content_type:str=None, accept:str=None):
    """
    Returns a dictionary which stores information about HTTP headers
    """
    headers = {}
    if content_type is not None:
        headers["Content-Type"] = content_type
    if accept is not None:
        headers["Accept"] = accept

    return headers