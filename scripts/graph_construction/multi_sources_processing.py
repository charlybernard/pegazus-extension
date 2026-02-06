import os
from rdflib import Graph, Literal, URIRef, SKOS
from scripts.graph_construction.namespaces import NameSpaces
from scripts.utils import str_processing as sp
from scripts.graph_construction import graphdb as gd
from scripts.graph_construction import graphrdf as gr
from scripts.graph_construction import resource_rooting as rr
import time
import json


np = NameSpaces()

def get_elements_with_labels(graphdb_url:URIRef, repository_name:str, has_filter_hidden_label=False):
    """
    Retrieves elements with labels and their types from a specified named graph in a GraphDB repository.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance that holds the repository.
    - repository_name (str): The name of the repository from which the elements will be retrieved.
    - has_filter_hidden_label (str): filter to avoid selecting elements which have already a hidden label

    Returns:
    - list: A list of bindings (elements with labels and their types) from the query result. Each binding contains the element, label, and element type.

    Description:
    This function executes a SPARQL query to retrieve elements of type `addr:AttributeVersion` or `addr:Landmark` from a specified named graph.
    For each element, it retrieves its label and type (e.g., `addr:Landmark` type or `addr:AttributeVersion` type). The result is returned as a list of bindings.

    Example usage:
    ```python
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'exampleRepository'
    named_graph_uri = URIRef('http://example.org/named_graph')

    # Retrieve elements with labels
    elements = get_elements_with_labels(graphdb_url, repository_name, has_filter_hidden_label)
    ```
    """

    filter_hidden_label = ""
    if has_filter_hidden_label:
        filter_hidden_label = "FILTER NOT EXISTS { ?elem skos:hiddenLabel ?x }"

    query = np.query_prefixes + f"""
        SELECT ?elem ?label ?elemType WHERE {{
            {{
                GRAPH ?g {{ ?elem a addr:AttributeVersion . }}
                ?lm a addr:Landmark ; addr:isLandmarkType ?elemType ; addr:hasAttribute ?attr .
                ?attr a addr:Attribute ; addr:isAttributeType atype:Name ; addr:hasAttributeVersion ?elem .
                ?elem addr:versionValue ?label .
            }} UNION {{
                GRAPH ?g {{ ?elem a addr:Landmark . }}
                ?elem rdfs:label ?label ; addr:isLandmarkType ?elemType .
            }}
            ?g a addr:SourceGraph .
            {filter_hidden_label}
        }}
        """
        
    results = gd.select_query_to_json(query, graphdb_url, repository_name)
    return results.get("results").get("bindings")

def get_pref_and_hidden_label_triples_for_element(element: URIRef, element_type: URIRef, label: Literal):
    """
    Generates preferred and hidden label triples for a given element based on its type and label.

    Parameters:
    - element (URIRef): The URI of the element for which the labels will be generated.
    - element_type (URIRef): The type of the element (e.g., Thoroughfare, Municipality, HouseNumber, etc.).
    - label (Literal): The label associated with the element, which will be used to generate the preferred and hidden labels.

    Returns:
    - list: A list of triples representing the preferred and hidden labels for the element. Each triple is a tuple of (subject, predicate, object),
            where the subject is the element URI, the predicate is either `SKOS.prefLabel` or `SKOS.hiddenLabel`, and the object is the label.

    Description:
    This function determines the type of the element and generates appropriate preferred and hidden label triples.
    The labels are normalized and simplified before being added to the list of triples.

    Example usage:
    ```python
    element = URIRef('http://example.org#SomeElement')
    element_type = URIRef('http://example.org#Thoroughfare')
    label = Literal('Main Street', lang='en')

    # Generate preferred and hidden label triples
    triples = get_pref_and_hidden_label_triples_for_element(element, element_type, label)
    ```
    """
    triples = []

    if element_type == np.LTYPE["Thoroughfare"]:
        lm_label_type = "thoroughfare"
    elif element_type in [np.LTYPE["Municipality"], np.LTYPE["District"]]:
        lm_label_type = "area"
    elif element_type in [np.LTYPE["HouseNumber"],np.LTYPE["StreetNumber"],np.LTYPE["DistrictNumber"],np.LTYPE["PostalCodeArea"]]:
        lm_label_type = "number"
    else:
        lm_label_type = None

    normalized_name, simplified_name = sp.normalize_and_simplify_name_version(label.strip(), lm_label_type, label.language)

    if normalized_name is not None:
        normalized_name_lit = Literal(normalized_name, lang=label.language)
        triple = (element, SKOS.prefLabel, normalized_name_lit)
        triples.append(triple)
    if simplified_name is not None:
        simplified_name_lit = Literal(simplified_name, lang=label.language)
        triple = (element, SKOS.hiddenLabel, simplified_name_lit)
        triples.append(triple)

    return triples

def get_pref_and_hidden_label_triples_for_elements(elements: list):
    """
    Generates preferred and hidden label triples for a list of elements.

    Parameters:
    - elements (list): A list of dictionaries, where each dictionary represents an element containing the following keys:
        - 'elem': The element URI.
        - 'elemType': The type of the element according landmark type it is related to (e.g., Housenumber, Thoroughfare, City...).
        - 'label': The label associated with the element.

    Returns:
    - list: A list of triples representing the preferred and hidden labels for the elements. Each triple is a tuple of (subject, predicate, object),
            where the subject is the element URI, the predicate is either `SKOS.prefLabel` or `SKOS.hiddenLabel`, and the object is the label.

    Description:
    This function processes a list of elements, retrieving the necessary URIs and labels for each element, and then calls
    `get_pref_and_hidden_label_triples_for_element` to generate the corresponding triples. The function accumulates the triples for all elements
    and returns them as a list.

    Example usage:
    ```python
    elements = [
        {'elem': some_elem_uri, 'elemType': some_elem_type, 'label': some_label},
        {'elem': another_elem_uri, 'elemType': another_elem_type, 'label': another_label}
    ]

    # Generate preferred and hidden label triples for the elements
    triples = get_pref_and_hidden_label_triples_for_elements(elements)
    ```
    """

    g = Graph()

    for element in elements:
        # Retrieval of URIs (attribute and attribute version) and geometry
        elem = gr.convert_result_elem_to_rdflib_elem(element.get('elem'))
        elem_type = gr.convert_result_elem_to_rdflib_elem(element.get('elemType'))
        label = gr.convert_result_elem_to_rdflib_elem(element.get('label'))

        triples_to_add = get_pref_and_hidden_label_triples_for_element(elem, elem_type, label)
        for triple in triples_to_add:
            g.add(triple)

    return g


def add_pref_and_hidden_labels_for_elements(graphdb_url:URIRef, repository_name:str, labels_named_graph_uri:URIRef, pref_hidden_labels_ttl_file:str):
    """
    Adds preferred and hidden labels for the elements (name attribute versions and landmark) to a specified repository in GraphDB.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance that holds the repository where the labels will be added.
    - repository_name (str): The name of the repository where the labels will be inserted.
    - labels_named_graph_uri (URIRef): The URI of the named graph containing the factoids from which the labels are generated.

    Returns:
    - None: The function does not return any value. It performs an update on the GraphDB repository by adding the triples.

    Description:
    This function retrieves elements with labels from the specified named graph, generates the triples for preferred and hidden labels,
    and then adds them to the specified repository.

    Example usage:
    ```python
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'exampleRepository'
    labels_named_graph_uri = URIRef('http://example.org/labels')

    # Add preferred and hidden labels for name attribute versions
    add_pref_and_hidden_labels_for_elements(graphdb_url, repository_name, labels_named_graph_uri)
    ```
    """

    elements = get_elements_with_labels(graphdb_url, repository_name, has_filter_hidden_label=True)
    graph_with_triples_to_add = get_pref_and_hidden_label_triples_for_elements(elements)
    graph_with_triples_to_add.serialize(pref_hidden_labels_ttl_file)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, pref_hidden_labels_ttl_file, named_graph_uri=labels_named_graph_uri)
    

def remove_all_triples_for_resources_to_remove(graphdb_url:URIRef, repository_name:str):
    """
    Removes all triples associated with resources marked for removal from the specified GraphDB repository.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance where the repository is located.
    - repository_name (str): The name of the repository from which the triples will be removed.

    Returns:
    - None: The function does not return any value. It performs an update on the GraphDB repository to delete the relevant triples.

    Description:
    This function constructs and executes a SPARQL `DELETE` query that removes all triples associated with resources 
    that are marked for removal. A resource is considered for removal if it has a `toRemove` property set to `true`.
    The query deletes both the triples directly referencing the resources marked for removal as well as any triples
    where these resources are the subject of other triples.

    Example usage:
    ```python
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'exampleRepository'

    # Remove all triples for resources marked for removal
    remove_all_triples_for_resources_to_remove(graphdb_url, repository_name)
    ```
    """

    to_remove_property = np.ADDR["toRemove"]

    query = np.query_prefixes + f"""
    DELETE {{
        ?s ?p ?tmpResource.
        ?tmpResource ?p ?o.
    }}
    WHERE {{
        ?tmpResource {to_remove_property.n3()} ?toRemove.
        FILTER(?toRemove)
        {{?tmpResource ?p ?o}} UNION {{?s ?p ?tmpResource}}
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def create_factoid_repository(graphdb_url:URIRef, repository_name:str, tmp_folder:str,
                              ont_file:str, ontology_named_graph_name:str,
                              ruleset_name:str=None, disable_same_as:bool=False, clear_if_exists:bool=False):
    """
    Initializes a repository to create a factoids graph in a GraphDB instance.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance where the repository will be created.
    - repository_name (str): The name of the repository to be created.
    - tmp_folder (str): The temporary folder where the repository configuration file will be stored.
    - ont_file (str): The file path to the ontology file (TTL format) to import into the repository.
    - ontology_named_graph_name (str): The name of the graph in which the ontology will be stored.
    - ruleset_name (str, optional): The name of the ruleset to apply to the repository (default is None).
    - disable_same_as (bool, optional): Whether to disable the `sameAs` property during repository creation (default is False).
    - clear_if_exists (bool, optional): Whether to clear all existing data in the repository if it already exists (default is False).

    Returns:
    - None: The function does not return any value. It creates the repository and imports the ontology file.

    Description:
    This function initializes a new repository for storing factoids and imports an ontology into the repository.
    If the `clear_if_exists` parameter is set to `True`, it clears the repository if it already exists. The function also adds necessary prefixes
    to the repository and imports the provided ontology file into the specified graph.

    Example usage:
    ```python
    create_factoid_repository(graphdb_url, 'factoidRepo', '/tmp', 'ontology.ttl', 'http://example.org/named_graph')
    ```
    """

    local_config_file_name = f"config_for_{repository_name}.ttl"
    local_config_file = os.path.join(tmp_folder, local_config_file_name)

    # Repository creation
    gd.create_repository(graphdb_url, repository_name, local_config_file, ruleset_file=None, ruleset_name=ruleset_name, disable_same_as=disable_same_as)

    if clear_if_exists:
        gd.clear_repository(graphdb_url, repository_name)

    gd.add_prefixes_to_repository(graphdb_url, repository_name, np.namespaces_with_prefixes)
    gd.add_named_graph_prefix_to_repository(graphdb_url, repository_name, "graph")
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, ont_file, ontology_named_graph_name)

def transfert_rdflib_graph_to_factoids_repository(graphdb_url: URIRef, repository_name: str, factoids_named_graph_name: str,
                                                  g: Graph, kg_file: str, tmp_folder: str,
                                                  ont_file: str, ontology_named_graph_name: str):
    """
    Transfers an RDFLib graph to a factoids repository in a GraphDB instance.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance where the repository is located.
    - repository_name (str): The name of the repository to which the RDFLib graph will be transferred.
    - factoids_named_graph_name (str): The name of the factoids graph in the repository.
    - g (Graph): The RDFLib graph containing the data to be transferred.
    - kg_file (str): The file path where the serialized RDFLib graph will be saved temporarily.
    - tmp_folder (str): The temporary folder where configuration files will be stored.
    - ont_file (str): The file path to the ontology file (TTL format) to be imported into the repository.
    - ontology_named_graph_name (str): The name of the graph where the ontology will be stored.

    Returns:
    - None: The function does not return any value. It transfers the RDFLib graph to the repository and imports the ontology.

    Description:
    This function serializes an RDFLib graph to a file and creates a factoids repository in the specified GraphDB instance.
    It then imports the RDFLib graph into the factoids repository and associates it with the specified named graph.
    The ontology file is also imported into the repository during the process.

    Example usage:
    ```python
    transfert_rdflib_graph_to_factoids_repository(graphdb_url, 'factoidRepo', 'factoidsGraph', g, 'graph.ttl', '/tmp', 'ontology.ttl', 'http://example.org/named_graph')
    ```
    """
    
    g.serialize(kg_file)

    # Creating repository
    create_factoid_repository(graphdb_url, repository_name, tmp_folder,
                                ont_file, ontology_named_graph_name, ruleset_name="rdfsplus-optimized",
                                disable_same_as=False, clear_if_exists=True)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, kg_file, factoids_named_graph_name)


def transfert_rdflib_graph_to_named_graph_repository(
        g: Graph, graphdb_url: URIRef,
        repository_name: str, named_graph_name: str,
        kg_file: str, named_graph_type:str=None,
        meta_named_graph_name:str=None, is_active: bool=True):
    
    g.serialize(kg_file)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, kg_file, named_graph_name)

    if meta_named_graph_name is not None:
        # Get the URI of the meta graph
        if named_graph_type == "source":
            add_source_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, named_graph_name, is_active=is_active)
        elif named_graph_type == "construction":
            add_construction_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, named_graph_name, is_active=is_active)
        elif named_graph_type == "facts":
            add_final_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, named_graph_name, is_active=is_active)
        else:
            add_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, named_graph_name, is_active=is_active)

####################################################################


def import_factoids_in_facts(graphdb_url:URIRef, repository_name:str,
                             factoids_named_graph_name:str, facts_named_graph_name:str, inter_sources_name_graph_name:str,
                             pref_hidden_labels_ttl_file:str):
    """
    Imports factoids into the facts graph and links them with inter-sources in a GraphDB repository.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance where the repository is located.
    - repository_name (str): The name of the repository containing the factoids and facts graphs.
    - factoids_named_graph_name (str): The name of the graph containing the factoids to be imported.
    - facts_named_graph_name (str): The name of the graph containing the facts to be linked with the factoids.
    - inter_sources_name_graph_name (str): The name of the graph containing the inter-sources to link with factoids.

    Returns:
    - None: The function does not return any value. It performs the import and linking of factoids with facts.

    Description:
    This function imports factoids into the facts graph in the specified repository and links the factoids with the facts using inter-source data.
    It first adds standardized and simplified labels for landmarks in the factoid graph to facilitate linking with fact landmarks.
    Then, it links the factoids with the facts in the specified graphs.

    Example usage:
    ```python
    import_factoids_in_facts(graphdb_url, 'factoidRepo', 'factoidsGraph', 'FinalGraph', 'interSourcesGraph')
    ```
    """
    
    facts_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    factoids_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, factoids_named_graph_name)
    inter_sources_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, inter_sources_name_graph_name)

    # Addition of standardised and simplified labels for landmarks (on the factoid graph) in order to make links with fact landmarks
    add_pref_and_hidden_labels_for_elements(graphdb_url, repository_name, pref_hidden_labels_ttl_file)

    rr.link_factoids_with_facts(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)


######################################################### Named graph management ######################################################

def add_named_graph_to_repository(
        graphdb_url:URIRef, repository_name:str,
        meta_named_graph_name:str, named_graph_name:str,
        graph_class:URIRef=None, is_active:bool=True):
    
    named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, named_graph_name)
    meta_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, meta_named_graph_name)

    if graph_class is None:
        graph_class = np.ADDR["Graph"]

    query = np.query_prefixes + f"""
        INSERT DATA {{
            GRAPH {meta_name_graph_uri.n3()} {{
                {named_graph_uri.n3()} a {graph_class.n3()} .
            }}
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

    if is_active is not None:
        # Set the named graph as active or not
        set_named_graph_active(graphdb_url, repository_name, named_graph_name, meta_named_graph_name, active=is_active)

def add_source_named_graph_to_repository(
        graphdb_url:URIRef, repository_name:str,
        meta_named_graph_name:str, source_named_graph_name:str, is_active:bool=True):
    
    graph_class = np.ADDR["SourceGraph"]
    add_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, source_named_graph_name, graph_class=graph_class, is_active=is_active)


def add_construction_named_graph_to_repository(
        graphdb_url:URIRef, repository_name:str,
        meta_named_graph_name:str,
        construction_named_graph_name:str, is_active:bool=True):
    
    graph_class = np.ADDR["ConstructionGraph"]
    add_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, construction_named_graph_name, graph_class=graph_class, is_active=is_active)

def add_final_named_graph_to_repository(
        graphdb_url:URIRef, repository_name:str,
        meta_named_graph_name:str,
        facts_named_graph_name:str, facts_named_graph_name_label:str=None, lang:str=None,
        is_active:bool=None):
    
    graph_class = np.ADDR["FinalGraph"]
    add_named_graph_to_repository(graphdb_url, repository_name, meta_named_graph_name, facts_named_graph_name, graph_class=graph_class, is_active=is_active)

    if facts_named_graph_name_label is not None:
        add_final_named_graph_label_to_repository(graphdb_url, repository_name, meta_named_graph_name, facts_named_graph_name, facts_named_graph_name_label, lang=lang)

def add_final_named_graph_label_to_repository(
        graphdb_url:URIRef, repository_name:str,
        meta_named_graph_name:str,
        facts_named_graph_name:str, facts_named_graph_name_label:str, lang:str=None):
    
    # Add label for the final graph in the meta graph
    meta_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, meta_named_graph_name)
    named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)

    lang_suffix = f"@{lang}" if lang is not None else ""

    query = np.query_prefixes + f"""
        INSERT DATA {{
            GRAPH {meta_named_graph_uri.n3()} {{
                {named_graph_uri.n3()} rdfs:label "{facts_named_graph_name_label}"{lang_suffix} .
            }}
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def set_all_named_graphs_active(
    graphdb_url: URIRef,
    repository_name: str,
    meta_named_graph_name: str,
    active: bool,
    graph_type: str=None
):
    
    meta_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, meta_named_graph_name)

    # Booléen RDF
    new_value = gr.get_boolean_literal(active)

    graph_types = {
        "source":np.ADDR["SourceGraph"],
        "final":np.ADDR["FinalGraph"],
        "construction":np.ADDR["ConstructionGraph"]
    }

    graph_class = graph_types.get(graph_type, np.ADDR["Graph"])

    query = np.query_prefixes + f"""
    DELETE {{
        GRAPH ?g {{
            ?gs addr:isActiveGraph ?oldValue .
        }}
    }}
    INSERT {{
        GRAPH ?g {{
            ?gs addr:isActiveGraph {new_value.n3()} .
        }}
    }}
    WHERE {{
        BIND ({meta_name_graph_uri.n3()} AS ?g)

        GRAPH ?g {{
            ?gs a ?gsClass .
            OPTIONAL {{ ?gs addr:isActiveGraph ?oldValue . }}
        }}

        ?gsClass rdfs:subClassOf* {graph_class.n3()} .
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def set_named_graph_active(
    graphdb_url: URIRef,
    repository_name: str,
    graph_name: str,
    meta_named_graph_name: str,
    active: bool=True,
):
    """
    Set the status of a single source graph in the meta graph.
    
    Args:
        graphdb_url: URI of the GraphDB instance
        repository_name: repository name
        graph_name: the source graph name to update
        meta_named_graph_name: name of the meta graph containing the sources
        active: True to activate (set as active), False to deactivate
    """

    # Récupère l'URI du graphe source
    gs_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, graph_name)
    meta_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, meta_named_graph_name)

    # Booléen RDF
    new_value = gr.get_boolean_literal(active)

    query = np.query_prefixes + f"""
    DELETE {{
        GRAPH ?g {{
            {gs_uri.n3()} addr:isActiveGraph ?oldValue .
        }}
    }}
    INSERT {{
        GRAPH ?g {{
            {gs_uri.n3()} addr:isActiveGraph {new_value.n3()} .
        }}
    }}
    WHERE {{
        BIND({meta_name_graph_uri.n3()} AS ?g)
        GRAPH ?g {{
            {gs_uri.n3()} a ?gsClass .
            OPTIONAL {{ {gs_uri.n3()} addr:isActiveGraph ?oldValue . }}
        }}
        ?gsClass rdfs:subClassOf* addr:Graph .
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def remove_construction_named_graphs(graphdb_url, repository_name):
    """
    Remove all named graphs of type addr:ConstructionGraph from a GraphDB repository.

    This function executes a SPARQL UPDATE query that:
    - Identifies all named graphs typed as addr:ConstructionGraph
    - Deletes all triples contained in each of these graphs

    Parameters
    ----------
    graphdb_url : URIRef
        Base URL of the GraphDB instance.
    repository_name : str
        Name of the repository where the construction graphs are stored.

    Notes
    -----
    - The named graphs are emptied by deleting all their triples.
      (GraphDB treats empty named graphs as removed.)
    - This operation is irreversible; use with caution.
    """

    # ---------------------------------------------------------------
    # Supprimer tous les graphes de construction et leurs références
    # 
    # Cette opération fait deux choses :
    # 1) Supprime tous les triples contenus dans chaque graphe nommé ?g
    #    typé addr:ConstructionGraph (le type est stocké hors du graphe).
    # 2) Supprime tous les triples où ?g est utilisé comme sujet ou objet
    #    dans n’importe quel graphe ?h.
    #
    # La requête SPARQL utilise :
    # - DELETE { ... } WHERE { ... } pour vider les graphes et les références
    # - GRAPH ?g et GRAPH ?h pour distinguer les graphes ciblés
    # - UNION pour capturer ?g comme sujet ou objet dans n’importe quel graphe
    # ---------------------------------------------------------------

    query1 = np.query_prefixes + f"""

    DELETE {{
        GRAPH ?g {{ ?s ?p ?o }}
    }}
    WHERE {{
        ?g a addr:ConstructionGraph .
        GRAPH ?g {{ ?s ?p ?o }}
    }}
    """

    query2 = np.query_prefixes + f"""

    DELETE {{
        ?g ?p1 ?o .
        ?s ?p2 ?g .
    }}
    WHERE {{
        ?g a addr:ConstructionGraph .

        {{ ?g ?p1 ?o }} UNION {{ ?s ?p2 ?g }}
    }}
    """

    queries = [query1, query2]

    for query in queries:
        # Execute the SPARQL UPDATE query on the target repository
        gd.update_query(query, graphdb_url, repository_name)


######################################################### Test functions ######################################################

def get_landmark_labels(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef):
    """
    Retrieves landmark labels and their types from a specified named graph in a GraphDB repository."
    """

    query = np.query_prefixes + f"""
        SELECT ?landmark ?landmarkType ?landmarkLabel ?relatedLandmarkType ?relatedLandmarkLabel WHERE {{
            GRAPH ?g {{ ?landmark a addr:Landmark . }}
            ?landmark rdfs:label ?landmarkLabel ; addr:isLandmarkType ?landmarkType .
            OPTIONAL {{
                ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?landmark ; addr:relatum ?relatedLandmark .
                ?relatedLandmark rdfs:label ?relatedLandmarkLabel ; addr:isLandmarkType ?relatedLandmarkType .
            }}
            FILTER(?g != {facts_named_graph_uri.n3()})
        }}
    """
    
    results = gd.select_query_to_json(query, graphdb_url, repository_name)
    return results.get("results").get("bindings")

def add_pref_and_hidden_labels_for_landmarks(graphdb_url:URIRef, repository_name:str,
                                             facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef,
                                             pref_hidden_labels_ttl_file:str):
    t1 = time.time()
    elements = get_landmark_labels(graphdb_url, repository_name, facts_named_graph_uri)
    t2 = time.time()
    print("Time to get elements with labels: ", t2 - t1)
    landmarks = get_pref_and_hidden_label_triples_for_landmarks(elements)
    t3 = time.time()
    print("Time to get triples: ", t3 - t2)
    print(landmarks)
    json.dump(landmarks, open("/home/CBernard2/Téléchargements/landmarks.json", "w"), indent=4)
    # graph_with_triples_to_add.serialize(pref_hidden_labels_ttl_file)

    # # Import the `kg_file` file into the directory
    # gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, pref_hidden_labels_ttl_file, named_graph_uri=facts_named_graph_uri)

def get_pref_and_hidden_label_triples_for_landmarks(elements: list):
    landmarks = {}

    for element in elements:
        # Retrieval of URIs (attribute and attribute version) and geometry
        lm = gr.convert_result_elem_to_rdflib_elem(element.get('landmark'))
        lm_type = gr.convert_result_elem_to_rdflib_elem(element.get('landmarkType'))
        lm_label = gr.convert_result_elem_to_rdflib_elem(element.get('landmarkLabel'))
        related_lm_type = gr.convert_result_elem_to_rdflib_elem(element.get('relatedLandmarkType'))
        related_lm_label = gr.convert_result_elem_to_rdflib_elem(element.get('relatedLandmarkLabel'))

        normalized_label, simplified_label = get_pref_and_hidden_label_for_landmark(lm_type, lm_label)
        related_normalized_label, related_simplified_label = get_pref_and_hidden_label_for_landmark(related_lm_type, related_lm_label)
        if isinstance(normalized_label, str) and isinstance(related_normalized_label, str):
            normalized_label += ", " + related_normalized_label
        if isinstance(simplified_label, str) and isinstance(related_simplified_label, str):
            simplified_label += " || " + related_simplified_label

        if lm_type not in landmarks.keys():
            landmarks[lm_type] = {}
        if simplified_label not in landmarks[lm_type].keys():
            landmarks[lm_type][simplified_label] = {
                "id":gr.generate_uuid(),
                "normalized_label":normalized_label,
                "simplified_label":simplified_label,
                "landmarks": [lm]
                }
        else:
            landmarks[lm_type][simplified_label]["landmarks"].append(lm)
        
    return landmarks

def get_pref_and_hidden_label_for_landmark(type: URIRef, label: Literal):
    if type == np.LTYPE["Thoroughfare"]:
        lm_label_type = "thoroughfare"
    elif type in [np.LTYPE["Municipality"], np.LTYPE["District"]]:
        lm_label_type = "area"
    elif type in [np.LTYPE["HouseNumber"],np.LTYPE["StreetNumber"],np.LTYPE["DistrictNumber"],np.LTYPE["PostalCodeArea"]]:
        lm_label_type = "number"
    else:
        lm_label_type = None

    label_value, label_lang = None, None
    if isinstance(label, Literal):
        label_value = label.strip()
        label_lang = label.language
    normalized_name, simplified_name = sp.normalize_and_simplify_name_version(label_value, lm_label_type, label_lang)

    return normalized_name, simplified_name

def create_facts_landmarks_graph(landmarks):
    g = Graph()

    for lm_type, lm_labels in landmarks.items():
        for lm_label, lm_data in lm_labels.items():
            lm_id = lm_data["id"]
            normalized_label = lm_data["normalized_label"]
            simplified_label = lm_data["simplified_label"]
            for landmark in lm_data["landmarks"]:
                g.add((landmark, SKOS.prefLabel, Literal(normalized_label)))
                g.add((landmark, SKOS.hiddenLabel, Literal(simplified_label)))
                g.add((landmark, np.ADDR["hasLandmarkID"], Literal(lm_id)))

    return g


########################################################## Main ######################################################

if __name__ == "__main__":
    # Example usage
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'addresses_from_factoids'
    facts_named_graph_uri = URIRef('http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts')
    inter_sources_name_graph_uri = URIRef('http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/inter_sources')
    pref_hidden_labels_ttl_file = 'pref_hidden_labels.ttl'

    add_pref_and_hidden_labels_for_landmarks(graphdb_url, repository_name,
                                             facts_named_graph_uri, inter_sources_name_graph_uri,
                                             pref_hidden_labels_ttl_file)
