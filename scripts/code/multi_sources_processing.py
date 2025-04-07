import os
from rdflib import Graph, Literal, URIRef, SKOS
from namespaces import NameSpaces
import str_processing as sp
import graphdb as gd
import graphrdf as gr
import resource_rooting as rr

np = NameSpaces()

def get_elements_with_labels(graphdb_url:URIRef, repository_name:str, named_graph_uri:URIRef):
    """
    Retrieves elements with labels and their types from a specified named graph in a GraphDB repository.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance that holds the repository.
    - repository_name (str): The name of the repository from which the elements will be retrieved.
    - factoids_named_graph_uri (URIRef): The URI of the named graph containing the factoids.

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
    elements = get_elements_with_labels(graphdb_url, repository_name, named_graph_uri)
    ```
    """

    query = np.query_prefixes + f"""
        SELECT ?elem ?label ?elemType WHERE {{
            BIND({named_graph_uri.n3()} AS ?g)
            {{
                GRAPH ?g {{ ?elem a addr:AttributeVersion . }}
                ?elem addr:versionValue ?label ;
                    addr:isAttributeVersionOf [
                        a addr:Attribute ;
                        addr:isAttributeType atype:Name ;
                        addr:isAttributeOf [a addr:Landmark ; addr:isLandmarkType ?elemType]] .
            }} UNION {{
                GRAPH ?g {{ ?elem a addr:Landmark . }}
                ?elem rdfs:label ?label ; addr:isLandmarkType ?elemType .
            }}
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

def generate_insert_data_query(triples: list, named_graph_uri: URIRef = None):
    """
    Generates a SPARQL INSERT DATA query to insert a list of triples into an optional named graph.

    Parameters:
    - triples (list): A list of triples, where each triple is a tuple (subject, predicate, object). 
                       Each element of the triple is either a URIRef or a Literal.
    - named_graph_uri (URIRef, optional): The URI of the named graph to insert the triples into. If not provided,
                                           the triples will be inserted into the default graph.

    Returns:
    - str: A string containing the SPARQL query. The query is formatted to insert the provided triples into
           the specified named graph (if provided), or into the default graph if no named graph is specified.

    Description:
    This function generates a SPARQL query in the form of `INSERT DATA` to add a set of triples to a specified graph in a GraphDB store.
    If a named graph URI is provided, the triples will be inserted into that graph. Otherwise, the triples will be inserted into the default graph.

    Example usage:
    ```python
    triples = [
        (URIRef('http://example.org#Alice'), URIRef('http://example.org#knows'), URIRef('http://example.org#Bob')),
        (URIRef('http://example.org#Bob'), URIRef('http://example.org#knows'), URIRef('http://example.org#Charlie'))
    ]
    named_graph_uri = URIRef('http://example.org/graph')

    query = generate_insert_data_query(triples, named_graph_uri)
    ```

    If no named graph is provided:
    ```python
    query = generate_insert_data_query(triples)
    ```

    This will generate a SPARQL query like:
    ```sparql
    INSERT DATA {
        GRAPH <http://example.org/graph> {
            <http://example.org#Alice> <http://example.org#knows> <http://example.org#Bob> .
            <http://example.org#Bob> <http://example.org#knows> <http://example.org#Charlie> .
        }
    }
    ```
    """

    # Prepare formatted triples
    triple_statements = [f"{s.n3()} {p.n3()} {o.n3()} ." for s, p, o in triples]
    triples_str = "\n".join(triple_statements)

    if isinstance(named_graph_uri, URIRef):
    # Build the query
        query = f"""
        INSERT DATA {{
            GRAPH {named_graph_uri.n3()} {{
                {triples_str}
            }}
        }}
        """
    else:
        query = f"""
        INSERT DATA {{
            {triples_str}
        }}
        """

    return query.strip()

def add_triples_to_repository(triples:list[tuple], graphdb_url:URIRef, repository_name:str, named_graph_uri:URIRef=None):
    """
    Adds a list of triples to a specified repository in a GraphDB store.

    Parameters:
    - triples (list[tuple]): A list of triples, where each triple is represented as a tuple (subject, predicate, object).
      These triples will be inserted into the specified repository.
    - graphdb_url (URIRef): The URL of the GraphDB instance that holds the repository where the triples will be added.
    - repository_name (str): The name of the repository where the triples will be inserted.
    - named_graph_uri (URIRef, optional): An optional named graph URI. If provided, the triples will be added to this
      specific graph within the repository. If not provided, the triples will be inserted into the default graph.

    Returns:
    - None: The function does not return any value. It performs an update on the GraphDB repository by adding the triples.

    Description:
    This function generates an insert data query using the provided triples and optional named graph URI.
    It then sends the query to the specified GraphDB repository to insert the data.

    Example usage:
    ```python
    # Define the triples
    triples = [
        (URIRef('http://example.org#Alice'), URIRef('http://example.org#knows'), URIRef('http://example.org#Bob')),
        (URIRef('http://example.org#Bob'), URIRef('http://example.org#knows'), URIRef('http://example.org#Charlie'))
    ]

    # Define the GraphDB URL and repository name
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'exampleRepository'

    # Add triples to the repository
    add_triples_to_repository(triples, graphdb_url, repository_name)
    ```
    """

    query = generate_insert_data_query(triples, named_graph_uri)
    gd.update_query(query, graphdb_url, repository_name)


def add_pref_and_hidden_labels_for_elements(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, pref_hidden_labels_ttl_file:str):
    """
    Adds preferred and hidden labels for the elements (name attribute versions and landmark) to a specified repository in GraphDB.

    Parameters:
    - graphdb_url (URIRef): The URL of the GraphDB instance that holds the repository where the labels will be added.
    - repository_name (str): The name of the repository where the labels will be inserted.
    - factoids_named_graph_uri (URIRef): The URI of the named graph containing the factoids from which the labels are generated.

    Returns:
    - None: The function does not return any value. It performs an update on the GraphDB repository by adding the triples.

    Description:
    This function retrieves elements with labels from the specified named graph, generates the triples for preferred and hidden labels,
    and then adds them to the specified repository.

    Example usage:
    ```python
    graphdb_url = URIRef('http://localhost:7200')
    repository_name = 'exampleRepository'
    factoids_named_graph_uri = URIRef('http://example.org/factoids')

    # Add preferred and hidden labels for name attribute versions
    add_pref_and_hidden_labels_for_elements(graphdb_url, repository_name, factoids_named_graph_uri)
    ```
    """

    elements = get_elements_with_labels(graphdb_url, repository_name, factoids_named_graph_uri)
    graph_with_triples_to_add = get_pref_and_hidden_label_triples_for_elements(elements)
    graph_with_triples_to_add.serialize(pref_hidden_labels_ttl_file)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, pref_hidden_labels_ttl_file, named_graph_uri=factoids_named_graph_uri)
    

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




def transfert_rdflib_graph_to_named_graph_repository(g: Graph, graphdb_url: URIRef, repository_name: str, named_graph_name: str, kg_file: str): 
    g.serialize(kg_file)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, kg_file, named_graph_name)
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
    import_factoids_in_facts(graphdb_url, 'factoidRepo', 'factoidsGraph', 'factsGraph', 'interSourcesGraph')
    ```
    """
    
    facts_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    factoids_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, factoids_named_graph_name)
    inter_sources_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, inter_sources_name_graph_name)

    # Addition of standardised and simplified labels for landmarks (on the factoid graph) in order to make links with fact landmarks
    add_pref_and_hidden_labels_for_elements(graphdb_url, repository_name, factoids_named_graph_uri, pref_hidden_labels_ttl_file)

    rr.link_factoids_with_facts(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
