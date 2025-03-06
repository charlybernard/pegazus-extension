import os
from rdflib import Graph, Literal, URIRef, SKOS
from namespaces import NameSpaces
import str_processing as sp
import graphdb as gd
import graphrdf as gr
import resource_rooting as rr

np = NameSpaces()

def add_pref_and_hidden_labels_to_landmarks(graphdb_url, repository_name, named_graph_uri:URIRef):
    add_pref_and_hidden_labels_for_name_attribute_versions(graphdb_url, repository_name, named_graph_uri)
    add_pref_and_hidden_labels_to_landmarks_from_name_attribute_versions(graphdb_url, repository_name, named_graph_uri)

def add_pref_and_hidden_labels_for_name_attribute_versions(graphdb_url, repository_name, factoids_named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
        SELECT ?av ?name ?ltype WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            GRAPH ?g {{ ?av a addr:AttributeVersion . }}
            ?av addr:versionValue ?name ;
                addr:isAttributeVersionOf [
                    a addr:Attribute ;
                    addr:isAttributeType atype:Name ;
                    addr:isAttributeOf [a addr:Landmark ; addr:isLandmarkType ?ltype]] .
        }}
        """

    results = gd.select_query_to_json(query, graphdb_url, repository_name)

    query_lines = ""
    for elem in results.get("results").get("bindings"):
        # Retrieval of URIs (attribute and attribute version) and geometry
        rel_av = gr.convert_result_elem_to_rdflib_elem(elem.get('av'))
        rel_name = gr.convert_result_elem_to_rdflib_elem(elem.get('name'))
        rel_landmark_type = gr.convert_result_elem_to_rdflib_elem(elem.get('ltype'))

        if rel_landmark_type == np.LTYPE["Thoroughfare"]:
            lm_label_type = "thoroughfare"
        elif rel_landmark_type in [np.LTYPE["Municipality"], np.LTYPE["District"]]:
            lm_label_type = "area"
        elif rel_landmark_type in [np.LTYPE["HouseNumber"],np.LTYPE["StreetNumber"],np.LTYPE["DistrictNumber"],np.LTYPE["PostalCodeArea"]]:
            lm_label_type = "housenumber"
        else:
            lm_label_type = None

        normalized_name, simplified_name = sp.normalize_and_simplify_name_version(rel_name.strip(), lm_label_type, rel_name.language)


        if normalized_name is not None:
            normalized_name_lit = Literal(normalized_name, lang=rel_name.language)
            query_lines += f"{rel_av.n3()} {SKOS.prefLabel.n3()} {normalized_name_lit.n3()}.\n"
        if simplified_name is not None:
            simplified_name_lit = Literal(simplified_name, lang=rel_name.language)
            query_lines += f"{rel_av.n3()} {SKOS.hiddenLabel.n3()} {simplified_name_lit.n3()}.\n"

    query = np.query_prefixes + f"""
        INSERT DATA {{
            GRAPH {factoids_named_graph_uri.n3()} {{
                {query_lines}
            }}
        }}
        """

    gd.update_query(query, graphdb_url, repository_name)

def add_pref_and_hidden_labels_to_landmarks_from_name_attribute_versions(graphdb_url, repository_name, named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?lm skos:prefLabel ?prefLabel ; skos:hiddenLabel ?hiddenLabel . }}
        }}
        WHERE {{
            BIND({named_graph_uri.n3()} AS ?g)
            GRAPH ?g {{ ?lm a addr:Landmark }}
            ?lm addr:hasAttribute [a addr:Attribute; addr:isAttributeType atype:Name ; addr:hasAttributeVersion ?av ] .
            OPTIONAL {{ ?av skos:prefLabel ?prefLabel . }}
            OPTIONAL {{ ?av skos:hiddenLabel ?hiddenLabel . }}
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def remove_all_triples_for_resources_to_remove(graphdb_url, repository_name):
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


def create_factoid_repository(graphdb_url, repository_name, tmp_folder, ont_file, ontology_named_graph_name, ruleset_name=None, disable_same_as=False, clear_if_exists=False):
    """
    Initialisation of a repository to create a factoids graph

    `clear_if_exists` is a bool to remove all statements if repository already exists"
    """

    local_config_file_name = f"config_for_{repository_name}.ttl"
    local_config_file = os.path.join(tmp_folder, local_config_file_name)
    # Repository creation
    gd.create_repository(graphdb_url, repository_name, local_config_file, ruleset_file=None, ruleset_name=ruleset_name, disable_same_as=disable_same_as)

    if clear_if_exists:
        gd.clear_repository(graphdb_url, repository_name)

    gd.add_prefixes_to_repository(graphdb_url, repository_name, np.namespaces_with_prefixes)
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, ont_file, ontology_named_graph_name)

def transfert_rdflib_graph_to_factoids_repository(graphdb_url:URIRef, repository_name:str, factoids_named_graph_name:str, g:Graph, kg_file:str, tmp_folder:str, ont_file:str, ontology_named_graph_name:str):
    g.serialize(kg_file)

    # Creating repository
    create_factoid_repository(graphdb_url, repository_name, tmp_folder,
                                ont_file, ontology_named_graph_name, ruleset_name="rdfsplus-optimized",
                                disable_same_as=False, clear_if_exists=True)

    # Import the `kg_file` file into the directory
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, kg_file, factoids_named_graph_name)


####################################################################


def import_factoids_in_facts(graphdb_url:URIRef, repository_name:str, factoids_named_graph_name:str, facts_named_graph_name:str, inter_sources_name_graph_name:str):
    facts_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    factoids_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, factoids_named_graph_name)
    inter_sources_name_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, inter_sources_name_graph_name)

    # Addition of standardised and simplified labels for landmarks (on the factoid graph) in order to make links with fact landmarks
    add_pref_and_hidden_labels_to_landmarks(graphdb_url, repository_name, factoids_named_graph_uri)

    rr.link_factoids_with_facts(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
