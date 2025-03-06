from rdflib import URIRef 
from namespaces import NameSpaces
import graphdb as gd

np = NameSpaces()

def transfert_factoids_to_facts_repository(graphdb_url, facts_repository_name, factoids_repository_name,
                                           factoids_ttl_file, permanent_ttl_file,
                                           factoids_repo_factoids_named_graph_name, factoids_repo_permanent_named_graph_name,
                                           facts_repo_factoids_named_graph_name, facts_repo_facts_named_graph_name):
    """
    Transfer factoids to facts graph
    """

    gd.export_data_from_repository(graphdb_url, factoids_repository_name, factoids_ttl_file, factoids_repo_factoids_named_graph_name)
    gd.export_data_from_repository(graphdb_url, factoids_repository_name, permanent_ttl_file, factoids_repo_permanent_named_graph_name)
    gd.import_ttl_file_in_graphdb(graphdb_url, facts_repository_name, factoids_ttl_file, facts_repo_factoids_named_graph_name)
    gd.import_ttl_file_in_graphdb(graphdb_url, facts_repository_name, permanent_ttl_file, facts_repo_facts_named_graph_name)

def transfert_immutable_triples(graphdb_url, repository_name, factoids_named_graph_uri, permanent_named_graph_uri):
    """
    All created triples are initially imported in factoids named graph.
    Some of them must be transfered in a permanent named graph, as they must not be modified while importing them in facts repository.
    """

    prefixes = np.query_prefixes + """
    PREFIX wb: <http://wikiba.se/ontology#>
    """

    # All triples whose predicate is `rico:isOrWasDescribedBy` are moved to permanent named graph
    query1 = prefixes + f"""
    DELETE {{
       ?s ?p ?o
    }}
    INSERT {{
        GRAPH {permanent_named_graph_uri.n3()} {{
            ?s ?p ?o.
        }}
    }}
    WHERE {{
        BIND(rico:isOrWasDescribedBy AS ?p)
        ?s ?p ?o.
    }} ;
    """

    # All triples whose subject is an URI and is a object of a triples whose predicate is `prov:wasDerivedFrom` are moved to permanent named graph
    query2 = prefixes + f"""
    DELETE {{
        GRAPH ?gf {{ ?prov ?p ?o }}
    }}
    INSERT {{
        GRAPH ?gp {{ ?prov ?p ?o }}
    }}
    WHERE
    {{
        BIND({factoids_named_graph_uri.n3()} AS ?gf)
        BIND({permanent_named_graph_uri.n3()} AS ?gp)
        GRAPH ?gf {{
            ?elem prov:wasDerivedFrom ?prov.
            ?prov ?p ?o.
        }}
    }}
    """

    # All triples whose subject is a Wikibase Item or Statement are moved to permanent named graph
    query3 = prefixes + f"""
    DELETE {{
        GRAPH ?gf {{ ?elem a ?type }}
    }}
    INSERT {{
        GRAPH ?gp {{ ?elem a ?type }}
    }}
    WHERE
    {{
        BIND({factoids_named_graph_uri.n3()} AS ?gf)
        BIND({permanent_named_graph_uri.n3()} AS ?gp)
        GRAPH ?gf {{
            ?elem a ?type.
        }}
        FILTER (?type in (wb:Item, wb:Statement))
    }}
    """

    queries = [query1, query2, query3]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def transfer_version_values_to_roots(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef):
    """
    Transfer attribute version values to root versions: if <?av addr:versionValue ?value> and <?rootAv addr:hasTrace ?av> then <?rootAv addr:versionValue ?value>.
    """

    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{ ?rootAttr addr:versionValue ?value }}
        }} WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            ?av addr:versionValue ?value ; addr:isTraceOf ?rootAttr .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def transfer_provenances_to_roots(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef):
    """
    Transférer les provenances (sources) des éléments vers leur racine
    """

    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{ ?rootElem prov:wasDerivedFrom ?provenance }}
        }} WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            ?elem prov:wasDerivedFrom ?provenance ; addr:isTraceOf ?rootElem .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def transfer_crisp_time_instant_elements_to_roots(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef):
    """
    Transfer the crisp time instant elements to their root.
    """

    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{ ?rootTime ?p ?timeElem }}
        }} WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            ?time ?p ?timeElem ; addr:isTraceOf ?rootTime .
            FILTER(?p IN (addr:timeStamp, addr:timeCalendar, addr:timePrecision))
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def transfer_elements_to_roots(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef):
    transfer_version_values_to_roots(graphdb_url, repository_name, facts_named_graph_uri)
    transfer_provenances_to_roots(graphdb_url, repository_name, facts_named_graph_uri)
    # transfer_crisp_time_instant_elements_to_roots(graphdb_url, repository_name, facts_named_graph_uri)