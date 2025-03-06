from rdflib import Graph, Literal, URIRef, Namespace, XSD
from namespaces import NameSpaces
import graphdb as gd
import graphrdf as gr
import geom_processing as gp
import multi_sources_processing as msp
import resource_initialisation as ri

np = NameSpaces()

## Creation sources

def create_source_resource(graphdb_url:URIRef, repository_name:str, source_uri:URIRef, source_label:str, publisher_label:str, lang:str, namespace:Namespace, named_graph_uri:URIRef):
    """
    Creation of the source for a resource
    """

    source_label_lit = Literal(source_label, lang=lang)
    query = np.query_prefixes + f"""
        INSERT DATA {{
            GRAPH {named_graph_uri.n3()} {{
                {source_uri.n3()} a rico:Record ; rdfs:label {source_label_lit.n3()} .
            }}
        }}
    """
    gd.update_query(query, graphdb_url, repository_name)

    if publisher_label is not None:
        publisher_uri = gr.generate_uri(namespace, "PUB")
        publisher_label_lit = Literal(publisher_label, lang=lang)
        query = np.query_prefixes + f"""
        INSERT DATA {{
            GRAPH {named_graph_uri.n3()} {{
                {source_uri.n3()} rico:hasPublisher {publisher_uri.n3()} .
                {publisher_uri.n3()} a rico:CorporateBody;
                    rdfs:label {publisher_label_lit.n3()}.
            }}
        }}
        """
        gd.update_query(query, graphdb_url, repository_name)

def link_provenances_with_source(graphdb_url:URIRef, repository_name:str, source_uri:URIRef, named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{
                ?prov rico:isOrWasDescribedBy ?sourceUri .
            }}
        }} WHERE {{
            BIND({named_graph_uri.n3()} AS ?g)
            BIND({source_uri.n3()} AS ?sourceUri)
            GRAPH ?g {{
                ?prov a prov:Entity .
            }}
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def detect_similar_landmarks_with_hidden_label_and_landmark_relation(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, landmark_type:URIRef, landmark_relation_type:URIRef, factoids_named_graph_uri:URIRef):
    # Detection of similar landmarks on the sole criterion of hiddenlabel similarity and belonging to the same landmark (they must have the same type)
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?landmark {similar_property.n3()} ?tmpLandmark . }}
        }}
        WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            {{
                SELECT DISTINCT ?hiddenLabel ?belongsLandmark {{
                    ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel .
                    ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType {landmark_relation_type.n3()}; addr:locatum ?tmpLandmark ; addr:relatum ?belongsLandmark .
                }}
            }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "LM_", STRUUID())) AS ?landmark)
        ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel.
        ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType {landmark_relation_type.n3()}; addr:locatum ?tmpLandmark ; addr:relatum ?belongsLandmark .
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def detect_similar_landmarks_with_hidden_label(graphdb_url, repository_name, similar_property:URIRef, landmark_type:URIRef, factoids_named_graph_uri:URIRef):
    # Detection of similar landmarks based solely on the hiddenlabel similarity criterion (they must have the same type)
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?landmark {similar_property.n3()} ?tmpLandmark . }}
        }}
        WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            {{
                SELECT DISTINCT ?hiddenLabel {{
                    ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel.
                }}
            }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "LM_", STRUUID())) AS ?landmark)
        ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel.
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def detect_similar_landmark_versions_with_hidden_label(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, landmark_type:URIRef, factoids_named_graph_uri:URIRef):
    # Detection of similar landmarks based solely on the hiddenlabel similarity criterion (they must have the same type).
    # To be considered as a version of a landmark, the landmark must be the subject of a triplet of the type `<?s addr:hasTime ?o>`.
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?landmark {similar_property.n3()} ?tmpLandmark . }}
        }}
        WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            {{
                SELECT DISTINCT ?hiddenLabel {{
                    ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel.
                }}
            }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "LM_", STRUUID())) AS ?landmark)
        ?tmpLandmark a addr:Landmark; addr:isLandmarkType {landmark_type.n3()} ; skos:hiddenLabel ?hiddenLabel ; addr:hasTime ?x.
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def detect_similar_attributes(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, factoids_named_graph_uri:URIRef):
    # Detection of similar attributes from the previous query
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{
                ?attr {similar_property.n3()} ?tmpAttr .
            }}
        }} WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            {{
                SELECT DISTINCT ?lm ?attrType WHERE {{
                    ?lm addr:hasAttribute [addr:isAttributeType ?attrType] .
                }}
            }}
            BIND(URI(CONCAT(STR(URI(factoids:)), "ATTR_", STRUUID())) AS ?attr)
            ?lm addr:hasAttribute ?tmpAttr .
            ?tmpAttr addr:isAttributeType ?attrType .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)


def detect_similar_attribute_versions(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, factoids_named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{
                ?av {similar_property.n3()} ?tmpAv .
            }}
        }} WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?g)
            {{
                SELECT DISTINCT ?attr ?versionValue WHERE {{
                    ?attr addr:hasAttributeVersion [addr:versionValue ?versionValue] .
                }}
            }}
            BIND(URI(CONCAT(STR(URI(factoids:)), "AV_", STRUUID())) AS ?av)
            ?attr addr:hasAttributeVersion ?tmpAv .
            ?tmpAv addr:versionValue ?versionValue .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def detect_similar_landmark_relations(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, factoids_named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gs {{
            ?lr1 {similar_property.n3()} ?lr2 .
        }}
    }}
    WHERE {{
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        ?lr1 a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrtype ; addr:locatum ?loc ; addr:relatum ?rel .
        ?lr2 a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrtype ; addr:locatum ?loc ; addr:relatum ?rel .
        FILTER (!sameTerm(?lr1, ?lr2))
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def merge_similar_landmarks_with_hidden_labels(graphdb_url:URIRef, repository_name:str, landmark_type:URIRef, factoids_named_graph_uri:URIRef):
    similar_property = np.SKOS["exactMatch"]

    # Detection and merging of similar landmarks
    detect_similar_landmarks_with_hidden_label(graphdb_url, repository_name, similar_property, landmark_type, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attributes
    detect_similar_attributes(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attribute versions
    detect_similar_attribute_versions(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

def merge_similar_landmark_versions_with_hidden_labels(graphdb_url:URIRef, repository_name:str, landmark_type:URIRef, factoids_named_graph_uri:URIRef):
    similar_property = np.SKOS["exactMatch"]

    # Detection and merging of similar landmarks
    detect_similar_landmark_versions_with_hidden_label(graphdb_url, repository_name, similar_property, landmark_type, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attributes
    detect_similar_attributes(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attribute versions
    detect_similar_attribute_versions(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

def merge_similar_landmarks_with_hidden_label_and_landmark_relation(graphdb_url:URIRef, repository_name:str, landmark_type:URIRef, landmark_relation_type:URIRef, factoids_named_graph_uri:URIRef):
    similar_property = np.SKOS["exactMatch"]

    # Detection and merging of similar landmarks
    detect_similar_landmarks_with_hidden_label_and_landmark_relation(graphdb_url, repository_name, similar_property, landmark_type, landmark_relation_type, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attributes
    detect_similar_attributes(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

    # Detection and merging of similar attribute versions
    detect_similar_attribute_versions(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

def merge_similar_landmark_relations(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef):
    similar_property = np.SKOS["exactMatch"]

    # Detection and merging of similar landmark relations
    detect_similar_landmark_relations(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)
    remove_temporary_resources_and_transfert_triples(graphdb_url, repository_name, similar_property, factoids_named_graph_uri)

def merge_similar_crisp_time_instants(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef):
    """
    Merge all crisp time instants which are similar, ie they are same time stamp, same calendar and same precision.
    """
    to_remove_property = np.ADDR["toRemove"]

    query = np.query_prefixes + f"""
        DELETE {{
            ?s ?p ?time .
        }}
        INSERT {{
            GRAPH ?gs {{
                ?s ?p ?newTime .
                ?newTime a addr:CrispTimeInstant ; addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCalendar ; addr:timePrecision ?timePrecision .
                ?time {to_remove_property.n3()} "true"^^xsd:boolean .
            }}
        }}
        WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?gs)
            {{
                SELECT DISTINCT ?timeStamp ?timeCalendar  ?timePrecision WHERE {{
                    ?time a addr:TimeInstant ; addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCalendar ; addr:timePrecision ?timePrecision .
                }}
            }}
            BIND(URI(CONCAT(STR(URI(factoids:)), "TI_", STRUUID())) AS ?newTime)
            ?time a addr:TimeInstant ; addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCalendar ; addr:timePrecision ?timePrecision .
            ?s ?p ?time .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def merge_similar_time_intervals(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef):
    """
    Merge all time interval which are similar, ie they have the same borders
    """
    to_remove_property = np.ADDR["toRemove"]

    query = np.query_prefixes + f"""
        DELETE {{
            ?s ?p ?timeInterval .
        }}
        INSERT {{
            GRAPH ?gs {{
                ?s ?p ?newTimeInterval .
                ?newTimeInterval addr:hasBeginning ?startTime ; addr:hasEnd ?endTime .
                ?timeInterval {to_remove_property.n3()} "true"^^xsd:boolean .
            }}
        }}
        WHERE {{
            BIND({factoids_named_graph_uri.n3()} AS ?gs)
            {{
                SELECT DISTINCT ?startTime ?endTime WHERE {{
                    ?timeInterval a addr:TimeInterval ; addr:hasBeginning ?startTime ; addr:hasEnd ?endTime .
                }}
            }}
            BIND(URI(CONCAT(STR(URI(factoids:)), "TI_", STRUUID())) AS ?newTimeInterval)
            ?timeInterval a addr:TimeInterval ; addr:hasBeginning ?startTime ; addr:hasEnd ?endTime .
            ?s ?p ?timeInterval .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def merge_similar_temporal_entities(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef):
    merge_similar_crisp_time_instants(graphdb_url, repository_name, factoids_named_graph_uri)
    merge_similar_time_intervals(graphdb_url, repository_name, factoids_named_graph_uri)
    msp.remove_all_triples_for_resources_to_remove(graphdb_url, repository_name)


def remove_temporary_resources_and_transfert_triples(graphdb_url:URIRef, repository_name:str, similar_property:URIRef, named_graph_uri:str):
    """
    Deletion of temporary resources and transfer of all their triplets to their associated resource (such as `<?resource skos:exactMatch ?temporaryResource>`).
    """
    query = np.query_prefixes + f"""
    DELETE {{
        GRAPH ?g {{
            ?s ?p ?tmpResource.
            ?tmpResource ?p ?o.
        }}
    }}
    INSERT {{
        GRAPH ?g {{
            ?s ?p ?resource.
            ?resource ?p ?o.
        }}
    }}
    WHERE {{
        ?resource {similar_property.n3()} ?tmpResource.
        GRAPH ?g {{
            {{?tmpResource ?p ?o}} UNION {{?s ?p ?tmpResource}}
          }}
    }} ;

    DELETE {{
        ?resource {similar_property.n3()} ?tmpResource.
    }}
    WHERE {{
        BIND({named_graph_uri.n3()} AS ?g)
        GRAPH ?g {{
            ?resource {similar_property.n3()} ?tmpResource.
        }}
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def merge_landmark_multiple_geometries(graphdb_url, repository_name, factoids_named_graph_uri, geom_kg_file):
    """
    Merge the geometries of a landmark if it has more than one
    """

    to_remove_property = np.ADDR["toRemove"]

    # Query to select all landmark geometries
    query = np.query_prefixes + """
        SELECT DISTINCT * WHERE {
            ?attr addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion .
            ?attrVersion addr:versionValue ?geom .
            }
        """
    results = gd.select_query_to_json(query, graphdb_url, repository_name)

    attr_geom_values = {}

    for elem in results.get("results").get("bindings"):
        # Recovery of URIs (attribute and attribute version) and geometry
        rel_attr = gr.convert_result_elem_to_rdflib_elem(elem.get('attr'))
        rel_attr_version = gr.convert_result_elem_to_rdflib_elem(elem.get('attrVersion'))
        rel_geom = gr.convert_result_elem_to_rdflib_elem(elem.get('geom'))

        if rel_attr in attr_geom_values.keys():
            attr_geom_values[rel_attr].append([rel_attr_version, rel_geom])
        else:
            attr_geom_values[rel_attr] = [[rel_attr_version, rel_geom]]

    # Add a version of geometry which is the result of merging all the versions linked to an attribute.
    # Indicate for each initial version that it must be deleted.
    g = Graph()
    for attr_uri, versions in attr_geom_values.items():
        if len(versions) > 1:
            geoms = [version[1] for version in versions]
            wkt_literal = gp.get_union_of_geosparql_wktliterals(geoms)
            attr_version_uri = gr.generate_uri(np.FACTOIDS, "AV")
            ri.create_attribute_version(g, attr_version_uri, wkt_literal)
            ri.add_version_to_attribute(g, attr_uri, attr_version_uri)
            for version in versions:
                g.add((version[0], to_remove_property, Literal("true", datatype=XSD.boolean)))

    # Export the graph to the `kg_file` file, which is imported into the
    g.serialize(geom_kg_file)
    gd.import_ttl_file_in_graphdb(graphdb_url, repository_name, geom_kg_file, named_graph_uri=factoids_named_graph_uri)

    msp.remove_all_triples_for_resources_to_remove(graphdb_url, repository_name)