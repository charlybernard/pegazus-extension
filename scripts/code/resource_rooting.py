from rdflib import URIRef, SKOS
from namespaces import NameSpaces
import graphdb as gd

np = NameSpaces()

######### Main function

# Function to rely all resources from `factoids_named_graph_uri` named graph to similar resources in `facts_named_graph_uri` (if they exists, else create the similar resource)
# Triple to tell similarity is store in `inter_sources_name_graph_uri`

def link_factoids_with_facts(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    """
    Landmarks are created as follows:
        * creation of links (using `addr:hasRoot`) between landmarks in the facts named graph and those which are in the factoid named graph ;
        * using inference rules, new `addr:hasRoot` links are deduced
        * for each resource defined in the factoids, we check whether it exists in the fact graph (if it is linked with a `addr:hasRoot` to a resource in the fact graph)
        * for unlinked factoid resources, we create its equivalent in the fact graph
    """

    make_rooting_for_landmarks(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
    make_rooting_for_landmark_relations(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
    make_rooting_for_landmark_attributes(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
    make_rooting_for_temporal_entities(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri)
    manage_labels_after_landmark_rooting(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
    

    # Les racines de modification sont créées sauf pour les modifications d'attributs.
    make_rooting_for_changes(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
    make_rooting_for_events(graphdb_url, repository_name, factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)

####################################################################

## Management of root elements
"""
This part includes functions for creating roots: each element of named graphs that include factoids must have an equivalent
in the factoid named graph. This equivalent is a root. A root can be the equivalent of several elements of several elements.
For example, if there is a ‘rue Gérard’ in several named graphs, they must be linked to the same root.
Roots apply to Landmark, LandmarkRelation, Attribute, AttributeVersion, Event, Change.
"""

########## Landmark

# Make rooting at landmarks level
# The way the rooting is made depends on the type of landmark

def make_rooting_for_landmarks(graphdb_url:URIRef, repository_name:str,
                                          factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    """
    Create `addr:hasRoot` links between similar landmarks.
    """

    label_property = SKOS.hiddenLabel

    landmark_type_uris = [np.LTYPE["Municipality"], np.LTYPE["District"], np.LTYPE["District"], np.LTYPE["PostalCodeArea"], np.LTYPE["Thoroughfare"]]
    for landmark_type_uri in landmark_type_uris:
        make_rooting_for_landmarks_according_label(graphdb_url, repository_name, landmark_type_uri, label_property,
                                                             factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)
        
    lm_and_lr_type_uris = [
        [np.LTYPE["HouseNumber"], np.LRTYPE["Belongs"]],
        [np.LTYPE["DistrictNumber"], np.LRTYPE["Belongs"]],
        [np.LTYPE["StreetNumber"], np.LRTYPE["Belongs"]],
    ]
    for elem in lm_and_lr_type_uris:
        lm_type_uri, lr_type_uri = elem
        make_rooting_for_landmarks_according_label_and_relation(graphdb_url, repository_name, lm_type_uri, lr_type_uri, label_property,
                                                             factoids_named_graph_uri, facts_named_graph_uri, inter_sources_name_graph_uri)

def make_rooting_for_landmarks_according_label(graphdb_url:URIRef, repository_name:str, landmark_type_uri:URIRef, label_property:URIRef,
                                                         factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    """
    Create roots and traces for landmark according a label criterion : a landmark is similar to a root landmark if they share the same label.
    `label_property` is the property for which the label is linked to the landmark (`rdfs:label`, `skos:hiddenLabel`, ...)
    """

    # Creating root landmark (if not exists) and rely it to the landmark
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?rootLandmark a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel . }}
        GRAPH ?gi {{
            ?landmark addr:hasRoot ?rootLandmark .
            ?rootLandmark addr:hasTrace ?landmark .
            
        }}
    }} WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        {{
            SELECT DISTINCT ?landmarkType ?keyLabel WHERE {{
                BIND({landmark_type_uri.n3()} AS ?landmarkType)
                ?l a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "LM_", STRUUID())) AS ?toCreateRootLandmark)
        OPTIONAL {{ GRAPH ?gf {{?existingRootLandmark a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .}}}}
        BIND(IF(BOUND(?existingRootLandmark), ?existingRootLandmark, ?toCreateRootLandmark) AS ?rootLandmark)
        GRAPH ?gs {{ ?landmark a addr:Landmark . }}
        ?landmark addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .
        MINUS {{ ?landmark addr:hasRoot ?rl . }}
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def make_rooting_for_landmarks_according_label_and_relation(graphdb_url:URIRef, repository_name:str,
                                                                      landmark_type_uri:URIRef, landmark_relation_type_uri:URIRef, label_property:URIRef,
                                                                      factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    """
    Create roots and traces for landmark according a label criterion : a landmark is similar to a root landmark if they share the same label and a the same kind of relation with the same landmark.
    This work wells with HouseNumber as the number is not enough to detect similarities, we need to get the landmark it belongs to.
    `label_property` is the property for which the label is linked to the landmark (`rdfs:label`, `skos:hiddenLabel`, ...)
    `landmark_relation_type_uri` describes the type of landmark relation (`lrtype:Belongs`, `ltype:Within`, ...) 
    """

    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?rootLandmark a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .
            ?rootLandmarkRelation a addr:LandmarkRelation ; addr:isLandmarkRelationType ?landmarkRelationType ; addr:locatum ?rootLandmark ; addr:relatum ?rootRelatum .
        }}
        GRAPH ?gi {{
            ?landmark addr:hasRoot ?rootLandmark .
            ?rootLandmark addr:hasTrace ?landmark .
            ?landmarkRelation addr:hasRoot ?rootLandmarkRelation .
            ?rootLandmarkRelation addr:hasTrace ?landmarkRelation .
        }}
    }}
    WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        BIND({landmark_type_uri.n3()} AS ?landmarkType)
        BIND({landmark_relation_type_uri.n3()} AS ?landmarkRelationType)
        {{
            SELECT DISTINCT ?landmarkType ?keyLabel ?landmarkRelationType ?rootRelatum WHERE {{
                ?lr a addr:LandmarkRelation ;
                addr:isLandmarkRelationType ?landmarkRelationType ;
                addr:locatum [a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel] ;
                addr:relatum [addr:hasRoot ?rootRelatum] .
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "LM_", STRUUID())) AS ?toCreateRootLandmark)
        BIND(URI(CONCAT(STR(URI(facts:)), "LR_", STRUUID())) AS ?toCreateRootLR)
        OPTIONAL {{
            GRAPH ?gf {{
                ?existingRootLandmark a addr:Landmark ; addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .
                ?existingRootLR a addr:LandmarkRelation ; addr:isLandmarkRelationType ?landmarkRelationType ;
                addr:locatum ?existingRootLandmark ; addr:relatum ?rootRelatum .
            }}
        }}
        BIND(IF(BOUND(?existingRootLandmark), ?existingRootLandmark, ?toCreateRootLandmark) AS ?rootLandmark)
        BIND(IF(BOUND(?existingRootLR), ?existingRootLR, ?toCreateRootLR) AS ?rootLandmarkRelation)
        GRAPH ?gs {{ ?landmark a addr:Landmark . }}
        ?landmark addr:isLandmarkType ?landmarkType ; {label_property.n3()} ?keyLabel .
        ?landmarkRelation a addr:LandmarkRelation ; addr:isLandmarkRelationType ?landmarkRelationType ;
        addr:locatum ?landmark ; addr:relatum [addr:hasRoot ?rootRelatum] .
        MINUS {{ ?landmark addr:hasRoot ?rl . }}
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)


########## Changes / Events

def make_rooting_for_changes(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    # Integration of changes in the fact graph (except for attribute changes, which are not unique)
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?rootChange a addr:Change ; addr:isChangeType ?changeType ; addr:appliedTo ?rootElem . }}
        GRAPH ?gi {{
            ?change addr:hasRoot ?rootChange .
            ?rootChange addr:hasTrace ?change .
        }}
    }} WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        {{
            SELECT DISTINCT ?changeType ?rootElem WHERE {{
                ?cg a addr:Change ; addr:isChangeType ?changeType ; addr:appliedTo [addr:hasRoot ?rootElem].
                MINUS {{ ?cg a addr:AttributeChange }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "CG_", STRUUID())) AS ?toCreateRootChange)
        OPTIONAL {{
            GRAPH ?gf {{ ?existingRootChange a addr:Change . }}
            ?existingRootChange addr:isChangeType ?changeType ; addr:appliedTo ?rootElem .
            }}
        BIND(IF(BOUND(?existingRootChange), ?existingRootChange, ?toCreateRootChange) AS ?rootChange)
        GRAPH ?gs {{ ?change a ?changeClass . }}
        ?changeClass rdfs:subClassOf addr:Change .
        ?change addr:isChangeType ?changeType ; addr:appliedTo [addr:hasRoot ?rootElem] .
        MINUS {{ ?change addr:hasRoot ?x . }}
    }}
    """
    gd.update_query(query, graphdb_url, repository_name)

def make_rooting_for_events(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    # Integration of events in the fact graph
    # If two events have at least one change in common, they are considered to be equal (a change depends on only one event).
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?rootEvent a addr:Event .
            ?rootChange addr:dependsOn ?rootEvent .
            }}
        GRAPH ?gi {{
            ?event addr:hasRoot ?rootEvent .
            ?rootEvent addr:hasTrace ?event .
        
        }}
    }} WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        {{
            SELECT DISTINCT ?rootChange WHERE {{
                ?ev a addr:Event ; addr:hasChange [addr:hasRoot ?rootChange] .
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "EV_", STRUUID())) AS ?toCreateRootEvent)
        OPTIONAL {{
            GRAPH ?gf {{?existingRootEvent a addr:Event . }}
            ?existingRootEvent addr:hasChange ?rootChange .
        }}
        BIND(IF(BOUND(?existingRootEvent), ?existingRootEvent, ?toCreateRootEvent) AS ?rootEvent)
        GRAPH ?gs {{ ?event a ?Event . }}
        ?event addr:hasChange [addr:hasRoot ?rootChange] .
        MINUS {{ ?event addr:hasRoot ?x . }}
    }}
    """
    gd.update_query(query, graphdb_url, repository_name)


########## Landmark relations

def make_rooting_for_landmark_relations(graphdb_url, repository_name, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    """
    Pour des relations entre repères dans le graphe nommé `factoids_named_graph_uri`, les lier avec une relation entre repères dans `facts_named_graph_uri` qui sont similaires (mêmes locatum, relatums et type de relation).
    Le lien créé est mis dans `factoids_facts_named_graph_uri`.
    """

    # Creation of a hiddenLabel for each LandmarkRelation in the (aggregation) fact graph. It is composed as follows: URI of the locatum + ‘&’ + ordered URIs of the relatums separated by a semicolon
    # For example, if a relationship has URILoc as its locatum and URIRel1 and URIRel2 as its relatums, the hidden label will be ‘URILoc1&URIRel1;URIRel2’.
    # We create this label for relationships that don't have one
    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{?lr skos:hiddenLabel ?hiddenLabel}}
        }} WHERE {{
            {{
                SELECT ?gf ?lr (CONCAT(STR(?rootLoc), "|", GROUP_CONCAT(STR(?rootRel); separator=";")) AS ?hiddenLabel) WHERE {{
                    BIND({facts_named_graph_uri.n3()} AS ?gf)
                    GRAPH ?gf {{ ?lr a addr:LandmarkRelation . }}
                    ?lr addr:relatum ?rootRel ; addr:locatum ?rootLoc .
                }}
                GROUP BY ?gf ?lr ?rootLoc ORDER BY ?rootRel
            }}
        }}
    """

    # We do the same thing for the relations in the factoid graph. We don't integrate the URIs of the locatums and relatums, but the URIs of their root in the fact graph.
    query2 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gi {{?lr skos:hiddenLabel ?hiddenLabel}}
        }} WHERE {{
            BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
            {{
                SELECT ?gs ?lr (CONCAT(STR(?rootLoc), "|", GROUP_CONCAT(STR(?rootRel); separator=";")) AS ?hiddenLabel) WHERE {{
                    BIND({factoids_named_graph_uri.n3()} AS ?gs)
                    GRAPH ?gs {{ ?lr a ?lrClass . }}
                    ?lrClass rdfs:subClassOf addr:LandmarkRelation .
                    ?lr addr:relatum [addr:hasRoot ?rootRel] ; addr:locatum [addr:hasRoot ?rootLoc] .
                }}
                GROUP BY ?gs ?lr ?rootLoc ORDER BY ?rootRel
            }}
        }}
    """

    query3 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{ ?rootLandmarkRelation a addr:LandmarkRelation ; addr:isLandmarkRelationType ?landmarkRelationType ; skos:hiddenLabel ?keyLabel . }}
            GRAPH ?gi {{
                ?landmarkRelation addr:hasRoot ?rootLandmarkRelation .
                ?rootLandmarkRelation addr:hasTrace ?landmarkRelation .
            }}
        }}
        WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
            BIND({factoids_named_graph_uri.n3()} AS ?gs)
            {{
                SELECT DISTINCT ?landmarkRelationType ?keyLabel WHERE {{
                    ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?landmarkRelationType ; skos:hiddenLabel ?keyLabel .
                }}
            }}
            BIND(URI(CONCAT(STR(URI(facts:)), "LR_", STRUUID())) AS ?toCreateRootLR)
            OPTIONAL {{
                GRAPH ?gf {{ ?existingRootLR a addr:LandmarkRelation }}
                ?existingRootLR skos:hiddenLabel ?keyLabel .
            }}
            BIND(IF(BOUND(?existingRootLR), ?existingRootLR, ?toCreateRootLR) AS ?rootLandmarkRelation)
            GRAPH ?gs {{ ?landmarkRelation a ?lrClass . }}
            ?lrClass rdfs:subClassOf addr:LandmarkRelation .
            ?landmarkRelation addr:isLandmarkRelationType ?landmarkRelationType ; skos:hiddenLabel ?keyLabel .
        }}
    """

    query4 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{ ?rootLandmarkRelation ?prop ?rootLandmark . }}
        }}
        WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            GRAPH ?gf {{ ?rootLandmarkRelation a addr:LandmarkRelation .}}
            ?lr addr:hasRoot ?rootLandmarkRelation ; ?prop [addr:hasRoot ?rootLandmark] .
            FILTER (?prop IN (addr:locatum, addr:relatum))
        }}
    """

    queries = [query1, query2, query3, query4]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

########## Atttibutes

def make_rooting_for_landmark_attributes(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    # Integration of changes in the fact graph (except for attribute changes, which are not unique)
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?rootAttr a addr:Attribute ; addr:isAttributeType ?attrType .
            ?rootLandmark addr:hasAttribute ?rootAttr .
        }}
        GRAPH ?gi {{
            ?attr addr:hasRoot ?rootAttr .
            ?rootAttr addr:hasTrace ?attr .
            }}
    }} WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)
        {{
            SELECT DISTINCT ?gf ?attrType ?rootLandmark ?rootAttr WHERE {{
                {{
                    SELECT DISTINCT ?gf ?attrType ?rootLandmark ?existingRootAttr WHERE {{
                        BIND({facts_named_graph_uri.n3()} AS ?gf)
                        ?landmark addr:hasRoot ?rootLandmark ; addr:hasAttribute [a addr:Attribute ; addr:isAttributeType ?attrType] . 
                        OPTIONAL {{
                            GRAPH ?gf {{ ?existingRootAttr a addr:Attribute . }}
                            ?existingRootAttr addr:isAttributeType ?attrType .
                            ?rootLandmark addr:hasAttribute ?existingRootAttr .
                        }}
                    }}
                }}
                BIND(IF(BOUND(?existingRootAttr), ?existingRootAttr, URI(CONCAT(STR(URI(facts:)), "ATTR_", STRUUID()))) AS ?rootAttr)
            }}
        }}

        GRAPH ?gs {{ ?attr a addr:Attribute . }}
        ?attr addr:isAttributeType ?attrType .
        ?landmark addr:hasRoot ?rootLandmark ; addr:hasAttribute ?attr .
        FILTER NOT EXISTS {{ ?attr addr:hasRoot ?x . }}
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

########## Temporal entities

def make_rooting_for_crisp_time_instants(graphdb_url:URIRef, repository_name:str,
                                        facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?rootTime a addr:CrispTimeInstant ; addr:timeStamp ?ts ; addr:timeCalendar ?tc ; addr:timePrecision ?tp . }}
        GRAPH ?gi {{
            ?time addr:hasRoot ?rootTime .
            ?rootTime addr:hasTrace ?time .
        }}
    }} WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        {{
            SELECT DISTINCT ?gf ?rootTime ?existingRootTime ?toCreateRootTime ?ts ?tc ?tp WHERE {{
                {{
                    SELECT DISTINCT ?gf ?existingRootTime ?ts ?tc ?tp {{
                        BIND({facts_named_graph_uri.n3()} AS ?gf)
                        GRAPH ?gs {{ ?time addr:timeStamp ?ts ; addr:timeCalendar ?tc ; addr:timePrecision ?tp .}}
                        OPTIONAL {{
                            GRAPH ?gf {{ ?existingRootTime addr:timeStamp ?ts ; addr:timeCalendar ?tc ; addr:timePrecision ?tp .}}
                        }}
                        FILTER (?gs != ?gf)
                    }}
                }}
                BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?toCreateRootTime)
                BIND(IF(BOUND(?existingRootTime), ?existingRootTime, ?toCreateRootTime) AS ?rootTime)
            }}
        }}
        GRAPH ?gs {{ ?time addr:timeStamp ?ts ; addr:timeCalendar ?tc ; addr:timePrecision ?tp .}}
        FILTER (?gs != ?gf)
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def make_rooting_for_crisp_time_intervals(graphdb_url:URIRef, repository_name:str,
                                          facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):

    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?rootTimeInt a addr:CrispTimeInterval ; addr:hasBeginning ?rootStartTime ; addr:hasEnd ?rootEndTime. }}
        GRAPH ?gi {{
            ?timeInt addr:hasRoot ?rootTimeInt .
            ?rootTimeInt addr:hasTrace ?timeInt .
        }}
    }} WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        {{
            SELECT DISTINCT ?gf ?rootTimeInt ?existingRootTimeInt ?toCreateRootTimeInt ?rootStartTime ?rootEndTime WHERE {{
                {{
                    SELECT DISTINCT ?gf ?existingRootTimeInt ?rootStartTime ?rootEndTime {{
                        BIND({facts_named_graph_uri.n3()} AS ?gf)
                        GRAPH ?gs {{ ?time addr:hasBeginning ?startTime ; addr:hasEnd ?endTime . }}
                        ?rootStartTime addr:hasTrace ?startTime .
                        ?rootEndTime addr:hasTrace ?endTime .
                        OPTIONAL {{ GRAPH ?gf {{?existingRootTimeInt addr:hasBeginning ?rootStartTime ; addr:hasEnd ?rootEndTime .}} }}
                        FILTER (?gs != ?gf)
                    }}
                }}
                BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?toCreateRootTimeInt)
                BIND(IF(BOUND(?existingRootTimeInt), ?existingRootTimeInt, ?toCreateRootTimeInt) AS ?rootTimeInt)
            }}
        }}
        GRAPH ?gs {{ ?timeInt addr:hasBeginning ?startTime ; addr:hasEnd ?endTime .}}
        FILTER (?gs != ?gf)
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def make_rooting_for_temporal_entities(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    make_rooting_for_crisp_time_instants(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri)
    make_rooting_for_crisp_time_intervals(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri)

###################################################### Other processes ######################################################

def manage_labels_after_landmark_rooting(graphdb_url:URIRef, repository_name:str, factoids_named_graph_uri:URIRef, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef):
    # Add a label for root landmarks which have been initialized after landmark rooting
    # If there is already a label (∃ <landmark rdfs:label label>), then add alternative labels if they exists
    # This query exists to get only one label per landmark, other labels are alt labels
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?rootLandmark rdfs:label ?rlLabel ; skos:altLabel ?rlAltLabel . }}
    }} WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({factoids_named_graph_uri.n3()} AS ?gs)

        GRAPH ?gi {{ ?rootLandmark addr:hasTrace ?landmark . }}
        OPTIONAL {{ ?rootLandmark rdfs:label ?rootLandmarkLabel . }}
        OPTIONAL {{ ?rootLandmark skos:altLabel ?rootLandmarkAltLabel . }}
        OPTIONAL {{ ?landmark rdfs:label ?landmarkLabel . }}
        OPTIONAL {{ ?landmark skos:prefLabel ?landmarkPrefLabel . }}

        BIND(IF(BOUND(?rootLandmarkLabel), ?rootLandmarkLabel,
                IF(BOUND(?landmarkPrefLabel), ?landmarkPrefLabel, ?landmarkLabel)
                ) AS ?rlLabel)

        BIND(IF(BOUND(?landmarkPrefLabel) && ?landmarkPrefLabel != ?rlLabel, ?landmarkPrefLabel, ?landmarkLabel) AS ?rlAltLabel)
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)
