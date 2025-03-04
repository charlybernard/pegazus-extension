import os
from rdflib import Graph, Namespace, Literal, URIRef, XSD, SKOS
from namespaces import NameSpaces
import str_processing as sp
import geom_processing as gp
import graphdb as gd
import graphrdf as gr
import resource_rooting as rr
import resource_transfert as rt

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



#####################################################################################################################

# Get the appearance and disappearance of landmark

def initialize_missing_changes_and_events_for_landmarks(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri):

    gregorian_calendar_uri = URIRef("http://www.wikidata.org/entity/Q1985727")

   # Add missing landmark changes after having imported all factoids
    # All landmarks must be related with two changes :
    # - one which describes its apprearance
    # - an other one which describes its disappearance (even if this landmark still exists)
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?missingChange a addr:LandmarkChange ; addr:isChangeType ?changeType ; addr:appliedTo ?lm ; addr:dependsOn ?missingEvent .
            ?missingEvent a addr:Event .
        }}
    }} WHERE {{
        {{
            SELECT * WHERE {{
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                VALUES ?changeType {{ ctype:LandmarkAppearance ctype:LandmarkDisappearance }}
                GRAPH ?gf {{ ?lm a addr:Landmark . }}

                FILTER NOT EXISTS {{
                    ?change addr:isChangeType ?changeType ; addr:appliedTo ?lm .
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "CG_", STRUUID())) AS ?missingChange)
        BIND(URI(CONCAT(STR(URI(facts:)), "EV_", STRUUID())) AS ?missingEvent)
    }}
    """

    # For each event in facts named graph which are not related to any time, rely it with all possible times in a temporary named graph
    # Possible times are all times related to their event traces or landmark valid time (startTime if it is a event related to a LandmarkAppearance, endTime if it is a event related to a LandmarkDisappearance)
    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?event ?propInstantTime ?time .
        }}
    }}
    WHERE {{
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        GRAPH ?gf {{ ?lm a addr:Landmark . }}
        ?lm addr:hasTrace ?lmTrace .
        ?change addr:isChangeType ?changeType ; addr:appliedTo ?lm ; addr:dependsOn ?event .
        {{
            VALUES (?changeType ?propIntervalTime ?propInstantTime) {{
                (ctype:LandmarkAppearance addr:hasBeginning addr:hasTimeBefore)
                (ctype:LandmarkDisappearance addr:hasEnd addr:hasTimeAfter)
            }}
            ?lmTrace addr:hasTime [?propIntervalTime ?time ] .
        }} UNION {{
            ?changeTrace addr:isChangeType ?changeType ; addr:appliedTo ?lmTrace ; addr:dependsOn ?eventTrace .
            ?eventTrace ?propInstantTime ?time .
            FILTER (?propInstantTime IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        }}
        FILTER NOT EXISTS {{ GRAPH ?gf {{ ?event addr:hasTime ?t }} }}
    }}
    """

    # Select the best time(s) from the sources to be the trace of the time of each event related to the appearance or disappearance of landmarks
    # First query get the precise time of each event (if it exists) : <?event addr:hasTime ?time>
    # Second one give a time estimation for event which does not have any precise time (according first query) : <?event addr:hasTimeBefore ?time> and/or <?event addr:hasTimeAfter ?time>
    query3a = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?time a addr:CrispTimeInstant .
            ?event ?propTime ?time .
        }}
        GRAPH ?gi {{ ?time addr:hasTrace ?timeTrace . }}
    }}
    WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        {{
            SELECT DISTINCT ?gf ?gt ?propTime ?timeCal ?event ?diffTime WHERE {{
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                BIND({gregorian_calendar_uri.n3()} AS ?timeCal)
                BIND(addr:hasTime AS ?propTime)
                GRAPH ?gf {{ ?lm a addr:Landmark . }}
                ?change addr:dependsOn ?event ; addr:appliedTo ?lm .
                GRAPH ?gt {{ ?event ?propTime ?time . }}
                ?time addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
                BIND(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) AS ?diffTime)
                FILTER NOT EXISTS {{ GRAPH ?gf {{ ?event addr:hasTime ?t }}}}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?time)
        ?event ?propTime ?timeTrace .
        ?timeTrace addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
        FILTER(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) = ?diffTime)
    }}
    """

    query3b = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?time a addr:CrispTimeInstant .
            ?event ?propTime ?time .
        }}
        GRAPH ?gi {{ ?time addr:hasTrace ?timeTrace . }}
    }}
    WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        {{
            SELECT DISTINCT ?gf ?gt ?propTime ?timeCal ?event (MIN(?diffTime) AS ?diffTimeMin) (MAX(?diffTime) AS ?diffTimeMax) WHERE {{
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                BIND({gregorian_calendar_uri.n3()} AS ?timeCal)
                VALUES ?propTime {{ addr:hasTimeBefore addr:hasTimeAfter }}
                GRAPH ?gf {{ ?lm a addr:Landmark . }}
                ?change addr:dependsOn ?event ; addr:appliedTo ?lm .
                GRAPH ?gt {{ ?event ?propTime ?time . }}
                ?time addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
                BIND(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) AS ?diffTime)
                FILTER NOT EXISTS {{ GRAPH ?gf {{ ?event addr:hasTime ?t }}}}
            }}
            GROUP BY ?gf ?gt ?propTime ?timeCal ?event
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?time)
        BIND(IF(?propTime = addr:hasTimeBefore, ?diffTimeMin, ?diffTimeMax) AS ?diffTime)
        ?event ?propTime ?timeTrace .
        ?timeTrace addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
        FILTER(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) = ?diffTime)
    }}

    """

    queries = [query1, query2, query3a, query3b]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

    gd.remove_named_graph_from_uri(tmp_named_graph_uri)
    rt.transfer_elements_to_roots(graphdb_url, repository_name, facts_named_graph_uri)

#####################################################################################################################

# Construction of the evolution from states (versions) and events (changes)

def get_elementary_divisions_of_versions_and_changes(graphdb_url:URIRef, repository_name:str, tmp_named_graph_uri:URIRef):
    # For each attribute, create as many TimeDescription object as there are temporal values related to it
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?rootAttr addr:hasTimeDescription [a addr:TimeDescription ; addr:hasTime ?time ; addr:hasTimeType ?timeType ; addr:hasSimplifiedTime ?simplifiedTime ; addr:hasRelatedElem ?attrElem ] .
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?g)
        ?rootAttr a addr:Attribute ; addr:hasTrace ?attr .
        {{
            ?lm a addr:Landmark ; addr:hasAttribute ?attr ; addr:hasTime [?propTime ?time ] .
            ?attr addr:hasAttributeVersion ?attrElem .
            FILTER(?propTime IN (addr:hasBeginning, addr:hasEnd))
            BIND(IF(?propTime = addr:hasBeginning, "start", "end") AS ?timeType)
        }} UNION {{
            ?attrElem a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?time] .
            BIND("null" AS ?timeType)
        }}
        ?time addr:timeStamp ?timeStamp .
        BIND(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) AS ?simplifiedTime)
    }}
    """

    # For each attribute, detect duplicate time values and create a list of changes without doublons
    query2 =  np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?time a addr:TemporalEntity ; addr:hasSimplifiedTime ?st ; addr:hasTrace ?timeTrace .
            ?event a addr:Event ; addr:hasTime ?time .
            ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?event ; addr:isDerivedFrom ?timeDescription .
        }}
    }} WHERE {{
        {{
            SELECT DISTINCT ?g ?attr ?st WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?g)
                GRAPH ?g {{ ?attr addr:hasTimeDescription [addr:hasSimplifiedTime ?st] }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "TI_", STRUUID())) AS ?time)
        BIND(URI(CONCAT(STR(URI(factoids:)), "EV_", STRUUID())) AS ?event)
        BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?change)
        GRAPH ?g {{
            ?attr addr:hasTimeDescription ?timeDescription .
            ?timeDescription addr:hasSimplifiedTime ?st ; addr:hasTime ?timeTrace .
        }}
    }}
    """

    # Order changes
    query3 =  np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{ ?cg addr:hasNextChange ?cgBis . }}
    }}
    WHERE {{
        {{
            SELECT ?g ?attr ?t (MIN(?diffTime) AS ?minDiffTime) WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?g)
                ?cg addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?t].
                ?cgBis addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?tBis].
                ?t addr:hasSimplifiedTime ?st .
                ?tBis addr:hasSimplifiedTime ?stBis .
                BIND(?stBis - ?st AS ?diffTime)
                FILTER(?t != ?tBis && ?diffTime > 0)
            }}
            GROUP BY ?g ?attr ?t
        }}
        
        ?cg addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?t].
        ?cgBis addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?tBis].
        ?t addr:hasSimplifiedTime ?st .
        ?tBis addr:hasSimplifiedTime ?stBis .
        FILTER(?stBis - ?st = ?minDiffTime)
    }}
    """

    # Create fake changes (related to -inf and +inf temporal values) 
    query4 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?newEvent a addr:Event .
            ?newChange a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?newEvent .
            ?prevChange addr:hasNextChange ?nextChange .
        }} 
    }} WHERE {{
        {{
            SELECT DISTINCT ?g ?attr ?cg ?firstChangeMissing WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?g)
                GRAPH ?g {{ ?cg addr:appliedTo ?attr . }}
                ?attr a addr:Attribute .
                {{
                    FILTER NOT EXISTS {{ ?cg addr:hasNextChange ?x }}
                    BIND("false"^^xsd:boolean AS ?firstChangeMissing)
                }} UNION {{
                    FILTER NOT EXISTS {{ ?x addr:hasNextChange ?cg }}
                    BIND("true"^^xsd:boolean AS ?firstChangeMissing)
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?newChange)
        BIND(URI(CONCAT(STR(URI(factoids:)), "EV_", STRUUID())) AS ?newEvent)
        BIND(IF(?firstChangeMissing, ?newChange, ?cg)  AS ?prevChange)
        BIND(IF(?firstChangeMissing, ?cg, ?newChange)  AS ?nextChange)
    }}
    """

    # Create versions between two successive changes (one makes effective the version while the other outdates it)
    # Get an explicit triple to have successive changes (`?cg1 addr:hasNextChange ?cg2`)
    query5 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?attr addr:hasAttributeVersion ?vers .
            ?vers a addr:AttributeVersion .
            ?cg1 addr:makesEffective ?vers .
            ?cg2 addr:outdates ?vers .
        }}
    }} WHERE {{
        {{
            SELECT DISTINCT ?g ?attr ?cg1 ?cg2 WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?g)
                ?attr a addr:Attribute .
                GRAPH ?g {{
                    ?cg1 addr:appliedTo ?attr .
                    ?cg2 addr:appliedTo ?attr .
                    ?cg1 addr:hasNextChange ?cg2 .
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "AV_", STRUUID())) AS ?vers)
    }}
    """

    # Link existing attribute changes with created one when the are related
    query6 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{ ?cg addr:hasTrace ?cgTrace . }}
    }} WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?g)
        ?attr a addr:Attribute .
        ?cgTrace a addr:AttributeChange .
        GRAPH ?g {{
            ?cg addr:appliedTo ?attr ; addr:isDerivedFrom [addr:hasRelatedElem ?cgTrace] .
        }}
    }}
    """

    # Link existing attribute versions related to changes with created one when the are related
    query7 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{ ?vers addr:hasTrace ?versTrace . }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?g)
        ?attr a addr:Attribute .
        ?cgTrace ?changeProp ?versTrace .
        GRAPH ?g {{
            VALUES ?changeProp {{ addr:makesEffective addr:outdates }}
            ?cg a addr:AttributeChange ; addr:appliedTo ?attr ; addr:hasTrace ?cgTrace ; ?changeProp ?vers.
        }}
    }}
    """

    # For each version, get changes which makes effective and outdates it (for query9)
    query8 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?versTrace ?changeProp ?cg .
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?g)
        ?attr a addr:Attribute .
        ?versTrace a addr:AttributeVersion .
        GRAPH ?g {{
            VALUES (?timeType ?changeProp) {{ ("start" addr:isMadeEffectiveBy) ("end" addr:isOutdatedBy) }}
            ?cg addr:appliedTo ?attr ; addr:isDerivedFrom [addr:hasRelatedElem ?versTrace ; addr:hasTimeType ?timeType] .
        }}
    }}
    """

    query9 = np.query_prefixes + f"""
    DELETE {{
        GRAPH ?g {{ ?versTrace addr:isMadeEffectiveBy ?cg1 ; addr:isOutdatedBy ?cg2 . }}
    }}
    INSERT {{
        GRAPH ?g {{ ?vers addr:hasTrace ?versTrace }}  
    }} WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?g)
        ?attr a addr:Attribute .
        ?versTrace a addr:AttributeVersion .
        GRAPH ?g {{
            ?cg1 addr:appliedTo ?attr .
            ?cg2 addr:appliedTo ?attr .
            ?versTrace addr:isMadeEffectiveBy ?cg1 ; addr:isOutdatedBy ?cg2 .
            {{
                ?cg1 addr:makesEffective ?vers .
            }} UNION {{
                ?cg2 addr:outdates ?vers .
            }} UNION {{
                ?cg1 addr:hasNextChange+ ?c .
                ?c addr:hasNextChange+ ?cg2 .
                ?c addr:makesEffective|addr:outdates ?vers .
            }}
        }}
    }}
    """
    
    queries = [query1, query2, query3, query4, query5, query6, query7, query8, query9]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)


def remove_empty_attribute_versions(graphdb_url:URIRef, repository_name:str, tmp_named_graph_uri:URIRef):
    """
    Remove empty attribute versions, ie versions which don't have any trace (∄ ?version addr:hasTrace ?versionTrace), excepted if this version is made effective AND outdated by two changes which have traces.
    Let's take a version named ?version. ∃ (?changeME, ?changeO), ?changeME addr:makesEffective ?version && ∄ ?changeO addr:hasTrace ?version.
    If ∄ ?version addr:hasTrace ?versionTrace:
    * (a) if ∃ (?changeMETrace, ?changeOTrace), ?changeME addr:hasTrace ?changeMETrace && ?changeO addr:hasTrace ?changeOTrace -> ø
    * (b) if ∃ ?changeME, ?changeME addr:hasTrace ?changeMETrace && ∄ ?changeO, ?changeO addr:hasTrace ?changeOTrace -> remove ?version and ?changeO
    * (c) if ∄ ?changeME, ?changeME addr:hasTrace ?changeMETrace && ∃ ?changeO, ?changeO addr:hasTrace ?changeOTrace -> remove ?version and ?changeME
    * (d) if ∄ ?changeME, ?changeME addr:hasTrace ?changeMETrace && ∄ ?changeO, ?changeO addr:hasTrace ?changeOTrace -> remove ?version, ?changeME and ?changeO

    The subquery selects all the empty versions to be removed and get their related changes.
    ?hasChangeMETrace and ?hasChangeOTrace are boolean to know if these changes have traces for the query to know which case the version belongs to (a, b, c or d).
    """

    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gt {{
                ?toRemoveChangeME addr:toRemove "true"^^xsd:boolean .
                ?toRemoveChangeO addr:toRemove "true"^^xsd:boolean .
                ?version addr:toRemove "true"^^xsd:boolean .
                ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?event ; addr:makesEffective ?vME ; addr:outdates ?vO.
                ?event a addr:Event ; addr:hasTimeAfter ?timeME ; addr:hasTimeBefore ?timeO .
                }}
        }}
        WHERE {{
            {{
                SELECT DISTINCT ?gt ?attr ?version ?changeME ?changeO ?hasChangeMETrace ?hasChangeOTrace WHERE {{
                    BIND({tmp_named_graph_uri.n3()} AS ?gt)
                    GRAPH ?gt {{
                        ?attr addr:hasAttributeVersion ?version .
                        ?version a addr:AttributeVersion .
                        ?changeME a addr:AttributeChange ; addr:makesEffective ?version .
                        ?changeO a addr:AttributeChange ; addr:outdates ?version .
                    }}
                    FILTER NOT EXISTS {{?version addr:hasTrace ?versionTrace .}}
                    OPTIONAL {{ ?changeME addr:hasTrace ?changeMETrace . }}
                    OPTIONAL {{ ?changeO addr:hasTrace ?changeOTrace . }}
                    BIND(IF(BOUND(?changeMETrace), "true"^^xsd:boolean, "false"^^xsd:boolean) AS ?hasChangeMETrace)
                    BIND(IF(BOUND(?changeOTrace), "true"^^xsd:boolean, "false"^^xsd:boolean) AS ?hasChangeOTrace)
                    FILTER(!(?hasChangeMETrace && ?hasChangeOTrace))
                }} 
            }}

            ?changeME addr:dependsOn ?eventME .
            ?changeO addr:dependsOn ?eventO .

            BIND(URI(CONCAT(STR(URI(factoids:)), "EV_", STRUUID())) AS ?newEvent)
            BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?newChange)

            BIND(IF(!?hasChangeMETrace && !?hasChangeOTrace, ?newChange, IF(!?hasChangeMETrace, ?changeO, ?changeME)) AS ?change)
            BIND(IF(!?hasChangeMETrace && !?hasChangeOTrace, ?newEvent, IF(!?hasChangeMETrace, ?eventO, ?eventME)) AS ?event)

            OPTIONAL {{
                ?eventME addr:hasTime ?timeME .
                FILTER(!?hasChangeMETrace)
            }}
            OPTIONAL {{
                ?eventO addr:hasTime ?timeO .
                FILTER(!?hasChangeOTrace)
            }}
            OPTIONAL {{
                ?changeO addr:makesEffective ?vME .
                FILTER(!?hasChangeOTrace)
            }}
            OPTIONAL {{
                ?changeME addr:outdates ?vO .
                FILTER(!?hasChangeMETrace)
            }}
            OPTIONAL {{
                BIND(?changeME AS ?toRemoveChangeME)
                FILTER(!?hasChangeMETrace)
            }}
            OPTIONAL {{
                BIND(?changeO AS ?toRemoveChangeO)
                FILTER(!?hasChangeOTrace)
            }}
        }}
        """
    
    gd.update_query(query, graphdb_url, repository_name)

    # Remove all triples where resources r for which it exists a triple <r addr:toRemove "true"^^xsd:boolean> is in these triples
    # In this case, remove selected versions and their related changes which are not traced
    remove_all_triples_for_resources_to_remove(graphdb_url, repository_name)

# Get attribute versions to merge
def to_be_merged_with(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    """
    Get attribute versions to merge :
    * a version has to be merged with itself ;
    * if an untraced change (a change ?cg such as ∄ ?cg addr:hasTrace ?cgTrace) makesEffective ?vME, outdates ?vO and ?vME has same version value as ?vO then ?vME has to be merged with ?vO
    * transitivity: if v1 addr:toBeMergedWith v2 and v2 addr:toBeMergedWith v3 then v1 addr:toBeMergedWith v3.
    
    """
    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gt {{
                ?vers addr:toBeMergedWith ?vers .
            }}
        }} WHERE {{
            BIND({tmp_named_graph_uri.n3()} AS ?gt)
            ?vers a addr:AttributeVersion .
        }}
    """

    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?vME addr:toBeMergedWith ?vO .
            ?vO addr:toBeMergedWith ?vME . 
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        ?change a addr:AttributeChange ; addr:makesEffective ?vME ; addr:outdates ?vO .
        FILTER NOT EXISTS {{ ?change addr:hasTrace ?changeTrace . }}
        ?vME addr:hasTrace ?vMETrace .
        ?vO addr:hasTrace ?vOTrace .
        {{ ?vMETrace addr:sameVersionValueAs ?vOTrace . }} UNION {{ FILTER(sameTerm(?vMETrace, ?vOTrace)) }}
        MINUS {{
            ?vME addr:hasTrace ?vMETrace2 .
            ?vO addr:hasTrace ?vOTrace2 .
            ?vMETrace2 addr:differentVersionValueFrom ?vOTrace2 .
        }}
    }}
    """

    # Aggregation of successive versions with similar values (in several queries)
    # Add triples indicating similarity (addr:toBeMergedWith) with successive versions that have similar values (addr:hasNextVersion or addr:hasOverlappingVersion)
    # If v1 addr:toBeMergedWith v2 and v2 addr:toBeMergedWith v3 then v1 addr:toBeMergedWith v3.
    query3 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gt {{ ?attrVers1 addr:toBeMergedWith ?attrVers2 . }}
        }} WHERE {{
            BIND({tmp_named_graph_uri.n3()} AS ?gt)
            ?attrVers1 addr:toBeMergedWith+ ?attrVers2 .
        }}
    """
    
    queries = [query1, query2, query3]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def merge_attribute_versions_to_be_merged(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    """
    It may be more than two versions are similar to each other. To detect all the similar versions, we will associate them with a mergedVal constructed from the URIs of the similar versions.
    So if v1 is similar to v2, v3 and v4, the mergedVal will be ‘uriV1;uriV2;uriV3;uriV4’ where uriVi is the URI of version i. v2, v3 and v4 will have the same mergedVal.
    Triple created will then be <v1 addr:hasMergedVal ‘uriV1;uriV2;uriV3;uriV4’>.
    This step is done with `query3`.
    """
    
    # For each version, we create a value (versMergeVal) which is the fusion of the URIs of versions that are similar.
    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gt {{ ?vers1 addr:versMergeVal ?versMergeVal }}
        }} WHERE {{
            BIND({tmp_named_graph_uri.n3()} AS ?gt)
            {{
                SELECT ?vers1 (GROUP_CONCAT(STR(?vers2) ; separator="|") as ?versMergeVal) WHERE {{
                    ?vers1 addr:toBeMergedWith ?vers2 .
                }}
                GROUP BY ?vers1 ORDER BY ?vers2
            }}
        }}
    """

    # Creation of merged attribute versions
    query2 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{
                ?attr addr:hasAttributeVersion ?rootAttrVers .
                ?rootAttrVers a addr:AttributeVersion .
            }}
            GRAPH ?gt {{
                ?rootAttrVers addr:isDerivedFrom ?attrVers .
            }}
        }}
        WHERE {{
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            BIND(URI(CONCAT(STR(URI(facts:)), "AV_", STRUUID())) AS ?rootAttrVers)
            {{
                SELECT DISTINCT ?gt ?versMergeVal WHERE {{
                    BIND({tmp_named_graph_uri.n3()} AS ?gt)
                    GRAPH ?gt {{
                        ?attrVers a addr:AttributeVersion ; addr:versMergeVal ?versMergeVal .
                    }}
                }}
            }}
            ?attr addr:hasAttributeVersion ?attrVers .
            ?attrVers addr:versMergeVal ?versMergeVal ; addr:hasTrace ?attrVersTrace.
        }}
        """

    # Creation of changes between consecutive merged attribute versions
    query3 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{
                ?newChange a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?newEvent ; addr:makesEffective ?vME ; addr:outdates ?vO .
                ?newEvent a addr:Event .
            }}
            GRAPH ?gt {{
                ?newChange addr:isDerivedFrom ?change .
                ?newEvent addr:isDerivedFrom ?event .
            }}
        }}
        WHERE {{
            BIND({tmp_named_graph_uri.n3()} AS ?gt)
            {{
                SELECT * WHERE {{
                    BIND({facts_named_graph_uri.n3()} AS ?gf)
                    ?change a addr:AttributeChange .
                    {{
                        ?change addr:makesEffective ?vMETrace ; addr:outdates ?vOTrace .
                        ?vME addr:isDerivedFrom ?vMETrace .
                        ?vO addr:isDerivedFrom ?vOTrace .
                        FILTER(!sameTerm(?vME, ?vO))
                    }} UNION {{
                        ?change addr:makesEffective ?vMETrace .
                        ?vME addr:isDerivedFrom ?vMETrace .
                        FILTER NOT EXISTS {{ ?change addr:outdates ?vOTrace . }}
                    }} UNION {{
                        ?change addr:outdates ?vOTrace .
                        ?vO addr:isDerivedFrom ?vOTrace .
                        FILTER NOT EXISTS {{ ?change addr:makesEffective ?vMETrace . }}
                    }}
                }}
            }}
            ?change addr:appliedTo ?attr ; addr:dependsOn ?event .
            BIND(URI(CONCAT(STR(URI(facts:)), "CG_", STRUUID())) AS ?newChange)
            BIND(URI(CONCAT(STR(URI(facts:)), "EV_", STRUUID())) AS ?newEvent)
        }}
        """

    query4 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{
                ?newTime a addr:CrispTimeInstant .
                ?event ?timeProp ?newTime .
            }}
            GRAPH ?gt {{
                ?newTime addr:isDerivedFrom ?timeTrace . 
            }}
        }} WHERE {{ 
            {{
                SELECT * WHERE {{
                    ?event a addr:Event ; addr:isDerivedFrom ?eventTrace .
                    {{
                        BIND(addr:hasTime AS ?timeProp)
                        ?eventTrace addr:hasTime ?timeTrace .
                    }} UNION {{
                        ?eventTrace ?timeProp ?timeTrace .
                        FILTER (?timeProp IN (addr:hasTimeBefore, addr:hasTimeAfter))
                        FILTER NOT EXISTS {{ ?eventTrace addr:hasTime ?time }}
                    }}
                }}
            }}
            BIND({tmp_named_graph_uri.n3()} AS ?gt)
            BIND({facts_named_graph_uri.n3()} AS ?gf)
            BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?newTime)
        }}
        """

    # Transfer traces from temporary elements to facts ones
    query5 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gi {{
                ?elem addr:hasTrace ?elemTrace .
            }}
        }} WHERE {{
            BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
            ?elem addr:isDerivedFrom ?tmpElem .
            ?tmpElem addr:hasTrace ?elemTrace .
        }}
        """

    queries = [query1, query2, query3, query4, query5]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)


def merge_similar_successive_attribute_versions(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    to_be_merged_with(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)
    merge_attribute_versions_to_be_merged(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)
    
    # Transfer factoid information to facts
    rt.transfer_elements_to_roots(graphdb_url, repository_name, facts_named_graph_uri)