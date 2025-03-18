from rdflib import URIRef
from namespaces import NameSpaces
import graphdb as gd
import multi_sources_processing as msp
import resource_transfert as rt


np = NameSpaces()

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
            ?lmTrace addr:hasTime [?propIntervalTime ?timeTrace ] .
        }} UNION {{
            ?changeTrace addr:isChangeType ?changeType ; addr:appliedTo ?lmTrace ; addr:dependsOn ?eventTrace .
            ?eventTrace ?propInstantTime ?timeTrace .
            FILTER (?propInstantTime IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        }}
        ?time addr:hasTrace ?timeTrace .
        FILTER NOT EXISTS {{ GRAPH ?gf {{ ?event addr:hasTime ?t }} }}
    }}
    """

    # Select the best time(s) from the sources to be the trace of the time of each event related to the appearance or disappearance of landmarks
    # First query get the precise time of each event (if it exists) : <?event addr:hasTime ?time>
    # Second one give a time estimation for event which does not have any precise time (according first query) : <?event addr:hasTimeBefore ?time> and/or <?event addr:hasTimeAfter ?time>
    query3a = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?event ?propTime ?time . }}
    }}
    WHERE {{
        BIND({inter_sources_name_graph_uri.n3()} AS ?gi)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND(addr:hasTime AS ?propTime)
        GRAPH ?gf {{ ?lm a addr:Landmark . }}
        ?change addr:dependsOn ?event ; addr:appliedTo ?lm .
        FILTER NOT EXISTS {{ GRAPH ?gf {{ ?event addr:hasTime ?t }} }}
        GRAPH ?gt {{ ?event ?propTime ?time . }}              
        ?event ?propTime ?time .
    }}
    """

    query3b = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?event ?propTime ?time . }}
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
        BIND(IF(?propTime = addr:hasTimeBefore, ?diffTimeMin, ?diffTimeMax) AS ?diffTime)
        ?event ?propTime ?time .
        ?time addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
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

def create_changes_for_versions_with_valid_time(graphdb_url:URIRef, repository_name:str, tmp_named_graph_uri:URIRef):
    """
    We create two changes for attribute versions which have a valid time (start and end time) :
    AttributeVersion(v) ^ Landmark(lm) ^ hasTime(lm, t) ^ hasAttribute(lm, attr) ^ hasAttributeVersion(attr, v) => AttributeChange(cgME) ^ AttributeChange(cgO) ^ makesEffective(cgME, v) ^ outdates(cgO, v)
    """

    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?change addr:isRealChange "true"^^xsd:boolean.
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        ?rootAttr addr:hasTrace ?attr .
        ?change addr:appliedTo ?attr .
    }}
    """

    # Initialisation of changes and events of attribute versions with valid time
    # These resources are temporary and will be removed later
    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?event a addr:Event ; addr:hasTime ?time .
            ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?event ; ?changeProp ?vers ; addr:isRealChange "false"^^xsd:boolean.
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        {{
            SELECT DISTINCT ?attr ?vers ?changeProp ?time WHERE {{
                VALUES (?changeProp ?propTime) {{ (addr:makesEffective addr:hasBeginning) (addr:outdates addr:hasEnd) }}
                ?lm a addr:Landmark ; addr:hasTime [?propTime ?time] ; addr:hasAttribute ?attr .
                ?attr addr:hasAttributeVersion ?vers .
                FILTER NOT EXISTS {{ ?change ?changeProp ?vers }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?change)
        BIND(URI(CONCAT(STR(URI(factoids:)), "EV_", STRUUID())) AS ?event)
    }}
    """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)


def get_elementary_changes(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    gregorian_calendar_uri = URIRef("http://www.wikidata.org/entity/Q1985727")

    # Four step to get elementary changes : 
    # 1. For each attribute, create as many TimeDescription object as there are temporal values related to it
    # 2. For each attribute, detect duplicate time values and create a list of changes without doublons, each change is related to a unique value (which is a simplified time)
    # 3. Order changes temporally (according simplified time which is a double)
    # 4. Create fake changes (related to -inf and +inf temporal values)

    # For each attribute, create as many TimeDescription object as there are temporal values related to it
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?rootAttr addr:hasTimeDescription [a addr:TimeDescription ; addr:hasTimeElement ?rootTime ; addr:hasTimeProperty addr:hasTime ; addr:hasSimplifiedTime ?simplifiedTime ; addr:hasRelatedChange ?change ] .
        }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        BIND({gregorian_calendar_uri.n3()} AS ?timeCal)
        GRAPH ?gf {{ ?rootAttr a addr:Attribute . }}
        ?rootAttr addr:hasTrace ?attr .
        ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn [addr:hasTime ?time] .
        ?rootTime addr:hasTrace ?time ; addr:timeStamp ?timeStamp ; addr:timeCalendar ?timeCal .
        BIND(ofn:asDays(?timeStamp - "0001-01-01"^^xsd:dateTimeStamp) AS ?simplifiedTime)
        FILTER NOT EXISTS {{ ?rootAttr addr:hasTimeDescription [addr:hasRelatedChange ?change] }}
    }}
    """

    # For each attribute, detect duplicate time values and create a list of changes without doublons
    query2 =  np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:hasTimeDescription [a addr:TimeDescription ; addr:hasTimeProperty addr:hasTime ; addr:hasSimplifiedTime ?simplifiedTime] .
        }}
    }} WHERE {{
        {{
            SELECT DISTINCT ?gt ?attr ?simplifiedTime ?timeProperty WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                GRAPH ?gt {{ ?attr addr:hasTimeDescription [addr:hasSimplifiedTime ?simplifiedTime ; addr:hasTimeProperty ?timeProperty] }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?change)
        }}
    """

    # Order changes
    query3 =  np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{ ?cg addr:hasNextChange ?cgBis . }}
    }}
    WHERE {{
        {{
            SELECT ?gt ?attr ?cg (MIN(?diffTime) AS ?minDiffTime) WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                GRAPH ?gt {{
                    ?cg addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime].
                    ?cgBis addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?stBis ; addr:hasTimeProperty addr:hasTime].
                }}
                BIND(?stBis - ?st AS ?diffTime)
                FILTER(!sameTerm(?cg, ?cgBis) && ?diffTime > 0)
            }}
            GROUP BY ?gt ?attr ?cg
        }}
        ?cg addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime].
        ?cgBis addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?stBis ; addr:hasTimeProperty addr:hasTime].
        FILTER(?stBis - ?st = ?minDiffTime)
    }}
    """

    # Create fake changes (related to -inf and +inf temporal values) 
    query4 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?newChange a addr:AttributeChange ; addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime] .
            ?prevChange addr:hasNextChange ?nextChange .
        }} 
    }} WHERE {{
        {{
            SELECT DISTINCT ?gt ?attr ?cg ?firstChangeMissing ?st WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                GRAPH ?gf {{ ?attr a addr:Attribute . }}
                GRAPH ?gt {{ ?cg addr:appliedTo ?attr . }}
                {{
                    FILTER NOT EXISTS {{ ?cg addr:hasNextChange ?x }}
                    BIND("false"^^xsd:boolean AS ?firstChangeMissing)
                    BIND("INF"^^xsd:double AS ?st)
                }} UNION {{
                    FILTER NOT EXISTS {{ ?x addr:hasNextChange ?cg }}
                    BIND("true"^^xsd:boolean AS ?firstChangeMissing)
                    BIND("-INF"^^xsd:double AS ?st)
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?newChange)
        BIND(IF(?firstChangeMissing, ?newChange, ?cg)  AS ?prevChange)
        BIND(IF(?firstChangeMissing, ?cg, ?newChange)  AS ?nextChange)
    }}
    """

    queries = [query1, query2, query3, query4]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)
    
def get_elementary_versions(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    # Create versions between two successive changes (one makes effective the version while the other outdates it)
    # Get an explicit triple to have successive changes (`?cg1 addr:hasNextChange ?cg2`)
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?attr addr:hasAttributeVersion ?vers .
            ?vers a addr:AttributeVersion .
            ?cg1 addr:makesEffective ?vers .
            ?cg2 addr:outdates ?vers .
        }}
    }} WHERE {{
        {{
            SELECT DISTINCT ?gt ?attr ?cg1 ?cg2 WHERE {{
                BIND({tmp_named_graph_uri.n3()} AS ?gt)
                BIND({facts_named_graph_uri.n3()} AS ?gf)
                GRAPH ?gf {{ ?attr a addr:Attribute . }}
                GRAPH ?gt {{
                    ?cg1 addr:appliedTo ?attr .
                    ?cg2 addr:appliedTo ?attr .
                    ?cg1 addr:hasNextChange ?cg2 .
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(factoids:)), "AV_", STRUUID())) AS ?vers)
    }}
    """

    # Order versions : hasNextVersion()
    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{
            ?vO addr:hasNextVersion ?vME .
        }}
    }} WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        GRAPH ?gf {{ ?attr a addr:Attribute . }}
        ?cg addr:appliedTo ?attr ; addr:makesEffective ?vME ; addr:outdates ?vO .
    }}
    """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def get_elementary_change_traces(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    # Link existing attribute changes with created one when the are related
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{ ?cg addr:derives ?cgTrace ; ?propTrace ?cgTrace. }}
    }} WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        GRAPH ?gf {{ ?attr a addr:Attribute . }}
        GRAPH ?gt {{
            ?cg addr:appliedTo ?attr ; addr:hasTimeDescription [addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime] .
            ?attr addr:hasTimeDescription [addr:hasSimplifiedTime ?st ; addr:hasRelatedChange ?cgTrace] .
        }}
        ?cgTrace a addr:AttributeChange ; addr:isRealChange ?realChange .
        BIND(IF(?realChange, addr:hasTrace, addr:derives) AS ?propTrace)
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def get_elementary_version_traces(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, tmp_named_graph_uri:URIRef):

    # Link existing attribute versions related to changes with created one when the are related :
    # * addr:makesEffective(cg, v) ^ addr:makesEffective(cgTrace, vTrace) ^ addr:hasTrace(cg, cgTrace) => addr:hasTrace(v, vTrace)
    # * addr:outdates(cg, v) ^ addr:outdates(cgTrace, vTrace) ^ addr:hasTrace(cg, cgTrace) => addr:hasTrace(v, vTrace)
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{ ?vers addr:hasTrace ?versTrace . }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        GRAPH ?gf {{ ?attr a addr:Attribute . }}
        ?cgTrace ?changeProp ?versTrace .
        GRAPH ?gt {{
            VALUES ?changeProp {{ addr:makesEffective addr:outdates }}
            ?cg a addr:AttributeChange ; addr:appliedTo ?attr ; addr:hasTrace ?cgTrace ; ?changeProp ?vers.
        }}
    }}
    """

    # Get traces for elementary versions which are not already traced
    # If hasTrace(vi, vTrace) ^ hasTrace(vj, vTrace) ^ vk is between vi and vj => hasTrace(vk, vTrace)
    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{ ?vers addr:hasTrace ?vTrace . }}
        }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        BIND({facts_named_graph_uri.n3()} AS ?gf)
        GRAPH ?gf {{ ?attr a addr:Attribute . }}
        ?attr addr:hasAttributeVersion ?vers .
        GRAPH ?gt {{
            ?cgME addr:makesEffective ?vers .
            ?cgO addr:outdates ?vers .
            {{ ?cgStart addr:hasNextChange+ ?cgME }} UNION {{ BIND(?cgME AS ?cgStart) }}
            {{ ?cgO addr:hasNextChange+ ?cgEnd }} UNION {{ BIND(?cgO AS ?cgEnd) }}
            ?cgStart addr:derives ?cgMEVTrace .
            ?cgEnd addr:derives ?cgOVTrace .
        }}
        ?cgMEVTrace addr:makesEffective ?vTrace .
        ?cgOVTrace addr:outdates ?vTrace .
    }}
    """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def get_elementary_versions_and_changes(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    create_changes_for_versions_with_valid_time(graphdb_url, repository_name, tmp_named_graph_uri)
    get_elementary_changes(graphdb_url, repository_name, facts_named_graph_uri, tmp_named_graph_uri)
    get_elementary_versions(graphdb_url, repository_name, facts_named_graph_uri, tmp_named_graph_uri)
    get_elementary_change_traces(graphdb_url, repository_name, facts_named_graph_uri, tmp_named_graph_uri)
    get_elementary_version_traces(graphdb_url, repository_name, facts_named_graph_uri, tmp_named_graph_uri)

    
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

    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gt {{
                ?toRemoveChangeME addr:toRemove "true"^^xsd:boolean .
                ?toRemoveChangeO addr:toRemove "true"^^xsd:boolean .
                ?version addr:toRemove "true"^^xsd:boolean .
                ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:makesEffective ?vME ; addr:outdates ?vO.
                ?change addr:hasTimeDescription [addr:hasSimplifiedTime ?stME ; addr:hasTimeProperty addr:hasTimeAfter ] , [addr:hasSimplifiedTime ?stO ; addr:hasTimeProperty addr:hasTimeBefore ]
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
                    FILTER NOT EXISTS {{ ?version addr:hasTrace ?versionTrace . }}
                    OPTIONAL {{ ?changeME addr:hasTrace ?changeMETrace . }}
                    OPTIONAL {{ ?changeO addr:hasTrace ?changeOTrace . }}
                    BIND(IF(BOUND(?changeMETrace), "true"^^xsd:boolean, "false"^^xsd:boolean) AS ?hasChangeMETrace)
                    BIND(IF(BOUND(?changeOTrace), "true"^^xsd:boolean, "false"^^xsd:boolean) AS ?hasChangeOTrace)
                    FILTER(!(?hasChangeMETrace && ?hasChangeOTrace))
                }} 
            }}

            BIND(URI(CONCAT(STR(URI(factoids:)), "CG_", STRUUID())) AS ?newChange)
            BIND(IF(!?hasChangeMETrace && !?hasChangeOTrace, ?newChange, IF(!?hasChangeMETrace, ?changeO, ?changeME)) AS ?change)

            OPTIONAL {{
                ?changeME addr:hasTimeDescription [addr:hasSimplifiedTime ?stME ; addr:hasTimeProperty addr:hasTime ] .
                FILTER(!?hasChangeMETrace)
            }}
            OPTIONAL {{
                ?changeO addr:hasTimeDescription [addr:hasSimplifiedTime ?stO ; addr:hasTimeProperty addr:hasTime ] .
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
    
    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gt {{ ?changeME addr:hasNextChange ?changeO . }}
    }}
    WHERE {{
        BIND({tmp_named_graph_uri.n3()} AS ?gt)
        GRAPH ?gt {{
            ?attr addr:hasAttributeVersion ?version .
            ?version a addr:AttributeVersion .
            ?changeME a addr:AttributeChange ; addr:makesEffective ?version .
            ?changeO a addr:AttributeChange ; addr:outdates ?version .
        }}
    }} 
    """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

    # Remove all triples where resources r for which it exists a triple <r addr:toRemove "true"^^xsd:boolean> is in these triples
    # In this case, remove selected versions and their related changes which are not traced
    msp.remove_all_triples_for_resources_to_remove(graphdb_url, repository_name)

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
                ?rootAttrVers addr:derives ?attrVers .
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
            ?attrVers addr:versMergeVal ?versMergeVal .
        }}
        """
    

    # Creation of changes between consecutive merged attribute versions
    query3 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?gf {{
                ?newChange a addr:AttributeChange ; addr:appliedTo ?attr ; addr:makesEffective ?vME ; addr:outdates ?vO .
            }}
            GRAPH ?gt {{
                ?newChange addr:derives ?change .
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
                        ?vME addr:derives ?vMETrace .
                        ?vO addr:derives ?vOTrace .
                        FILTER(!sameTerm(?vME, ?vO))
                    }} UNION {{
                        ?change addr:makesEffective ?vMETrace .
                        ?vME addr:derives ?vMETrace .
                        FILTER NOT EXISTS {{ ?change addr:outdates ?vOTrace . }}
                    }} UNION {{
                        ?change addr:outdates ?vOTrace .
                        ?vO addr:derives ?vOTrace .
                        FILTER NOT EXISTS {{ ?change addr:makesEffective ?vMETrace . }}
                    }}
                }}
            }}
            ?change addr:appliedTo ?attr .
            BIND(URI(CONCAT(STR(URI(facts:)), "CG_", STRUUID())) AS ?newChange)
        }}
        """

    queries = [query1, query2, query3]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)


def merge_similar_successive_attribute_versions(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    to_be_merged_with(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)
    merge_attribute_versions_to_be_merged(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)
    

def create_events_and_times_from_attribute_changes(graphdb_url:URIRef, repository_name:str, facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{
            ?event a addr:Event .
            ?change addr:dependsOn ?event .
        }} 
    }}
    WHERE {{
        {{
            SELECT DISTINCT * WHERE {{
                BIND ({facts_named_graph_uri.n3()} AS ?gf)
                GRAPH ?gf {{
                    ?change addr:appliedTo ?attr .
                    ?attr a addr:Attribute .
                    FILTER NOT EXISTS {{ ?change addr:dependsOn ?event . }}
                }}
            }}
        }}
        BIND(URI(CONCAT(STR(URI(facts:)), "EV_", STRUUID())) AS ?event)
    }} 
    """

    query2 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gf {{ ?event ?propTime ?time }}
    }}
    WHERE {{
        BIND ({facts_named_graph_uri.n3()} AS ?gf)
        GRAPH ?gf {{
            ?change addr:appliedTo ?attr ; addr:dependsOn ?event .
            ?attr a addr:Attribute .
        }}
        ?change addr:derives ?derivedCg .
        {{
            VALUES ?propTime {{ addr:hasTime }}
            ?derivedCg addr:hasTimeDescription [ addr:hasSimplifiedTime ?st ; addr:hasTimeProperty ?propTime ].
            ?attr addr:hasTimeDescription [ addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime ; addr:hasTimeElement ?time ].
        }} UNION {{
            ?derivedCg addr:hasTimeDescription [ addr:hasSimplifiedTime ?st ; addr:hasTimeProperty ?propTime ].
            FILTER NOT EXISTS {{ ?derivedCg addr:hasTimeDescription [ addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime ]. }}
            ?attr addr:hasTimeDescription [ addr:hasSimplifiedTime ?st ; addr:hasTimeProperty addr:hasTime ; addr:hasTimeElement ?time ].
        }}
    }}
    """

    query3 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?gi {{ ?version addr:hasTrace ?versionTrace . }}
    }}
    WHERE {{
        BIND ({facts_named_graph_uri.n3()} AS ?gf)
        BIND ({inter_sources_name_graph_uri.n3()} AS ?gi)
        GRAPH ?gf {{
            ?version a addr:AttributeVersion .
        }}
        ?version addr:derives [ addr:hasTrace ?versionTrace ] .
    }}
    """

    queries = [query1, query2, query3]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def get_attribute_version_evolution_from_elementary_elements(graphdb_url:URIRef, repository_name:str,
                                                             facts_named_graph_uri:URIRef, inter_sources_name_graph_uri:URIRef, tmp_named_graph_uri:URIRef):
    
    # First step : remove empty attribute versions : remove untraced versions if there are related at least to one untraced change 
    remove_empty_attribute_versions(graphdb_url, repository_name, tmp_named_graph_uri)

    # Merge similar successive versions if they have similar values
    merge_similar_successive_attribute_versions(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)
    
    # For all changes which are in facts named graph, link it to an event which has to have a time
    # <event addr:hasTime time> if possible, then <event addr:hasTimeBefore beforeTime> and/or <event addr:hasTimeAfter afterTime>
    create_events_and_times_from_attribute_changes(graphdb_url, repository_name, facts_named_graph_uri, inter_sources_name_graph_uri, tmp_named_graph_uri)

    # Transfer factoid information to facts
    rt.transfer_elements_to_roots(graphdb_url, repository_name, facts_named_graph_uri)