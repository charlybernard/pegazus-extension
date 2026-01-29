import re
import datetime
from rdflib import Literal, URIRef
from rdflib.namespace import XSD
from scripts.graph_construction.namespaces import NameSpaces, OntologyMapping
from scripts.graph_construction import graphdb as gd


np = NameSpaces()
om = OntologyMapping()

def get_query_to_compare_time_instants(time_named_graph_uri:URIRef, time_instant_select_conditions:str):
    """"
    `time_instant_select_conditions` defines conditions to select two instants which have to be compared : ?ti1 and ?ti2
    """
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?ti1 ?timeProp ?ti2 .
            ?ti1 ?precSameTime ?ti2 .

        }}
    }}
    WHERE {{
        BIND ({time_named_graph_uri.n3()} AS ?g)
        {{
            SELECT DISTINCT ?ti1 ?ti2 ?ts1 ?ts2 ?tp1 ?tp2 ?tc WHERE {{
                {time_instant_select_conditions}

                ?ti1 a addr:CrispTimeInstant; addr:timeStamp ?ts1; addr:timeCalendar ?tc; addr:timePrecision ?tp1.
                ?ti2 a addr:CrispTimeInstant; addr:timeStamp ?ts2; addr:timeCalendar ?tc; addr:timePrecision ?tp2.

                FILTER (?ti1 != ?ti2)
                FILTER(?ts1 <= ?ts2)

                MINUS {{
                    ?ti1 ?p ?ti2 .
                    FILTER(?p IN (addr:instantSameTime, addr:instantBefore, addr:instantAfter))
                }}
            }}
        }}


        BIND(YEAR(?ts1) = YEAR(?ts2) AS ?sameYear)
        BIND(MONTH(?ts1) = MONTH(?ts2) AS ?sameMonth)
        BIND(DAY(?ts1) = DAY(?ts2) AS ?sameDay)

        BIND(IF(time:unitMillenium in (?tp1, ?tp2), FLOOR(YEAR(?ts1)/1000) = FLOOR(YEAR(?ts2)/1000),
                IF(time:unitCentury in (?tp1, ?tp2), FLOOR(YEAR(?ts1)/100) = FLOOR(YEAR(?ts2)/100),
                    IF(time:unitDecade in (?tp1, ?tp2), FLOOR(YEAR(?ts1)/10) = FLOOR(YEAR(?ts2)/10),
                        IF(time:unitYear in (?tp1, ?tp2), ?sameYear,
                            IF(time:unitMonth in (?tp1, ?tp2), ?sameYear && ?sameMonth,
                                IF(time:unitDay in (?tp1, ?tp2), ?sameYear && ?sameMonth && ?sameDay,
                                    "false"^^xsd:boolean)
                            ))))) AS ?sameTime)

        BIND(IF(?tp1 = time:unitMillenium, "1"^^xsd:integer, 
                IF(?tp1 = time:unitCentury, "2"^^xsd:integer,
                    IF(?tp1 = time:unitDecade, "3"^^xsd:integer,
                        IF(?tp1 = time:unitYear, "4"^^xsd:integer,
                            IF(?tp1 = time:unitMonth, "5"^^xsd:integer,
                                IF(?tp1 = time:unitDay, "6"^^xsd:integer,
                                    "0"^^xsd:integer)
                            ))))) AS ?ti1prec)

        BIND(IF(?tp2 = time:unitMillenium, "1"^^xsd:integer, 
                IF(?tp2 = time:unitCentury, "2"^^xsd:integer,
                    IF(?tp2 = time:unitDecade, "3"^^xsd:integer,
                        IF(?tp2 = time:unitYear, "4"^^xsd:integer,
                            IF(?tp2 = time:unitMonth, "5"^^xsd:integer,
                                IF(?tp2 = time:unitDay, "6"^^xsd:integer,
                                    "0"^^xsd:integer)
                            ))))) AS ?ti2prec)

        BIND(IF(?ti1prec > ?ti2prec, addr:instantLessPreciseThan, 
                IF(?ti1prec < ?ti2prec, addr:instantMorePreciseThan, addr:instantAsPreciseAs
                )) AS ?precisonPred)
                
        OPTIONAL {{
            FILTER(?sameTime)
            BIND(?precisonPred AS ?precSameTime)
        }}

        BIND(IF(?sameTime, addr:instantSameTime, addr:instantBefore) AS ?timeProp)
    }}
    """

    return query

def get_query_to_compare_time_intervals(time_named_graph_uri:URIRef, time_interval_select_conditions:str):
    """
    Compare time intervals according Allen algebra
    """
    
    query = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalBefore ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg .
        ?i1end addr:instantBefore ?i2beg .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalMeets ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg .
        ?i1end addr:instantSameTime ?i2beg .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalOverlaps ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasBeginning ?i1beg ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg ; addr:hasEnd ?i2end .
        ?i1beg addr:instantBefore ?i2beg .
        ?i1end addr:instantAfter ?i2beg .
        ?i1end addr:instantBefore ?i2end .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalStarts ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasBeginning ?i1beg ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg ; addr:hasEnd ?i2end .
        ?i1beg addr:instantSameTime ?i2beg .
        ?i1end addr:instantBefore ?i2end .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalDuring ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasBeginning ?i1beg ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg ; addr:hasEnd ?i2end .
        ?i1beg addr:instantAfter ?i2beg .
        ?i1end addr:instantBefore ?i2end .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalFinishes ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasBeginning ?i1beg ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg ; addr:hasEnd ?i2end .
        ?i1beg addr:instantAfter ?i2beg .
        ?i1end addr:instantSameTime ?i2end .
    }} ;

    INSERT {{
        GRAPH ?g {{
            ?i1 time:intervalEquals ?i2
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        {time_interval_select_conditions}
        
        ?i1 a addr:CrispTimeInterval ; addr:hasBeginning ?i1beg ; addr:hasEnd ?i1end .
        ?i2 a addr:CrispTimeInterval ; addr:hasBeginning ?i2beg ; addr:hasEnd ?i2end .
        ?i1beg addr:instantSameTime ?i2beg .
        ?i1end addr:instantSameTime ?i2end .
    }}
    """

    return query

def compare_time_instants_of_events_from_traces(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    """
    Sort all time instants related to events which are a trace of one event.
    """
    
    time_instant_select_conditions = """
        ?ev a addr:Event ; addr:hasTrace [?tpred1 ?ti1] ; [?tpred2 ?ti2] .
        FILTER(?tpred1 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        FILTER(?tpred2 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
    """

    query = get_query_to_compare_time_instants(time_named_graph_uri, time_instant_select_conditions)

    gd.update_query(query, graphdb_url, repository_name)

def compare_time_instants_of_events(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    """
    Sort all time instants related to one event.
    """
    
    time_instant_select_conditions = """
        ?ev a addr:Event ; ?tpred1 ?ti1 ; ?tpred2 ?ti2 .
        FILTER(?tpred1 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        FILTER(?tpred2 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
    """

    query = get_query_to_compare_time_instants(time_named_graph_uri, time_instant_select_conditions)

    gd.update_query(query, graphdb_url, repository_name)

def compare_time_instants_of_attributes(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    """
    Sort all time instants related to one attribute.
    """
    
    time_instant_select_conditions = """
        ?attr a addr:Attribute .
        ?cg1 a addr:AttributeChange ; addr:dependsOn [?tpred1 ?ti1] ; addr:appliedTo ?attr .
        ?cg2 a addr:AttributeChange ; addr:dependsOn [?tpred2 ?ti2] ; addr:appliedTo ?attr .
        FILTER(?tpred1 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        FILTER(?tpred2 IN (addr:hasTime, addr:hasTimeBefore, addr:hasTimeAfter))
        """
    
    query = get_query_to_compare_time_instants(time_named_graph_uri, time_instant_select_conditions)

    gd.update_query(query, graphdb_url, repository_name)

def compare_time_intervals_of_attribute_versions(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    """
    Sort all time intervals of versions related to one attribute.
    """
    
    time_interval_select_conditions = """
        ?attr a addr:Attribute ; addr:hasAttributeVersion ?av1, ?av2 .
        ?av1 addr:hasTime ?i1 .
        ?av2 addr:hasTime ?i2 .
        FILTER (?av1 != ?av2)
        """
    
    query = get_query_to_compare_time_intervals(time_named_graph_uri, time_interval_select_conditions)

    gd.update_query(query, graphdb_url, repository_name)

def get_earliest_and_latest_time_instants_for_events(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    """
    An event can get related to multiple instants through addr:hasTimeBefore and addr:hasTimeAfter. This function gets the latest and the earliest time instant for each event.
    If a previous latest or earliest time instant is no longer the correct one, it is removed.
    """
    
    query1 = np.query_prefixes + f"""
    INSERT {{
        GRAPH ?g {{
            ?ev ?estPred ?t .
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        VALUES (?erPred ?estPred ?compPred) {{
            (addr:hasTimeAfter addr:hasEarliestTimeInstant addr:instantAfter)
            (addr:hasTimeBefore addr:hasLatestTimeInstant addr:instantBefore)
        }}
        ?ev a addr:Event ; ?erPred ?t .
        OPTIONAL {{
            ?ev addr:hasTime ?time .
        }}
        OPTIONAL {{
            ?ev ?erPred ?tBis .
            FILTER (?tBis != ?t)
            {{
                ?tBis ?compPred ?t .
            }}UNION{{
                ?tBis time:instantMorePreciseThan ?t ;
                addr:instantSameTime ?t .
            }}
        }}
        FILTER(!BOUND(?tBis) && !BOUND(?time))
    }}
    """

    query2 = np.query_prefixes + f"""
        DELETE {{
            ?ev ?estPred ?tEst
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            VALUES ?estPred {{ addr:hasEarliestTimeInstant addr:hasLatestTimeInstant }}
            ?ev a addr:Event ; ?estPred ?tEst .
            {{
                ?ev addr:hasTime ?time .
            }}UNION{{
                ?ev ?estPred ?tEstBis .
                FILTER(?tEst != ?tEstBis)
                MINUS {{
                    ?tEstBis addr:instantSameTime ?tEst ; addr:instantAsPreciseAs ?tEst .
                }}
            }}
        }}
        """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)

def remove_earliest_and_latest_time_instants(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
    DELETE {{
        GRAPH ?g {{
            ?s ?p ?o
        }}
    }}
    WHERE {{
        BIND({time_named_graph_uri.n3()} AS ?g)
        ?s ?p ?o .
        FILTER(?p IN (addr:hasLatestTimeInstant, addr:hasEarliestTimeInstant))
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def get_validity_interval_for_attribute_versions(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    # Creation of a time interval of attribute version without any time interval
    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{
                ?av addr:hasTime ?timeInterval .
                ?timeInterval a addr:CrispTimeInterval .
                }}
        }}
        WHERE {{
            ?av a addr:AttributeVersion .
            MINUS {{ ?av addr:hasTime [a addr:CrispTimeInterval] }}
            BIND(URI(CONCAT(STR(URI(facts:)), "TI_", STRUUID())) AS ?timeInterval)
        }}
    """

    # Add instants for time intervals related to attribute versions
    query2 = np.query_prefixes + f"""
        DELETE {{
            ?timeInterval addr:hasBeginning ?curTIBeg ; addr:hasEnd ?curTIEnd .
        }}
        INSERT {{
            GRAPH ?g {{
                ?timeInterval addr:hasBeginning ?ti1 ; addr:hasEnd ?ti2 .
            }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)

            ?av a addr:AttributeVersion ; addr:hasTime ?timeInterval .
            ?timeInterval a addr:CrispTimeInterval .
            ?cg1 a addr:AttributeChange; addr:dependsOn ?ev1 ; addr:makesEffective ?av .
            ?cg2 a addr:AttributeChange; addr:dependsOn ?ev2 ; addr:outdates ?av .
            OPTIONAL {{?ev1 addr:hasTime ?tip1}}
            OPTIONAL {{?ev2 addr:hasTime ?tip2}}
            OPTIONAL {{?ev1 addr:hasLatestTimeInstant ?til1 .}}
            OPTIONAL {{?ev2 addr:hasLatestTimeInstant ?til2 .}}
            OPTIONAL {{?ev1 addr:hasEarliestTimeInstant ?tie1 .}}
            OPTIONAL {{?ev2 addr:hasEarliestTimeInstant ?tie2 .}}
            
            FILTER(BOUND(?tip1) || BOUND(?til1) || BOUND(?tie1))
            FILTER(BOUND(?tip2) || BOUND(?til2) || BOUND(?tie2))

            BIND(IF(BOUND(?tip1), ?tip1, IF(BOUND(?til1), ?til1, ?tie1)) AS ?ti1)
            BIND(IF(BOUND(?tip2), ?tip2, IF(BOUND(?tie2), ?tie2, ?til2)) AS ?ti2)

            OPTIONAL{{
                ?timeInterval addr:hasBeginning ?curTIBeg .
                MINUS {{
                    ?curTIBeg addr:instantSameTime ?ti1 ; addr:instantAsPreciseAs ?ti1 .
                }}
                FILTER(?curTIBeg != ?ti1)
            }}
            OPTIONAL{{
                ?timeInterval addr:hasEnd ?curTIEnd .
                MINUS {{
                    ?curTIEnd addr:instantSameTime ?ti2 ; addr:instantAsPreciseAs ?ti2 .
                }}
                FILTER(?curTIEnd != ?ti2)
            }}
        }}
    """

    queries = [query1, query2]
    for query in queries :
        gd.update_query(query, graphdb_url, repository_name)

def add_time_relations(graphdb_url:URIRef, repository_name:str, time_named_graph_name:str):
    """
    Add temporal relationships:
    * comparison of instants belonging to the same event (i1 before/after i2)
    * comparison of instants linked to the same attribute (i1 before/after i2)
    * deduction of earliest/latest instants linked to events from before/after instants
    * creation of validity intervals for attribute versions
    * comparison of version intervals between versions of the same attribute

    The set of triples is stored in the named graph whose name is `time_named_graph_name`.
    """
    
    time_named_graph_uri = URIRef(gd.get_named_graph_uri_from_name(graphdb_url, repository_name, time_named_graph_name))
    compare_time_instants_of_events(graphdb_url, repository_name, time_named_graph_uri)
    compare_time_instants_of_attributes(graphdb_url, repository_name, time_named_graph_uri)
    get_earliest_and_latest_time_instants_for_events(graphdb_url, repository_name, time_named_graph_uri)
    get_validity_interval_for_attribute_versions(graphdb_url, repository_name, time_named_graph_uri)
    compare_time_intervals_of_attribute_versions(graphdb_url, repository_name, time_named_graph_uri)


def compare_events(graphdb_url:URIRef, repository_name:str, time_named_graph_name:str=None):

    time_named_graph_uri = URIRef(gd.get_named_graph_uri_from_name(graphdb_url, repository_name, time_named_graph_name))

    get_similar_events(graphdb_url, repository_name, time_named_graph_uri)
    get_events_before(graphdb_url, repository_name, time_named_graph_uri)

def get_similar_events(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):
    query = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 owl:sameAs ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?ev1 a addr:Event .
            ?ev2 a addr:Event .
            ?ev1 addr:eventBefore ?ev2 .
            ?ev1 addr:eventAfter ?ev2 .
        }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def get_events_before(graphdb_url:URIRef, repository_name:str, time_named_graph_uri:URIRef):

    # An event A whose time value is before a time value dependent on an event B, then A is before B.
    query1 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?ev1 a addr:Event .
            ?ev2 a addr:Event .
            FILTER (?ev1 != ?ev2)
            ?ev1 addr:hasTime ?t1 .
            ?ev2 addr:hasTime ?t2 .
            ?t1 addr:instantBefore ?t2 .
        }}
    """

    # For a landmark, the event linked to the change describing its appearance is located before the event linked to the change describing its disappearance.
    query2 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?lm a addr:Landmark .
            ?cg1 a addr:Change ; addr:isChangeType ctype:LandmarkAppearance ; addr:appliedTo ?lm ; addr:dependsOn ?ev1 .
            ?cg2 a addr:Change ; addr:isChangeType ctype:LandmarkDisappearance ; addr:appliedTo ?lm ; addr:dependsOn ?ev2 .
        }}
        """
    
    # For a landmark relation, the event linked to the change describing its appearance is located before the event linked to the change describing its disappearance.
    query3 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?lr a addr:LandmarkRelation .
            ?cg1 a addr:Change ; addr:isChangeType ctype:LandmarkRelationAppearance ; addr:appliedTo ?lr ; addr:dependsOn ?ev1 .
            ?cg2 a addr:Change ; addr:isChangeType ctype:LandmarkRelationDisappearance ; addr:appliedTo ?lr ; addr:dependsOn ?ev2 .
        }}
        """
    
    # For a landmark relation, the event linked to the change describing its appearance is after any event linked to a change in the appearance of a landmark included in the relationship.
    # Ie, a landmark relation can only exist when the landmarks described exist.
    query4 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?lr a addr:LandmarkRelation .
            ?lm a addr:Landmark .
            ?lr (addr:locatum|addr:relatum) ?lm .
            ?cg1 a addr:Change ; addr:isChangeType ctype:LandmarkAppearance ; addr:appliedTo ?lm ; addr:dependsOn ?ev1 .
            ?cg2 a addr:Change ; addr:isChangeType ctype:LandmarkRelationAppearance ; addr:appliedTo ?lr ; addr:dependsOn ?ev2 .
        }}
        """
    
    # For a landmark relation, the event linked to the change describing its disappearance is above all an event linked to a change in the disappearance of a landmark included in the relationship.
    # Ie, a rlandmark relation disappears before the landmarks described disappear.
    query5 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?lr a addr:LandmarkRelation .
            ?lm a addr:Landmark .
            ?lr (addr:locatum|addr:relatum) ?lm .
            ?cg1 a addr:Change ; addr:isChangeType ctype:LandmarkRelationDisappearance ; addr:appliedTo ?lr ; addr:dependsOn ?ev1 .
            ?cg2 a addr:Change ; addr:isChangeType ctype:LandmarkDisappearance ; addr:appliedTo ?lm ; addr:dependsOn ?ev2 .
        }}
        """
    
    # An event linked to a change describing a version's effectiveness is located before the event linked to the change describing the version's expiration.
    query6 = np.query_prefixes + f"""
        INSERT {{
            GRAPH ?g {{ ?ev1 addr:eventBefore ?ev2 . }}
        }}
        WHERE {{
            BIND({time_named_graph_uri.n3()} AS ?g)
            ?av a addr:AttributeVersion .
            ?cg1 a addr:AttributeChange ; addr:makesEffective ?av ; addr:dependsOn ?ev1 .
            ?cg2 a addr:AttributeChange ; addr:outdates ?av ; addr:dependsOn ?ev2 .
        }}
        """
    
    queries = [query1, query2, query3, query4, query5, query6]
    for query in queries :
        gd.update_query(query, graphdb_url, repository_name)

def get_time_precision_from_integer(precision_int:int):
    """
    Get the precision of a time instant from an integer.
    The integer is defined in the ontology as follows:
    1: millennium
    2: century
    3: decade
    4: year
    5: month
    6: day
    """

    precisions = {
        0: "billion_years",
        1: "hundred_million_years",
        3: "million_years",
        4: "hundred_thousand_years",
        5: "ten_thousand_years",
        6: "millennium",
        7: "century",
        8: "decade",
        9: "year",
        10: "month",
        11: "day",
        12: "hour",
        13: "minute",
        14: "second"
    }

    precision = precisions.get(precision_int)    
    return precision

def get_time_calendar_from_wikidata_uri(calendar_uri:URIRef):
    calendars = {
        np.WD["Q1985727"]: "gregorian",
        np.WD["Q181974"]: "republican",
        np.WD["Q1985786"]: "julian",
    }

    calendar = calendars.get(calendar_uri)
    return calendar

def get_time_instant_elements(time_dict:dict):
    if not isinstance(time_dict, dict):
        return [None, None, None]
    
    time_stamp = time_dict.get("stamp")
    time_cal = time_dict.get("calendar")
    time_prec = time_dict.get("precision")
    
    if None in [time_stamp, time_cal, time_prec]:
        return [None, None, None]
    
    stamp = get_literal_time_stamp(time_stamp)
    precision = om.get_time_unit(time_prec)
    calendar = om.get_time_calendar(time_cal)

    return [stamp, calendar, precision]

def get_literal_time_stamp(time_stamp:str):
    return Literal(time_stamp, datatype=XSD.dateTimeStamp)

def get_current_timestamp():
    return datetime.datetime.now().isoformat() + "Z"

def get_valid_time_description(time_description:dict):
    stamp_key, calendar_key, precision_key = "stamp", "calendar", "precision"
    start_time_key, end_time_key = "start", "end"
    start_time = get_time_instant_elements(time_description.get(start_time_key))
    end_time = get_time_instant_elements(time_description.get(end_time_key))

    if not isinstance(start_time, list) or None in start_time:
        time_description[start_time_key] = {stamp_key:None, precision_key:None, calendar_key:None}

    if not isinstance(end_time, list) or None in end_time:
        time_description[end_time_key] = {stamp_key:get_current_timestamp(), precision_key:"day", calendar_key:"gregorian"}

    return time_description

def get_gregorian_date_from_timestamp(time_stamp:str):
    time_match_pattern = "^(-|\+|)\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$"
    if re.match(time_match_pattern, time_stamp) is not None:
        time_stamp += "T00:00:00Z"
        time_description = {"stamp":time_stamp, "calendar":"gregorian", "precision":"day"}
        time_elements = get_time_instant_elements(time_description)

        return time_elements
    
    return [None, None, None]