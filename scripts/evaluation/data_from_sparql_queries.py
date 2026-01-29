from scripts.graph_construction import graphdb as gd
from scripts.graph_construction.namespaces import NameSpaces

np = NameSpaces()

def select_streetnumbers_attr_geom_change_times(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    query = np.query_prefixes  + f"""
    SELECT DISTINCT 
    ?lm ?label ?change 
    (ofn:asDays(?time - "0001-01-01"^^xsd:dateTimeStamp) AS ?timeDay)
    (ofn:asDays(?timeBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?timeBeforeDay)
    (ofn:asDays(?timeAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?timeAfterDay)
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{ ?lm a addr:Landmark }}
        ?lm addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute ?attr ; skos:hiddenLabel ?snLabel .
        ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?lm ; addr:relatum [skos:hiddenLabel ?thLabel] .
        BIND(CONCAT(?thLabel, "||", ?snLabel) AS ?label)
        ?attr addr:isAttributeType atype:Geometry .
        ?change addr:appliedTo ?attr ; addr:dependsOn ?event .
        OPTIONAL {{ ?event addr:hasTime [addr:timeStamp ?time] }}
        OPTIONAL {{ ?event addr:hasTimeBefore [addr:timeStamp ?timeBefore] }}
        OPTIONAL {{ ?event addr:hasTimeAfter [addr:timeStamp ?timeAfter] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)

    query = np.query_prefixes  + f"""
    SELECT DISTINCT 
    ?sn ?label ?attrVersion ?sourceLabel
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{ ?sn a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; skos:hiddenLabel ?snLabel .}}
        ?sn addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
        [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
        ?th addr:isLandmarkType ltype:Thoroughfare ; skos:hiddenLabel ?thLabel .
        BIND(CONCAT(?thLabel, "||", ?snLabel) AS ?label)
        ?attrVersion prov:wasDerivedFrom ?prov .
        ?prov rico:isOrWasDescribedBy [rdfs:label ?sourceLabel] .
    }}
    """
    
    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

##################################

def select_streetnumbers_labels(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    query = np.query_prefixes  + f"""
    SELECT DISTINCT ?sn ?snLabel ?thLabel
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; rdfs:label|skos:altLabel ?snLabel .
            [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
            ?th addr:isLandmarkType ltype:Thoroughfare ; rdfs:label|skos:altLabel ?thLabel .
        }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)


def select_streetnumbers_attr_geom_version_valid_times(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = np.query_prefixes  + f"""

    SELECT DISTINCT ?sn ?attrVersion ?startTime ?endTime
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        }}
        FILTER NOT EXISTS {{
            ?attrVersion addr:hasTrace ?traceAV1, ?traceAV2 .
            ?traceAV1 addr:differentVersionValueFrom ?traceAV2 .
            }}
        ?cgMe addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.
        ?evME addr:hasTime|addr:hasTimeBefore [addr:timeStamp ?startTime] .
        ?evO addr:hasTime|addr:hasTimeAfter [addr:timeStamp ?endTime] .
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumbers_attr_geom_version_values(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = np.query_prefixes  + f"""

    SELECT DISTINCT ?attrVersion ?versionValue
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        }}
        ?attrVersion addr:versionValue ?versionValue .
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumbers_attr_geom_change_valid_times(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = np.query_prefixes  + f"""
    SELECT DISTINCT ?sn ?attr ?change ?time ?timeAfter ?timeBefore 
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{ ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?ev . }}
        ?attr addr:isAttributeType atype:Geometry .
        ?sn a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute ?attr .
        OPTIONAL {{ ?ev addr:hasTime [addr:timeStamp ?time] }} 
        OPTIONAL {{ ?ev addr:hasTimeAfter [addr:timeStamp ?timeAfter] }}
        OPTIONAL {{ ?ev addr:hasTimeBefore [addr:timeStamp ?timeBefore] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumber_modified_attr_geom_versions(graphdb_url, repository_name,
                                                    facts_named_graph_name, named_graph_names:list, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    named_graph_uris = [gd.get_named_graph_uri_from_name(graphdb_url, repository_name, name) for name in named_graph_names]
    named_graph_filter = ",".join([uri.n3() for uri in named_graph_uris])
    
    query = np.query_prefixes  + f"""
    SELECT DISTINCT 
    ?newAttrVersion ?attrVersion
    (ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
    (ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
    (ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
    (ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
    (ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
    (ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{ ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber .}}
        ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?newAttrVersion] .
        ?cgME addr:makesEffective ?newAttrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?newAttrVersion ; addr:dependsOn ?evO.
        ?newAttrVersion prov:wasDerivedFrom ?attrVersion .
        GRAPH ?g {{ ?attrVersion a prov:Entity . }}
        FILTER (?g IN ({named_graph_filter}))
        
        OPTIONAL {{ ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }}
        OPTIONAL {{ ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }}
        OPTIONAL {{ ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }}
        OPTIONAL {{ ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }}
        OPTIONAL {{ ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }}
        OPTIONAL {{ ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }}
    }}
    """
    print(query)

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumber_unmodified_attr_geom_versions(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    query = np.query_prefixes  + f"""
    SELECT DISTINCT 
    ?attrVersion
    (ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
    (ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
    (ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
    (ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
    (ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
    (ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
    WHERE {{
        ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber .
        ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
        ?cgME addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.

        OPTIONAL {{ ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }}
        OPTIONAL {{ ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }}
        OPTIONAL {{ ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }}
        OPTIONAL {{ ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }}
        OPTIONAL {{ ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }}
        OPTIONAL {{ ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)