import json
import re
from rdflib import Graph, Literal, URIRef, Namespace, XSD
from rdflib.namespace import RDF, RDFS, XSD, SKOS
from namespaces import NameSpaces
import geom_processing as gp
import file_management as fm
import str_processing as sp
import time_processing as tp
import multi_sources_processing as msp
import wikidata as wd
import graphdb as gd
import graphrdf as gr
import resource_transfert as rt
import resource_initialisation as ri
import factoids_cleaning as fc
import states_events_json as sej
import create_factoids_descriptions as cfd

np = NameSpaces()

## Wikidata data

def create_landmark_change_and_event(g:Graph, lm_label:str, lm_type:URIRef, lm_prov_uri:URIRef, appeareance:bool, time_list:list, lang:str):
        # Creating URIs
        lm_label_lit, lm_uri = gr.get_name_literal(lm_label, lang), gr.generate_uri(np.FACTOIDS, "LM")
        name_attr_uri, name_attr_type_uri, name_attr_version_uri = gr.generate_uri(np.FACTOIDS, "ATTR"), np.ATYPE["Name"], gr.generate_uri(np.FACTOIDS, "AV")
        time_uri, event_uri = gr.generate_uri(np.FACTOIDS, "TI"), gr.generate_uri(np.FACTOIDS, "EV")
        lm_change_app_uri, name_attr_change_app_uri = gr.generate_uri(np.FACTOIDS, "CG"), gr.generate_uri(np.FACTOIDS, "CG")
        time_stamp, time_calendar, time_precision = time_list

        # Definition of the types of changes depending on whether or not an appearance is required
        if appeareance:
            lm_change_app_type_uri = np.CTYPE["LandmarkAppearance"]
            name_attr_change_app_type_uri = np.CTYPE["AttributeVersionAppearance"]
            ri.create_attribute_change(g, name_attr_change_app_uri, name_attr_change_app_type_uri, name_attr_uri, made_effective_versions_uris=[name_attr_version_uri])
        else:
            lm_change_app_type_uri = np.CTYPE["LandmarkDisappearance"]
            name_attr_change_app_type_uri = np.CTYPE["AttributeVersionDisappearance"]
            ri.create_attribute_change(g, name_attr_change_app_uri, name_attr_change_app_type_uri, name_attr_uri, outdated_versions_uris=[name_attr_version_uri])

        ri.create_landmark(g, lm_uri, lm_label_lit, lm_type)
        ri.create_landmark_attribute_and_version(g, lm_uri, name_attr_uri, name_attr_type_uri, name_attr_version_uri, lm_label_lit)
        ri.create_landmark_change(g, lm_change_app_uri, lm_change_app_type_uri, lm_uri)
        ri.create_crisp_time_instant(g, time_uri, time_stamp, time_calendar, time_precision)
        ri.create_event_with_time(g, event_uri, time_uri)
        ri.create_change_event_relation(g, lm_change_app_uri, event_uri)
        ri.create_change_event_relation(g, name_attr_change_app_uri, event_uri)

        uris = [event_uri, lm_uri, name_attr_version_uri]
        for uri in uris:
            ri.add_provenance_to_resource(g, uri, lm_prov_uri)

def get_paris_landmarks_from_wikidata(out_csv_file):
    """
    Get thouroughfares and districts of Paris and get the communes of the former Seine department
    """

    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
    PREFIX wb: <http://wikiba.se/ontology#>
    PREFIX time: <http://www.w3.org/2006/time#>

    SELECT DISTINCT ?landmarkId ?landmarkType ?nomOff ?startTimeStamp ?startTimePrec ?startTimeCal ?startTimeDef ?endTimeStamp ?endTimePrec ?endTimeCal ?endTimeDef ?statement ?statementType
    WHERE {
        {
            ?landmarkId p:P361 [ps:P361 wd:Q16024163].
            BIND("Thoroughfare" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P361 [ps:P361 wd:Q107311481].
            BIND("Thoroughfare" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q252916].
            BIND("District" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q702842]; p:P131 [ps:P131 wd:Q90].
            BIND("District" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q484170]; p:P131 [ps:P131 wd:Q1142326].
            BIND("Municipality" AS ?landmarkType)
        }
        {
            ?landmarkId p:P1448 ?nomOffSt.
            ?nomOffSt ps:P1448 ?nomOff.
            BIND(?nomOffSt AS ?statement)
            BIND(wb:Statement AS ?statementType)
            OPTIONAL {?nomOffSt pqv:P580 ?startTimeValSt }
            OPTIONAL {?nomOffSt pqv:P582 ?endTimeValSt }
        }UNION{
            ?landmarkId rdfs:label ?nomOff.
            FILTER (LANG(?nomOff) = "fr")
            MINUS {?landmarkId p:P1448 ?nomOffSt}
            BIND(?landmarkId AS ?statement)
            BIND(wb:Item AS ?statementType)
        }
        OPTIONAL { ?landmarkId p:P571 [psv:P571 ?startTimeValIt] }
        OPTIONAL { ?landmarkId p:P576 [psv:P576 ?endTimeValIt] }
        BIND(IF(BOUND(?startTimeValSt), ?startTimeValSt, IF(BOUND(?startTimeValIt), ?startTimeValIt, "")) AS ?startTimeVal)
        BIND(IF(BOUND(?endTimeValSt), ?endTimeValSt, IF(BOUND(?endTimeValIt), ?endTimeValIt, "")) AS ?endTimeVal)
        OPTIONAL { ?startTimeVal wb:timeValue ?startTimeStamp ; wb:timePrecision ?startTimePrecRaw ; wb:timeCalendarModel ?startTimeCal . }
        OPTIONAL { ?endTimeVal wb:timeValue ?endTimeStamp ; wb:timePrecision ?endTimePrecRaw ; wb:timeCalendarModel ?endTimeCal . }
        BIND(IF(?statementType = wb:Statement, BOUND(?startTimeValSt), IF(?statementType = wb:Item, BOUND(?startTimeValIt), "false"^^xsd:boolean)) AS ?startTimeDef)
        BIND(IF(?statementType = wb:Statement, BOUND(?endTimeValSt), IF(?statementType = wb:Item, BOUND(?endTimeValIt), "false"^^xsd:boolean)) AS ?endTimeDef)

        BIND(IF(?startTimePrecRaw = 11, time:unitDay,
                IF(?startTimePrecRaw = 10, time:unitMonth,
                    IF(?startTimePrecRaw = 9, time:unitYear,
                        IF(?startTimePrecRaw = 8, time:unitDecade,
                        IF(?startTimePrecRaw = 7, time:unitCentury,
                            IF(?startTimePrecRaw = 6, time:unitMillenium, ?x
                                )))))) AS ?startTimePrec)
        BIND(IF(?endTimePrecRaw = 11, time:unitDay,
                IF(?endTimePrecRaw = 10, time:unitMonth,
                    IF(?endTimePrecRaw = 9, time:unitYear,
                        IF(?endTimePrecRaw = 8, time:unitDecade,
                        IF(?endTimePrecRaw = 7, time:unitCentury,
                            IF(?endTimePrecRaw = 6, time:unitMillenium, ?x
                                )))))) AS ?endTimePrec)
        }
    """

    query = wd.save_select_query_as_csv_file(query, out_csv_file)


def get_paris_locations_from_wikidata(out_csv_file):
    """
    Get the location of Paris data (thoroughfares and areas) from Wikidata
    """

    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
    PREFIX wb: <http://wikiba.se/ontology#>
    PREFIX time: <http://www.w3.org/2006/time#>

    SELECT DISTINCT ?locatumId ?relatumId ?landmarkRelationType ?dateStartStamp ?dateStartCal ?dateStartPrec ?dateEndStamp ?dateEndCal ?dateEndPrec ?statement ?statementType WHERE {
    {
        ?locatumId p:P361 [ps:P361 wd:Q16024163].
    }UNION{
        ?locatumId p:P361 [ps:P361 wd:Q107311481].
    }UNION{
        ?locatumId p:P31 [ps:P31 wd:Q252916].
    }UNION{
        ?locatumId p:P31 [ps:P31 wd:Q702842]; p:P131 [ps:P131 wd:Q90].
    }UNION{
        ?locatumId p:P31 [ps:P31 wd:Q484170]; p:P131 [ps:P131 wd:Q1142326].
    }
    BIND(wb:Statement AS ?statementType)
    ?locatumId p:P131 ?statement.
    ?statement ps:P131 ?relatumId.
    OPTIONAL {?statement pq:P580 ?dateStartStamp; pqv:P580 [wb:timeCalendarModel ?dateStartCal ; wb:timePrecision ?dateStartPrecRaw]}
    OPTIONAL {?statement pq:P582 ?dateEndStamp; pqv:P582 [wb:timeCalendarModel ?dateEndCal; wb:timePrecision ?dateEndPrecRaw]}
    BIND("Within" AS ?landmarkRelationType)
    BIND(IF(?dateStartPrecRaw = 11, time:unitDay,
            IF(?dateStartPrecRaw = 10, time:unitMonth,
                IF(?dateStartPrecRaw = 9, time:unitYear,
                    IF(?dateStartPrecRaw = 8, time:unitDecade,
                    IF(?dateStartPrecRaw = 7, time:unitCentury,
                        IF(?dateStartPrecRaw = 6, time:unitMillenium, ?x
                            )))))) AS ?dateStartPrec)
    BIND(IF(?dateEndPrecRaw = 11, time:unitDay,
            IF(?dateEndPrecRaw = 10, time:unitMonth,
                IF(?dateEndPrecRaw = 9, time:unitYear,
                    IF(?dateEndPrecRaw = 8, time:unitDecade,
                    IF(?dateEndPrecRaw = 7, time:unitCentury,
                        IF(?dateEndPrecRaw = 6, time:unitMillenium, ?x
                            )))))) AS ?dateEndPrec)
    }
    """

    query = wd.save_select_query_as_csv_file(query, out_csv_file)

## Use Wikidata endpoint to select data
def get_data_from_wikidata(wdp_land_csv_file, wdp_loc_csv_file):
    """
    Obtain CSV files for data from Wikidata
    """
    get_paris_landmarks_from_wikidata(wdp_land_csv_file)
    get_paris_locations_from_wikidata(wdp_loc_csv_file)

def create_factoids_repository_wikidata_paris(graphdb_url, wdp_repository_name, tmp_folder,
                                     ont_file, ontology_named_graph_name,
                                     factoids_named_graph_name, permanent_named_graph_name,
                                     wdp_land_csv_file, wdp_loc_csv_file, wdp_kg_file, wdp_valid_time={}, lang=None):

    # Creation of a basic graph with rdflib and export to the `wdp_kg_file` file
    g = create_graph_from_wikidata_paris(wdp_land_csv_file, wdp_loc_csv_file, wdp_valid_time, lang)

    # Export the graph and import it into the directory
    msp.transfert_rdflib_graph_to_factoids_repository(graphdb_url, wdp_repository_name, factoids_named_graph_name, g, wdp_kg_file, tmp_folder, ont_file, ontology_named_graph_name)

    # Adapting data with the ontology, merging duplicates, etc.
    clean_repository_wikidata_paris(graphdb_url, wdp_repository_name, wdp_valid_time, factoids_named_graph_name, permanent_named_graph_name, lang)

def create_graph_from_wikidata_paris(wdp_land_file, wdp_loc_file, source_valid_time, lang):
    wd_pref, wd_ns = "wd", Namespace("http://www.wikidata.org/entity/")
    wds_pref, wds_ns = "wds", Namespace("http://www.wikidata.org/entity/statement/")
    wb_pref, wb_ns = "wb", Namespace("http://wikiba.se/ontology#")
    wiki_prefixes_and_namespaces = [[wd_pref, wd_ns], [wds_pref, wds_ns], [wb_pref, wb_ns]]

    ## File columns Wikidata
    lm_id_col, lm_type_col, lm_label_col = "landmarkId", "landmarkType", "nomOff"
    prov_id_col, prov_id_type_col = "statement", "statementType"
    lr_type_col = "landmarkRelationType"
    locatum_id_col, relatum_id_col = "locatumId","relatumId"
    start_time_stamp_col, start_time_cal_col, start_time_prec_col, start_time_def_col = "startTimeStamp", "startTimeCal", "startTimePrec", "startTimeDef"
    end_time_stamp_col, end_time_cal_col, end_time_prec_col, end_time_def_col = "endTimeStamp", "endTimeCal", "endTimePrec", "endTimeDef"

    # Read the two files
    content_lm = fm.read_csv_file_as_dict(wdp_land_file, id_col=lm_id_col, delimiter=",", encoding='utf-8-sig')
    content_lr = fm.read_csv_file_as_dict(wdp_loc_file, delimiter=",", encoding='utf-8-sig')

    source_valid_time = tp.get_valid_time_description(source_valid_time)

    g = Graph()
    gr.add_namespaces_to_graph(g, np.namespaces_with_prefixes)
    for [prefix, ns] in wiki_prefixes_and_namespaces:
        g.bind(prefix, ns)

    # Creating landmarks
    for value in content_lm.values():
        lm_id = value.get(lm_id_col)
        lm_label = value.get(lm_label_col)
        lm_type = value.get(lm_type_col)
        lm_prov_id = value.get(prov_id_col)
        lm_prov_id_type = value.get(prov_id_type_col)
        start_time_stamp = tp.get_literal_time_stamp(value.get(start_time_stamp_col)) if value.get(start_time_stamp_col) != "" else None
        start_time_prec = gr.get_valid_uri(value.get(start_time_prec_col))
        start_time_cal = gr.get_valid_uri(value.get(start_time_cal_col))
        start_time_def = Literal(value.get(start_time_def_col), datatype=XSD.boolean)
        start_time = [start_time_stamp, start_time_cal, start_time_prec, start_time_def]
        end_time_stamp = tp.get_literal_time_stamp(value.get(end_time_stamp_col)) if value.get(end_time_stamp_col) != "" else None
        end_time_prec = gr.get_valid_uri(value.get(end_time_prec_col))
        end_time_cal = gr.get_valid_uri(value.get(end_time_cal_col))
        end_time_def = Literal(value.get(end_time_def_col), datatype=XSD.boolean)
        end_time = [end_time_stamp, end_time_cal, end_time_prec, end_time_def]

        create_data_value_from_wikidata_landmark(g, lm_id, lm_label, lm_type, lm_prov_id, lm_prov_id_type, start_time, end_time, source_valid_time, lang)

    # Creating landmark relations
    for value in content_lr.values():
        lr_type = value.get(lr_type_col)
        lr_prov_id = value.get(prov_id_col)
        lr_prov_id_type = value.get(prov_id_type_col)
        locatum_id = value.get(locatum_id_col)
        relatum_id = value.get(relatum_id_col)
        create_data_value_from_wikidata_landmark_relation(g, lr_type, locatum_id, relatum_id, lr_prov_id, lr_prov_id_type)

    return g

def create_data_value_from_wikidata_landmark(g, lm_id, lm_label, lm_type, lm_prov_id, lm_prov_id_type, start_time:list, end_time:list, source_valid_time:dict, lang):
    """
    `source_valid_time` : dictionary describing the source's validity start and end dates
    `source_valid_time = {"start":{"stamp":..., "precision":..., "calendar":...}, "end":{} }`
    """

    name_attr_version_value = gr.get_name_literal(lm_label, lang)

    # URI of the thoroughfare, creation of the thoroughfare, addition of geometry and alternative labels
    lm_uri, lm_type_uri = gr.generate_uri(np.FACTOIDS, "LM"), np.LTYPE[lm_type]
    wd_uri = URIRef(lm_id)

    # Provenances creation
    lm_prov_uri, lm_prov_id_type_uri = URIRef(lm_prov_id), URIRef(lm_prov_id_type)
    ri.create_prov_entity(g, lm_prov_uri)
    g.add((lm_prov_uri, RDF.type, lm_prov_id_type_uri)) # Indicate that `lm_prov_uri` is a Wikibase statement or item
    g.add((lm_uri, SKOS.closeMatch, wd_uri))

    lm_attr_types_and_values = [[np.ATYPE["Name"], name_attr_version_value]]
    ri.create_landmark_version(g, lm_uri, lm_type_uri, lm_label, lm_attr_types_and_values, source_valid_time, lm_prov_uri, np.FACTOIDS, lang)

    start_time_stamp, start_time_calendar, start_time_precision, start_time_def = start_time
    end_time_stamp, end_time_calendar, end_time_precision, end_time_def = end_time

    # Add an event describing the appearance of the lane and its name (if indicated by a date)
    if start_time_def:
        create_landmark_change_and_event(g, lm_label, lm_type_uri, lm_prov_uri, True, [start_time_stamp, start_time_calendar, start_time_precision], lang)
    if end_time_def:
        create_landmark_change_and_event(g, lm_label, lm_type_uri, lm_prov_uri, False, [end_time_stamp, end_time_calendar, end_time_precision], lang)

def create_data_value_from_wikidata_landmark_relation(g, lr_type, locatum_id, relatum_id, lr_prov_id, lr_prov_id_type):
    # URIs of landmark relations
    lr_uri = gr.generate_uri(np.FACTOIDS, "LR")
    locatum_uri = URIRef(locatum_id)
    relatum_uri = URIRef(relatum_id)

    # Provenances creation
    lr_prov_uri, lr_prov_id_type_uri = URIRef(lr_prov_id), URIRef(lr_prov_id_type)
    ri.create_prov_entity(g, lr_prov_uri)
    g.add((lr_prov_uri, RDF.type, lr_prov_id_type_uri))  # Indicate that `lm_prov_uri` is a Wikibase statement

    # Creating landmark relations
    ri.create_landmark_relation(g, lr_uri, np.LRTYPE[lr_type], locatum_uri, [relatum_uri])
    ri.add_provenance_to_resource(g, lr_uri, lr_prov_uri)

def remove_orphan_provenance_entities(graphdb_url:str, repository_name:str):
    """
    Remove all provenance entities which are not related with any statement
    """

    query = np.query_prefixes + f"""
    DELETE {{
        ?wdLr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrType ; addr:locatum ?wdLoc ; addr:relatum ?wdRel ; prov:wasDerivedFrom ?prov.
    }}
    WHERE {{
        ?wdLr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrType ; addr:locatum ?wdLoc ; addr:relatum ?wdRel ; prov:wasDerivedFrom ?prov.
        OPTIONAL {{
        ?l skos:closeMatch ?wdLoc .
    	?r skos:closeMatch ?wdRel .
    	BIND(URI(CONCAT(STR(URI(factoids:)), "LR_", STRUUID())) AS ?lmRel)
        }}
        BIND(BOUND(?l) && BOUND(?r) AS ?exist)
        BIND(IF(?exist, ?lmRel, ?x) AS ?lr)
        BIND(IF(?exist, ?l, ?x) AS ?loc)
        BIND(IF(?exist, ?r, ?x) AS ?rel)
    }}
    """

    gd.update_query(query, graphdb_url, repository_name)

def create_landmark_relations_for_wikidata_paris(graphdb_url:str, repository_name:str, factoids_named_graph_uri:URIRef):
    query1 = np.query_prefixes + f"""
    DELETE {{
        ?wdLr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrType ; addr:locatum ?wdLoc ; addr:relatum ?wdRel ; prov:wasDerivedFrom ?prov.
    }}
    INSERT {{
        GRAPH ?g {{
            ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrType ; addr:locatum ?loc ; addr:relatum ?rel ; prov:wasDerivedFrom ?prov.
        }}
    }}
    WHERE {{
        BIND({factoids_named_graph_uri.n3()} AS ?g)
        ?wdLr a addr:LandmarkRelation ; addr:isLandmarkRelationType ?lrType ; addr:locatum ?wdLoc ; addr:relatum ?wdRel ; prov:wasDerivedFrom ?prov.
        OPTIONAL {{
        ?l skos:closeMatch ?wdLoc .
    	?r skos:closeMatch ?wdRel .
    	BIND(URI(CONCAT(STR(URI(factoids:)), "LR_", STRUUID())) AS ?lmRel)
        }}
        BIND(BOUND(?l) && BOUND(?r) AS ?exist)
        BIND(IF(?exist, ?lmRel, ?x) AS ?lr)
        BIND(IF(?exist, ?l, ?x) AS ?loc)
        BIND(IF(?exist, ?r, ?x) AS ?rel)
    }}
    """

    # Eliminating orphan provenance
    query2 = np.query_prefixes + f"""
    DELETE {{
        ?s ?p ?prov .
        ?prov ?p ?o .
    }}
    WHERE {{
        BIND({factoids_named_graph_uri.n3()} AS ?g)
        GRAPH ?g {{?prov a prov:Entity}}
        FILTER NOT EXISTS {{?x prov:wasDerivedFrom ?prov}}
        {{?s ?p ?prov}}UNION{{?prov ?p ?o}}
    }}
    """

    queries = [query1, query2]
    for query in queries:
        gd.update_query(query, graphdb_url, repository_name)


def clean_repository_wikidata_paris(graphdb_url:str, repository_name:str, source_valid_time:dict, factoids_named_graph_name:str, permanent_named_graph_name:str, lang:str):
    factoids_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, factoids_named_graph_name)
    permanent_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, permanent_named_graph_name)

    create_landmark_relations_for_wikidata_paris(graphdb_url, repository_name, factoids_named_graph_uri)
    fc.merge_similar_temporal_entities(graphdb_url, repository_name, factoids_named_graph_uri)

    # Transfer all provenance descriptions to the permanent named graph
    rt.transfert_immutable_triples(graphdb_url, repository_name, factoids_named_graph_uri, permanent_named_graph_uri)

    # The URI below defines the source linked to Wikidata
    vdp_source_uri = np.FACTS["Source_WD"]
    source_label = "Wikidata"
    source_lang = "mul"
    fc.create_source_resource(graphdb_url, repository_name, vdp_source_uri, source_label, None, source_lang, np.FACTS, permanent_named_graph_uri)
    fc.link_provenances_with_source(graphdb_url, repository_name, vdp_source_uri, permanent_named_graph_uri)

################################################################ Events ###########################################################

def create_graph_from_events(events_json_file:str):
    """
    Creation of a graph from the events JSON file
    """

    # Creation of a basic graph with rdflib
    event_descriptions = fm.read_json_file(events_json_file)

    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_event_descriptions(event_descriptions)
    np.bind_namespaces(g)

    return g

##################################################### BAN ##########################################################

def create_graph_from_paris_ban(ban_file:str, valid_time:dict, source:dict, lang:str):
    """
    Creation of a graph from the BAN file
    """

    ban_pref, ban_ns = "ban", Namespace("https://adresse.data.gouv.fr/base-adresse-nationale/")
    
    ban_description = cfd.create_state_description_for_ban(ban_file, valid_time, source, lang, ban_ns)    
    g = sej.create_graph_from_states_descriptions(ban_description)
    g.bind(ban_pref, ban_ns)
    np.bind_namespaces(g)

    return g

##################################################### OSM ##########################################################

def create_graph_from_osm(osm_file:str, osm_hn_file:str, valid_time:dict, source:dict, lang:str):
    osm_pref, osm_ns = "osm", Namespace("https://www.openstreetmap.org/")
    osm_rel_pref, osm_rel_ns = "osmRel", Namespace("https://www.openstreetmap.org/relation/")

    osm_description = cfd.create_state_description_for_osm(osm_file, osm_hn_file, valid_time, source, lang, osm_ns)
    g = sej.create_graph_from_states_descriptions(osm_description)
    g.bind(osm_pref, osm_ns)
    g.bind(osm_rel_pref, osm_rel_ns)
    np.bind_namespaces(g)

    return g

##################################################### Ville de Paris ##########################################################

def create_graph_from_ville_paris(vpa_file:str, vpc_file:str, vpa_valid_time:dict, vpa_source:dict, vpc_source:dict, lang:str):
    vpa_pref, vpa_ns = "vdpa", Namespace("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/denominations-emprises-voies-actuelles/records/")
    vpc_pref, vpc_ns = "vdpc", Namespace("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/denominations-des-voies-caduques/records/")

    state_vpa_description, event_vpa_description = cfd.create_state_and_event_description_for_ville_paris_actuelles(vpa_file, vpa_valid_time, vpa_source, lang, vpa_ns)
    event_vpc_description = cfd.create_event_description_for_ville_paris_caduques(vpc_file, vpc_source, lang, vpc_ns)

    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_states_descriptions(state_vpa_description)
    g += sej.create_graph_from_event_descriptions(event_vpa_description)
    g += sej.create_graph_from_event_descriptions(event_vpc_description)
    g.bind(vpa_pref, vpa_ns)
    g.bind(vpc_pref, vpc_ns)
    np.bind_namespaces(g)

    return g

##################################################### Geojson ##########################################################

def create_graph_from_geojson_states_of_thoroughfares(geojson_file:str, lang, valid_time:dict, source:dict, identity_property:str, name_attribute:str):
    state_description = cfd.create_state_description_for_geojson_states(geojson_file, "thoroughfare", identity_property, name_attribute, lang, valid_time, source)
    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_states_descriptions(state_description)
    np.bind_namespaces(g)

    return g

def create_graph_from_geojson_states_of_streetnumbers(geojson_file:str, lang, valid_time:dict, source:dict, identity_property:str, name_attribute:str):
    state_description = cfd.create_state_description_for_geojson_states_of_streetnumbers(geojson_file, identity_property, name_attribute, lang, valid_time, source)
    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_states_descriptions(state_description)
    np.bind_namespaces(g)

    return g

##################################################### Generic ##########################################################


def clean_imported_repository(graphdb_url:URIRef, repository_name:str, factoids_named_graph_name:str, permanent_named_graph_name:str):
    factoids_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, factoids_named_graph_name)
    permanent_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, permanent_named_graph_name)

    # Transferring non-modifiable triples to the permanent named graph
    rt.transfert_immutable_triples(graphdb_url, repository_name, factoids_named_graph_uri, permanent_named_graph_uri)


def create_factoids_repository(graphdb_url:URIRef, repository_name:str, tmp_folder:str,
                               ont_file:str, ontology_named_graph_name:str, kg_file:str,
                               factoids_named_graph_name:str, permanent_named_graph_name:str,
                               g:Graph):
    # Export the graph and import it into the repository
    msp.transfert_rdflib_graph_to_factoids_repository(graphdb_url, repository_name, factoids_named_graph_name, g, kg_file, tmp_folder, ont_file, ontology_named_graph_name)

    # Adapting data with the ontology, merging duplicates, etc.
    clean_imported_repository(graphdb_url, repository_name, factoids_named_graph_name, permanent_named_graph_name)
