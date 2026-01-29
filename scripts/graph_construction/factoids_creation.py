from rdflib import Graph, Literal, URIRef, Namespace
from scripts.graph_construction.namespaces import NameSpaces
from scripts.utils import file_management as fm
from scripts.graph_construction import multi_sources_processing as msp
from scripts.resource_management import wikidata as wd
from scripts.graph_construction import graphdb as gd
from scripts.graph_construction import resource_transfert as rt
from scripts.resource_management import states_events_json as sej
from scripts.graph_construction import create_factoids_descriptions as cfd


np = NameSpaces()

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

################################################################################################################

## Wikidata data

def create_graph_from_wikidata(wdp_land_csv_file:str, wdp_loc_csv_file:str, source:dict, lang:str):
    """
    Creation of a graph from the Wikidata file
    """

    wd_description = cfd.create_event_description_for_wikidata(wdp_land_csv_file, wdp_loc_csv_file, lang, source)
    g = sej.create_graph_from_event_descriptions(wd_description)
    np.bind_namespaces(g)

    return g

### Use Wikidata endpoint to select data

def get_data_from_wikidata(wdp_land_csv_file, wdp_loc_csv_file):
    """
    Obtain CSV files for data from Wikidata
    """
    get_paris_landmarks_from_wikidata(wdp_land_csv_file)
    get_paris_locations_from_wikidata(wdp_loc_csv_file)

def get_paris_landmarks_from_wikidata(out_csv_file):
    """
    Get thouroughfares and districts of Paris and get the communes of the former Seine department
    """

    query = """
    PREFIX wb: <http://wikiba.se/ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
    PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>

    SELECT DISTINCT ?landmarkId ?landmarkType ?nomOff ?lang ?timeType ?timeStamp ?timePrec ?timeCal ?statement
        WHERE {
        {
            ?landmarkId p:P361 [ps:P361 wd:Q16024163].
            BIND("thoroughfare" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P361 [ps:P361 wd:Q107311481].
            BIND("thoroughfare" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q252916].
            BIND("district" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q702842]; p:P131 [ps:P131 wd:Q90].
            BIND("district" AS ?landmarkType)
        }UNION{
            ?landmarkId p:P31 [ps:P31 wd:Q484170]; p:P131 [ps:P131 wd:Q1142326].
            BIND("municipality" AS ?landmarkType)
        }
        {
            VALUES (?timeProp ?timeType) { (pqv:P580 "start") (pqv:P582 "end") }
            ?landmarkId p:P1448 ?statement.
            ?statement ps:P1448 ?nomOff ; ?timeProp [wb:timeValue ?timeStamp ; wb:timePrecision ?timePrec ; wb:timeCalendarModel ?timeCal].
            BIND(LANG(?nomOff) AS ?lang)
        }UNION
        {
            VALUES (?prop ?timeProp ?timeType) { (p:P571 psv:P571 "start") (p:P576 psv:P576 "end") }
            ?landmarkId wdt:P361 wd:Q107311481 ; rdfs:label ?nomOff ; ?prop ?statement.
            BIND("fr" AS ?lang)
            FILTER (LANG(?nomOff) = ?lang)
            FILTER NOT EXISTS {?landmarkId p:P1448 ?nomOffSt}
            ?statement ?timeProp [wb:timeValue ?timeStamp ; wb:timePrecision ?timePrec ; wb:timeCalendarModel ?timeCal] .
        }
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
    OPTIONAL {?statement pq:P580 ?dateStartStamp; pqv:P580 [wb:timeCalendarModel ?dateStartCal ; wb:timePrecision ?dateStartPrec]}
    OPTIONAL {?statement pq:P582 ?dateEndStamp; pqv:P582 [wb:timeCalendarModel ?dateEndCal; wb:timePrecision ?dateEndPrec]}
    BIND("within" AS ?landmarkRelationType)
    }
    """

    query = wd.save_select_query_as_csv_file(query, out_csv_file)

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

################################################################ States ###########################################################

def create_graph_from_states(states_json_file:str):
    """
    Creation of a graph from the JSON file which contains a list of different states
    """

    # Creation of a basic graph with rdflib
    states_descriptions = fm.read_json_file(states_json_file)
    
    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_state_descriptions(states_descriptions)
    np.bind_namespaces(g)

    return g

##################################################### BAN ##########################################################

def create_graph_from_paris_ban(ban_file:str, valid_time:dict, source:dict, lang:str):
    """
    Creation of a graph from the BAN file
    """

    ban_pref, ban_ns = "ban", Namespace("https://adresse.data.gouv.fr/base-adresse-nationale/")

    ban_description = cfd.create_state_description_for_ban(ban_file, valid_time, source, lang, ban_ns) 
    g = sej.create_graph_from_state_descriptions(ban_description)
    g.bind(ban_pref, ban_ns)
    np.bind_namespaces(g)

    return g

##################################################### OSM ##########################################################

def create_graph_from_osm(osm_file:str, osm_hn_file:str, valid_time:dict, source:dict, lang:str):
    osm_pref, osm_ns = "osm", Namespace("https://www.openstreetmap.org/")
    osm_rel_pref, osm_rel_ns = "osmRel", Namespace("https://www.openstreetmap.org/relation/")

    osm_description = cfd.create_state_description_for_osm(osm_file, osm_hn_file, valid_time, source, lang, osm_ns)
    g = sej.create_graph_from_state_descriptions(osm_description)
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
    g = sej.create_graph_from_state_descriptions(state_vpa_description)
    g += sej.create_graph_from_event_descriptions(event_vpa_description)
    g += sej.create_graph_from_event_descriptions(event_vpc_description)
    g.bind(vpa_pref, vpa_ns)
    g.bind(vpc_pref, vpc_ns)
    np.bind_namespaces(g)

    return g

##################################################### Geojson ##########################################################

def create_graph_from_geojson_states_of_thoroughfares(geojson_file:str, lang, valid_time:dict, source:dict, name_attribute:str, identity_property:str=None):
    lm_type = "thoroughfare"
    state_description = cfd.create_state_description_for_geojson_states(geojson_file, lm_type, name_attribute, identity_property, lang, valid_time, source)

    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_state_descriptions(state_description)
    np.bind_namespaces(g)

    return g

def create_graph_from_geojson_states_of_streetnumbers_from_addresses(geojson_file:str, lang, valid_time:dict, source:dict, name_attribute:str):
    state_description = cfd.create_state_description_for_geojson_states_of_streetnumbers(geojson_file, streetnumber_and_thoroughfare_name_attribute=name_attribute,
                                                                                         lang=lang, time_description=valid_time, source_description=source)

    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_state_descriptions(state_description)
    np.bind_namespaces(g)

    return g

def create_graph_from_geojson_states_of_streetnumbers(geojson_file:str, lang, valid_time:dict, source:dict, streetnumber_name_attribute:str, thoroughfare_name_attribute:str):
    state_description = cfd.create_state_description_for_geojson_states_of_streetnumbers(geojson_file, streetnumber_name_attribute, thoroughfare_name_attribute,
                                                                                         lang=lang, time_description=valid_time, source_description=source)

    # Creation of a basic graph with rdflib
    g = sej.create_graph_from_state_descriptions(state_description)
    np.bind_namespaces(g)

    return g
