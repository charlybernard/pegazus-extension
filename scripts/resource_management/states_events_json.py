from rdflib import Graph, Literal, URIRef
from scripts.graph_construction.namespaces import NameSpaces, OntologyMapping
from scripts.utils import time_processing as tp
from scripts.graph_construction import graphrdf as gr
from scripts.resource_management import resource_initialisation as ri

np = NameSpaces()
om = OntologyMapping()

############################################################ Functions for event and version creation ############################################################

def get_provenance_uri(provenance_description:dict):
    prov_uri_str = provenance_description.get("uri")
    if prov_uri_str is not None:
        prov_uri = URIRef(prov_uri_str)
    else:
        prov_uri = gr.generate_uri(np.FACTS, "PROV")
    return prov_uri

def create_provenance(g, provenance_description:dict):
    prov_uri = get_provenance_uri(provenance_description)
    ri.create_prov_entity(g, prov_uri)

    prov_label = provenance_description.get("label")
    prov_lang = provenance_description.get("lang")
    if prov_label is not None:
        prov_label_lit = gr.get_literal_with_lang(prov_label, prov_lang)
        g.add((prov_uri, np.RDFS.label, prov_label_lit))

    return prov_uri

def create_time_instant(g:Graph, time_uri:URIRef, time_description:dict):
    stamp, calendar, precision = tp.get_time_instant_elements(time_description)
    ri.create_crisp_time_instant(g, time_uri, stamp, calendar, precision)

def create_time_interval(g:Graph, time_uri:URIRef, time_description:dict):
    start_time, end_time = time_description.get("start"), time_description.get("end")
    start_time_uri, end_time_uri = gr.generate_uri(np.FACTOIDS, "TI"), gr.generate_uri(np.FACTOIDS, "TI")

    create_time_instant(g, start_time_uri, start_time)
    create_time_instant(g, end_time_uri, end_time)
    ri.create_crisp_time_interval(g, time_uri, start_time_uri, end_time_uri)

def get_attribute_version_value(attribute_version_description:dict):
    
    value = attribute_version_description.get("value")
    lang = attribute_version_description.get("lang")
    datatype = attribute_version_description.get("datatype")
    datatype_url = om.get_datatype(datatype)

    if lang is not None:
        version_val = gr.get_literal_with_lang(value, lang)
    elif datatype_url is not None:
        version_val = gr.get_literal_with_datatype(value, datatype_url)
    else:
        version_val = gr.get_literal_without_option(value)

    return version_val

############################################################## Source creation ############################################################

def create_source_from_description(g:Graph, description:dict):
    """
    Creating source from a description which is a dictionary
    """

    # Get the source URI 
    source_uri_str = description.get("uri")
    source_uri = URIRef(source_uri_str) if source_uri_str is not None else gr.generate_uri(np.FACTS, "SRC")

    # Get the source label and language
    lang, label, comment = description.get("lang"), description.get("label"), description.get("comment")
    label_lit = gr.get_literal_with_lang(label, lang)
    comment_lit = gr.get_literal_with_lang(comment, lang)

    # Initialise the source
    ri.create_source(g, source_uri, label_lit, comment_lit)
    
    # Add the publisher to the source (if exists)
    publisher_desc = description.get("publisher")
    if isinstance(publisher_desc, dict):
        publisher_uri = create_publisher_from_description(g, publisher_desc, lang)
        ri.add_publisher_to_source(g, source_uri, publisher_uri)

    return source_uri

def create_publisher_from_description(g:Graph, description:dict, lang:str=None):
    """
    Creating publisher from a description which is a dictionary
    """

    # Get the publisher URI
    publisher_uri_str = description.get("uri")
    publisher_uri = URIRef(publisher_uri_str) if publisher_uri_str is not None else gr.generate_uri(np.FACTS, "PUB")
    publisher_label = description.get("label")
    publisher_lang = description.get("lang") if description.get("lang") is not None else lang
    publisher_label_lit = gr.get_literal_with_lang(publisher_label, publisher_lang)

    # Create the publisher
    ri.create_publisher(g, publisher_uri, publisher_label_lit)

    return publisher_uri

############################################################ Event creation ####################################################################

def create_graph_from_event_descriptions(descriptions:list[dict]):
    events_desc = descriptions.get("events")
    source_desc = descriptions.get("source")
    g = Graph()

    source_uri = None
    if isinstance(source_desc, dict):
        source_uri = create_source_from_description(g, source_desc)

    for desc in events_desc:
        create_graph_from_event_description(g, desc, source_uri)

    return g

def create_graph_from_event_description(g:Graph, event_description:dict, source_uri:URIRef=None):
    """
    Generate a graph describing an event from a description which is a dictionary
    Example of event_description

    ```
    event_description = {
        "time": {"stamp":"1851-02-18", "calendar":"gregorian", "precision":"day"},
        "label": "Par arrêté municipal du 30 août 1978, sa portion orientale, de la rue Bobillot à la rue du Moulin-des-Prés, prend le nom de rue du Père-Guérin.",
        "lang": "fr",
        "landmarks": [
            {"id":1, "label":"rue du Père Guérin", "lang":"fr, "type":"thoroughfare", "changes":[
                {"on":"landmark", "type":"appearance"},
                {"on":"attribute", "attribute":"geometry"},
                {"on":"attribute", "attribute":"name", "makes_effective":[{"value":"rue du Père Guérin", "lang":"fr"}]}
            ]},
            {"id":2, "label":"rue Gérard", "lang":"fr, "type":"thoroughfare", "changes":[
                {"on":"attribute", "attribute":"Geometry"},
            ]}
        ],
        "relations": [
            {"type":"touches", "locatum":1, "relatum":2}, , "changes":[
                {"on":"relation", "type":"apprearance"},
        ], 
        "provenance": {"uri": "https://fr.wikipedia.org/wiki/Rue_G%C3%A9rard_(Paris)"}
    }
    ```
    """

    event_uri = gr.generate_uri(np.FACTOIDS, "EV")
    landmark_uris = {}

    created_entities = [event_uri]
    time_description, label, lang, landmark_descriptions, landmark_relation_descriptions, provenance_description = get_event_description_elements(event_description)
    create_event_with_time(g, event_uri, time_description)

    # Create a label for the event if it exists
    if label is not None:
        ev_label = gr.get_literal_with_lang(label, lang)
        g.add((event_uri, np.RDFS.comment, ev_label))

    for desc in landmark_descriptions:
        lm_id, lm_uri = desc.get("id"), gr.generate_uri(np.FACTOIDS, "LM")
        landmark_uris[lm_id] = lm_uri
        new_entities = create_event_landmark(g, event_uri, lm_uri, desc)
        created_entities += new_entities

    for desc in landmark_relation_descriptions:
        lr_uri = gr.generate_uri(np.FACTOIDS, "LR")
        create_event_landmark_relation(g, event_uri, lr_uri, desc, landmark_uris)
        created_entities.append(lr_uri)

    prov_uri = create_provenance(g, provenance_description)
    if isinstance(prov_uri, URIRef):
        for entity in created_entities:
            ri.add_provenance_to_resource(g, entity, prov_uri)
            if isinstance(source_uri, URIRef):
                ri.add_source_to_provenance(g, prov_uri, source_uri)
                

def get_event_description_elements(event_description:dict):
    """
    Extract the elements of an event description
    """

    time_description = event_description.get("time")
    # # Check if the time description is a dictionary and contains the keys 'stamp', 'calendar' and 'precision'
    # if not isinstance(time_description, dict) or not all(key in time_description for key in ["stamp", "calendar", "precision"]):
    #     raise ValueError("The time description must contain the keys 'stamp', 'calendar' and 'precision'")

    label = event_description.get("label") or None
    
    lang = event_description.get("lang")
    # Check if the label is a string or not None
    if not isinstance(lang, str) and lang is not None:
        raise ValueError("The lang must be a string")
    
    landmark_descriptions = event_description.get("landmarks")
    # Check if the landmarks are a list of dictionaries
    if landmark_descriptions is None:
        raise ValueError("`landmarks` value is not defined, it must be a list of dictionaries")
    elif not isinstance(landmark_descriptions, list) or not all(isinstance(desc, dict) for desc in landmark_descriptions):
        raise ValueError("The landmarks must be a list of dictionaries")
    
    landmark_relation_descriptions = event_description.get("relations") or []
    # Check if the landmark relations are a list of dictionaries
    if not isinstance(landmark_relation_descriptions, list) or not all(isinstance(desc, dict) for desc in landmark_relation_descriptions):
        raise ValueError("The landmark relations must be a list of dictionaries")
    
    provenance_description = event_description.get("provenance")
    # Check if the provenance description is a dictionary
    if provenance_description is None:
        raise ValueError("`provenance` value is not defined, it must be a dictionary")
    elif not isinstance(provenance_description, dict):
        raise ValueError("The provenance description must be a dictionary")

    return time_description, label, lang, landmark_descriptions, landmark_relation_descriptions, provenance_description

def create_event_with_time(g:Graph, event_uri:URIRef, time_description:dict):
    time_uri = gr.generate_uri(np.FACTOIDS, "TI")
    if isinstance(time_description, dict):
        ri.create_event_with_time(g, event_uri, time_uri)
        create_time_instant(g, time_uri, time_description)
    else:
        ri.create_event(g, event_uri)

def create_event_landmark(g:Graph, event_uri:URIRef, landmark_uri:URIRef, landmark_description:dict):
    created_entities = [landmark_uri]
    label = landmark_description.get("label")
    lang = landmark_description.get("lang")
    label_lit = gr.get_name_literal(label, lang)
    type = landmark_description.get("type")
    type_uri = om.get_landmark_type(type)
    ri.create_landmark(g, landmark_uri, label_lit, type_uri)
    
    changes = landmark_description.get("changes") or []
    for change in changes:
        created_entities_from_change = create_event_change(g, event_uri, landmark_uri, change)
        created_entities += created_entities_from_change
        
    return created_entities

def create_event_change(g:Graph, event_uri:URIRef, landmark_uri:URIRef, change_description:dict):
    created_entities = []
    on = change_description.get("on")
    if on == "landmark":
        create_event_change_on_landmark(g, event_uri, landmark_uri, change_description)
    elif on == "attribute":
        created_versions = create_event_change_on_landmark_attribute(g, event_uri, landmark_uri, change_description)
        created_entities += created_versions

    return created_entities

def create_event_change_on_landmark(g:Graph, event_uri:URIRef, landmark_uri:URIRef, change_description:dict):
    change_types = {
        "appearance":"landmark_appearance",
        "disappearance":"landmark_disappearance",
        "numerotation":"landmark_numerotation",
        "classement":"landmark_classement",
        "unclassement":"landmark_unclassement",
    }
    
    change_uri = gr.generate_uri(np.FACTOIDS, "CG")
    change_type = change_description.get("type")
    change_type_uri = om.get_change_type(change_types.get(change_type))

    ri.create_landmark_change(g, change_uri, change_type_uri, landmark_uri)
    ri.create_change_event_relation(g, change_uri, event_uri)
    
def create_event_change_on_landmark_attribute(g:Graph, event_uri:URIRef, landmark_uri:URIRef, change_description:dict):
    change_type_uri = om.get_change_type("attribute_version_transition")
    attribute_type = change_description.get("attribute")
    attribute_type_uri = om.get_attribute_type(attribute_type)

    attribute_uri = gr.generate_uri(np.FACTOIDS, "ATTR")
    change_uri = gr.generate_uri(np.FACTOIDS, "CG")

    made_effective_versions = change_description.get("makes_effective") or []
    outdated_versions = change_description.get("outdates") or []

    made_effective_versions_uris, outdated_versions_uris = [], []
    for vers in made_effective_versions:
        vers_uri = gr.generate_uri(np.FACTOIDS, "AV")
        create_event_attribute_version(g, attribute_uri, vers_uri, vers)
        made_effective_versions_uris.append(vers_uri)

    for vers in outdated_versions:
        vers_uri = gr.generate_uri(np.FACTOIDS, "AV")
        create_event_attribute_version(g, attribute_uri, vers_uri, vers)
        outdated_versions_uris.append(vers_uri)

    created_entities = made_effective_versions_uris + outdated_versions_uris
    ri.create_landmark_attribute(g, attribute_uri, attribute_type_uri, landmark_uri)
    ri.create_attribute_change(g, change_uri, change_type_uri, attribute_uri, made_effective_versions_uris, outdated_versions_uris)
    ri.create_change_event_relation(g, change_uri, event_uri)

    return created_entities
    
def create_event_attribute_version(g:Graph, attribute_uri:URIRef, attribute_version_uri:URIRef, attribute_version_description:dict):
    version_val = get_attribute_version_value(attribute_version_description)
    ri.create_attribute_version_and_add_to_attribute(g, attribute_uri, attribute_version_uri, version_val)


def create_event_landmark_relation(g:Graph, event_uri:URIRef, landmark_relation_uri:URIRef, landmark_relation_description:dict, landmark_uris:dict):
    change_types = {"appearance":"landmark_relation_appearance", "disappearance":"landmark_relation_disappearance"}

    type = landmark_relation_description.get("type")
    type_uri = om.get_landmark_relation_type(type)

    locatum = landmark_relation_description.get("locatum")
    locatum_uri = landmark_uris.get(locatum)
    
    relatums = landmark_relation_description.get("relatum")
    if not isinstance(relatums, list):
        relatums = [relatums]
    relatum_uris = [landmark_uris.get(x) for x in relatums]

    ri.create_landmark_relation(g, landmark_relation_uri, type_uri, locatum_uri, relatum_uris)

    change_descs = landmark_relation_description.get("change") or []
    for cg_desc in change_descs:
        change_type = cg_desc.get("type")
        change_type_uri = om.get_change_type(change_types.get(change_type))
        if change_type_uri is not None:
            change_uri = gr.generate_uri(np.FACTOIDS, "CG")
            ri.create_landmark_relation_change(g, change_uri, change_type_uri, landmark_relation_uri)
            ri.create_change_event_relation(g, change_uri, event_uri)


####################################################### States creation ####################################################################

def create_graph_from_state_descriptions(states_descriptions:dict):
    """
    Generate a graph describing a set of states from a description which is a dictionary
    Example of states_descriptions
    ```
    states_descriptions = {
        "landmarks": [
            {
                "id": 1,
                "label": "23",
                "type": "street_number",
                "attributes": {
                    "geometry": {
                        "value": "POINT(2.3589 48.8394)",
                        "datatype": "wkt_literal"
                    },
                    "name": {
                        "value": "rue du Père Guérin",
                        "lang": "fr",
                    }
                },
                "provenance": {
                    "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
                }
            },
            {
                "id": 2,
                "label": "rue du Père Guérin",
                "lang": "fr",
                "type": "thoroughfare",
                "attributes": {
                    "geometry": {
                        "value": "POINT(2.3589 48.8394)",
                        "datatype": "wkt_literal"
                    },
                    "name": {
                        "value": "rue du Père Guérin",
                        "lang": "fr",
                    }
                },
                "provenance": {
                    "uri": "https://fr.wikipedia.org/wiki/Rue_G%C3%A9rard_(Paris)"
                }
            },
            {
                "id": 3,
                "label": "Paris",
                "lang": "fr",
                "type": "municipality",
                "attributes": {
                    "geometry": {
                        "value": "POINT(2.3589 48.8394)",
                        "datatype": "wkt_literal"
                    },
                    "name": {
                        "value": "Paris",
                        "lang": "fr",
                    }
                },
                "provenance": {
                    "uri": "https://fr.wikipedia.org/wiki/Paris"
                }
            }
        ],
        "relations": [
            {
                "type": "belongs",
                "id": 1,
                "locatum": 1,
                "relatum": [2],
                "provenance": {
                    "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
                }
            },
            {
                "type": "within",
                "id": 2,
                "locatum": 1,
                "relatum": [3],
                "provenance": {
                    "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
                }
            },
        ],
        "addresses": [
            {
                "label": "23 rue du Père Guérin, Paris",
                "lang": "fr",
                "target": 1,
                "segments": [1, 2],
            }
        ],
        "time": {
            "start": {
                "stamp": "1851-02-18",
                "calendar": "gregorian",
                "precision": "day"
            },
            "end": {
                "stamp": "1853-02-18",
                "calendar": "gregorian",
                "precision": "day"
            }
        },
        "source": {
            "uri": "https://fr.wikipedia.org/wiki/",
            "label": "Wikipedia",
            "lang": "fr",
            "comment": "Encyclopédie libre, universelle et collaborative",
            "publisher": {
                "uri": "https://fr.wikipedia.org/wiki/Wikipedia:À_propos",
                "label": "Contributeurs de Wikipedia"
            }
    }
    ```
    """

    g = Graph()
    landmarks, relations = {}, {}

    lm_states_descriptions = states_descriptions.get("landmarks") or []
    lm_relations_states_descriptions = states_descriptions.get("relations") or []
    addr_states_descriptions = states_descriptions.get("addresses") or []
    time_description = states_descriptions.get("time")
    source_description = states_descriptions.get("source")

    valid_time_uri = None
    # Check if the time description is a dictionary and contains the keys 'start' and 'end'
    if isinstance(time_description, dict) and {"start", "end"}.issubset(time_description.keys()):
        valid_time_uri = gr.generate_uri(np.FACTOIDS, "TI")
        create_time_interval(g, valid_time_uri, time_description)

    source_uri = None
    if isinstance(source_description, dict):
        source_uri = create_source_from_description(g, source_description)

    for desc in lm_states_descriptions:
        lm_id, lm_uri = create_landmark_version_from_description(g, desc, valid_time_uri, source_uri)
        landmarks[lm_id] = lm_uri

    for desc in lm_relations_states_descriptions:
        lr_id, lr_uri = create_landmark_relation_version_from_description(g, desc, landmarks, valid_time_uri, source_uri)
        relations[lr_id] = lr_uri

    for desc in addr_states_descriptions:
        create_address_version_from_description(g, desc, landmarks, relations)

    return g

def create_landmark_version_from_description(g:Graph, lm_state_description:dict, valid_time_uri:URIRef=None, source_uri:URIRef=None):
    """
    ```
    lm_state_description = {
        "id": 1,
        "label": "rue du Père Guérin",
        "lang": "fr",
        "type": "thoroughfare",
        "attributes": {
            "geometry": {
                "value": "POINT(2.3589 48.8394)",
                "datatype": "wkt_literal"
            },
            "name": {
                "value": "rue du Père Guérin",
                "lang": "fr",
            }
        },
        "time": {
            "start": {
                "stamp": "1851-02-18",
                "calendar": "gregorian",
                "precision": "day"
            },
            "end": {
                "stamp": "1853-02-18",
                "calendar": "gregorian",
                "precision": "day"
            }
        },
        "provenance": {
            "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
        }
    }
    ```
    """

    lm_uri = gr.generate_uri(np.FACTOIDS, "LM") # Generate a unique URI for the landmark
    lm_id = lm_state_description.get("id") # Extract the landmark ID from the version description
    lm_label = lm_state_description.get("label") # Extract the landmark label from the version description
    lm_lang = lm_state_description.get("lang") # Extract the language from the version description
    lm_type = lm_state_description.get("type") # Extract the landmark type from the version description
    lm_attributes = lm_state_description.get("attributes") # Extract the properties from the version description
    lm_provenance = lm_state_description.get("provenance") or {} # Extract the provenance from the version description
    lm_valid_time = lm_state_description.get("time") # Extract the valid time from the version description (optional)

    # Add valid time to the landmark, if it exists
    # If it does not exist, use the valid time URI if it is provided
    if lm_valid_time is not None:
        time_description = tp.get_valid_time_description(lm_valid_time) # Create a time description for the landmark
        lm_valid_time_uri = gr.generate_uri(np.FACTOIDS, "TI")
        create_time_interval(g, lm_valid_time_uri, time_description) # Create the time interval for the landmark
        ri.add_time_to_resource(g, lm_uri, lm_valid_time_uri)
    elif isinstance(valid_time_uri, URIRef):
        # Add the valid time interval to the landmark
        ri.add_time_to_resource(g, lm_uri, valid_time_uri)

    lm_type_uri = om.get_landmark_type(lm_type) # Get the URI for the landmark type
    attr_types_and_values = create_version_attribute_version(lm_attributes) # Create a dictionary of attribute types and values
    lm_provenance_uri = create_provenance(g, lm_provenance) # Get the URI for the provenance

    ri.create_landmark_with_attributes(g, lm_uri, lm_type_uri, lm_label, attr_types_and_values, lm_provenance_uri, np.FACTOIDS, lm_lang)

    # Add provenance to the landmark and source to the provenance if it exists
    if lm_provenance_uri is not None and source_uri is not None:
        ri.add_source_to_provenance(g, lm_provenance_uri, source_uri)

    return lm_id, lm_uri

def create_landmark_relation_version_from_description(g:Graph, lr_state_description:list[dict], landmark_uris:dict, valid_time_uri:URIRef=None, source_uri:URIRef=None):
    """
    ```
    lr_state_description = {
        "type": "belongs",
        "id": 1, 
        "locatum": 1,
        "relatum": [3],
        "provenance": {
            "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
        }
    }
    ```

    ```
    landmark_uris = {
        "1": "http://example.org/landmark/23RuePereGuerin",
        "2": "http://example.org/landmark/43RuePereGuerin"
        "3": "http://example.org/landmark/RuePereGuerin",
    }
    ```
    """
    lr_uri = gr.generate_uri(np.FACTOIDS, "LR") # Generate a unique URI for the landmark relation
    lr_id = lr_state_description.get("id") # Extract the landmark relation ID from the version description
    lr_type = lr_state_description.get("type") # Extract the landmark relation type from the version description
    lr_provenance = lr_state_description.get("provenance") or {} # Extract the provenance from the version description
    lr_locatum = lr_state_description.get("locatum") # Extract the landmark relation locatum from the version description
    lr_relatums = lr_state_description.get("relatum") # Extract the landmark relation relatum from the version description

    if not isinstance(lr_relatums, list):
        lr_relatums = [lr_relatums]
    
    lr_type_uri = om.get_landmark_relation_type(lr_type) # Get the URI for the landmark relation type
    lr_locatum_uri = landmark_uris.get(lr_locatum) # Get the URI for the landmark relation locatum
    lr_relatums_uris = [landmark_uris.get(x) for x in lr_relatums] # Get the URIs for the landmark relation relatum
    lr_provenance_uri = create_provenance(g, lr_provenance) # Get the URI for the provenance

    ri.create_landmark_relation(g, lr_uri, lr_type_uri, lr_locatum_uri, lr_relatums_uris)
    if lr_provenance_uri is not None:
        ri.add_provenance_to_resource(g, lr_uri, lr_provenance_uri)
        if source_uri is not None:
            ri.add_source_to_provenance(g, lr_provenance_uri, source_uri)

    if valid_time_uri is not None:
        ri.add_time_to_resource(g, lr_uri, valid_time_uri)

    return lr_id, lr_uri

def create_version_attribute_version(lm_attributes:dict):
    attr_types_and_values = []
    for attr_type, attr_value in lm_attributes.items():
        attr_type_uri = om.get_attribute_type(attr_type)
        value_lit = get_attribute_version_value(attr_value)
        attr_types_and_values.append([attr_type_uri,value_lit])

    return attr_types_and_values

def create_address_version_from_description(g:Graph, addr_state_description:list[dict], landmark_uris:dict, segment_uris:dict):
    """
    ```
    addr_state_description = {
        "label": "23 rue du Père Guérin, 75013 Paris",
        "lang": "fr",
        "target": 1,
        "segments": [1, 2, 3, 4],
        "provenance": {
            "uri": "https://fr.wikipedia.org/wiki/Rue_du_P%C3%A8re-Gu%C3%A9rin"
        }
    }
    ```

    ```
    landmark_uris = {
        "1": "http://example.org/landmark/23RuePereGuerin",
        "2": "http://example.org/landmark/43RuePereGuerin"
        "3": "http://example.org/landmark/RuePereGuerin",
        "4": "http://example.org/landmark/Paris",
        "5": "http://example.org/landmark/75013",
    }
    ```

    ```
    segment_uris = {
        "1": "http://example.org/landmark/123IsSimilarTo123",
        "2": "http://example.org/landmark/123BelongsToRuePereGuerin"
        "3": "http://example.org/landmark/123WithinParis",
        "4": "http://example.org/landmark/123Within75013"
    }
    ```
    """

    addr_uri = gr.generate_uri(np.FACTOIDS, "ADDR") # Generate a unique URI for the address
    addr_label = addr_state_description.get("label") # Extract the address label from the version description
    addr_lang = addr_state_description.get("lang") # Extract the language from the version description
    addr_target = addr_state_description.get("target") # Extract the target (it is a landmark) from the version description
    addr_segments = addr_state_description.get("segments") # Extract the segments from the version description

    addr_segments_uris = [segment_uris.get(key) for key in addr_segments]

    addr_target_uri = landmark_uris.get(addr_target) # Get the URI for the address target
    # Case when the target is not defined, create it (undefined landmark)
    if addr_target_uri is None:
        lm_uri = gr.generate_uri(np.FACTOIDS, "LM") # Generate a unique URI for the landmark
        lm_type_uri = om.get_landmark_relation_type("undefined")
        ri.create_landmark(g, lm_uri, None, lm_type_uri)

    addr_label_lit = gr.get_literal_with_lang(addr_label, addr_lang) # Create a literal for the address label
    ri.create_address(g, addr_uri, addr_label_lit, addr_segments_uris, addr_target_uri)
    
    return addr_uri

