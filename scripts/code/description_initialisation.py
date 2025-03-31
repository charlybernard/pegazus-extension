from rdflib import Graph, Literal, URIRef, Namespace, XSD
from namespaces import NameSpaces


np = NameSpaces()

################################################## Generate descriptions ######################################################

def create_landmark_version_description(lm_id, lm_label, lm_type:str, lang:str, lm_attributes:dict, lm_provenance:dict, time_description:dict):
    """
    Create a landmark version description
    """

    description = {
        "id": lm_id,
        "label": lm_label,
        "type": lm_type,
        "lang": lang,
        "attributes": lm_attributes,
        "provenance": lm_provenance,
        "time": time_description
    }

    return description

def create_landmark_relation_version_description(lr_id, lr_type:str, locatum_id:str, relatum_ids:list[str], lm_provenance:dict, time_description:dict):
    """
    Create a landmark relation description
    """
    description = {
        "id": lr_id,
        "type": lr_type,
        "locatum": locatum_id,
        "relatum": relatum_ids,
        "provenance": lm_provenance,
        "time": time_description
    }

    return description

def create_address_description(addr_uuid:str, addr_label:str, lang:str, target_uuid:str, segment_uuids:list[str], lm_provenance:dict):
    """
    Create an address description
    """
    description = {
        "id": addr_uuid,
        "label": addr_label,
        "lang": lang,
        "target": target_uuid,
        "segments": segment_uuids,
        "provenance": lm_provenance,
    }

    return description

def create_event_description(label:str, lang:str, landmarks:list, relations:list, provenance:dict, time_description:dict):
    """
    Create an event description
    """

    description = {
        "time": time_description, 
        "lang": lang,
        "landmarks": landmarks,
        "relations": relations,
        "provenance": provenance
    }
    if label is not None:
        description["label"] = label

    return description

def create_landmark_event_description(lm_id:str, lm_type:str, lm_label:str, lm_lang:str, changes:list=None):
    """
    Create a landmark event description
    """

    description = {
        "id": lm_id, 
        "label": lm_label,
        "lang": lm_lang,
        "type": lm_type,
    }

    if isinstance(changes, list) and len(changes) != 0:
        description["changes"] = changes

    return description

def create_landmark_change_event_description(cg_type:str):
    """
    Create a landmark change event description
    """

    description = {
        "on": "landmark", 
        "type": cg_type
    }

    return description

def create_landmark_relation_change_event_description(cg_type:str):
    """
    Create a landmark relation change event description
    """

    description = {
        "on": "landmark_relation", 
        "type": cg_type
    }

    return description

def create_landmark_attribute_change_event_description(attr_type:str, makes_effective:list=None, outdates:list=None):
    """
    Create a landmark attribute change event description
    """

    description = {
        "on": "attribute", 
        "attribute": attr_type
    }

    if isinstance(makes_effective, list) and len(makes_effective) != 0:
        description["makes_effective"] = makes_effective

    if isinstance(outdates, list) and len(outdates) != 0:
        description["outdates"] = outdates

    return description