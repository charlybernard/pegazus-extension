from scripts.graph_construction.namespaces import NameSpaces

np = NameSpaces()

################################################## Generate descriptions ######################################################

def create_landmark_version_description(
        lm_id:str, lm_label:str, lm_type:str, lang:str,
        lm_attributes:dict=None, lm_provenance:dict=None, time_description:dict=None):
    """
    Create a landmark version description
    """

    description = {}

    if lm_id is not None:
        description["id"] = lm_id
    
    if lm_label is not None:
        description["label"] = lm_label
        if lang is not None:
            description["lang"] = lang

    if lm_type is not None:
        description["type"] = lm_type

    if isinstance(lm_attributes, dict):
        description["attributes"] = lm_attributes
        
    if isinstance(lm_provenance, dict):
        description["provenance"] = lm_provenance

    if isinstance(time_description, dict):
        description["time"] = time_description

    return description

def create_landmark_relation_description(
        lr_id:str, lr_type:str,
        locatum_id:str, relatum_ids:list[str],
        lm_provenance:dict=None, time_description:dict=None,
        changes:list=None):
    """
    Create a landmark relation description
    """

    description = {}

    if lr_id is not None:
        description["id"] = lr_id
    
    if lr_type is not None:
        description["type"] = lr_type

    if locatum_id is not None:
        description["locatum"] = locatum_id
        
    if isinstance(relatum_ids, list):
        description["relatum"] = relatum_ids

    if isinstance(lm_provenance, dict):
        description["provenance"] = lm_provenance

    if isinstance(time_description, dict):
        description["time"] = time_description

    if isinstance(changes, list) and len(changes) != 0:
        description["changes"] = changes

    return description


def create_landmark_attribute_version_description(value:str, lang:str=None, datatype:str=None):
    description = None
    if value is not None:
        description = {"value":value}
        if lang is not None:
            description["lang"] = lang
        if datatype is not None:
            description["datatype"] = datatype
    
    return description

def create_address_description(addr_uuid:str, addr_label:str, lang:str, target_uuid:str, segment_uuids:list[str], lm_provenance:dict=None):
    """
    Create an address description
    """
    description = {}

    if addr_uuid is not None:
        description["id"] = addr_uuid
    if addr_label is not None:
        description["label"] = addr_label
        if lang is not None:
            description["lang"] = lang
    if target_uuid is not None:
        description["target"] = target_uuid
    if isinstance(segment_uuids, list):
        description["segments"] = segment_uuids
    if isinstance(lm_provenance, dict):
        description["provenance"] = lm_provenance
    
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
        "on": "relation", 
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