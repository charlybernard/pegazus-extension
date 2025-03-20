from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, SKOS
from namespaces import NameSpaces
import graphrdf as gr
import str_processing as sp
import time_processing as tp

np = NameSpaces()

############################################## Resource creation ##########################################################

# This section aims at creating some resources such as : Landmark, LandmarkRelation, Attribute, AttributeVersion, Change, Event, TemporalEntity...

######### Landmark management #########
# Functions to manage with landmarks 

def create_landmark(g:Graph, landmark_uri:URIRef, label:Literal, landmark_type:URIRef):
    g.add((landmark_uri, RDF.type, np.ADDR["Landmark"]))
    g.add((landmark_uri, np.ADDR["isLandmarkType"], landmark_type))
    if label is not None:
        g.add((landmark_uri, RDFS.label, label))

def create_landmark_version(g:Graph, lm_uri:URIRef, lm_type_uri:URIRef, lm_label:str, attr_types_and_values:list[list], time_description:dict, provenance_uri:URIRef, factoids_namespace:Namespace, lang:str):
    if lm_label is not None:
        lm_label_lit = gr.get_name_literal(lm_label, lang)
    else:
        lm_label_lit = None
    create_landmark(g, lm_uri, lm_label_lit, lm_type_uri)

    for attr in attr_types_and_values:
        attr_type_uri, attr_value_lit = attr
        attr_uri, attr_version_uri = gr.generate_uri(factoids_namespace, "ATTR"), gr.generate_uri(factoids_namespace, "AV")
        create_landmark_attribute_and_version(g, lm_uri, attr_uri, attr_type_uri, attr_version_uri, attr_value_lit)

        # Add provenance (if supplied)
        if provenance_uri is not None:
            add_provenance_to_resource(g, attr_version_uri, provenance_uri)

        # If the attribute is of type `Name`, we add alternative labels to its versions.
        if attr_type_uri in [np.ATYPE["Name"]]:
            attr_value_lit_label, attr_value_lit_lang = attr_value_lit.value, attr_value_lit.language
            add_other_labels_for_resource(g, attr_version_uri, attr_value_lit_label, attr_value_lit_lang, lm_type_uri)

    # Adding alternative labels for the landmark
    add_other_labels_for_resource(g, lm_uri, lm_label, lang, lm_type_uri)
    add_validity_time_interval_to_landmark(g, lm_uri, time_description)

    if provenance_uri is not None:
        add_provenance_to_resource(g, lm_uri, provenance_uri)

######### Landmark relation management #########
# Functions to manage with landmark relations 

def create_landmark_relation(g:Graph, landmark_relation_uri:URIRef, landmark_relation_type:URIRef,
                             locatum_uri:URIRef, relatum_uris:list[URIRef],  is_address_segment=False, is_final_address_segment=False):
    lr_class = "LandmarkRelation"
    if is_final_address_segment:
        lr_class = "FinalAddressSegment"
    elif is_address_segment:
        lr_class = "AddressSegment"

    g.add((landmark_relation_uri, RDF.type, np.ADDR[lr_class]))
    g.add((landmark_relation_uri, np.ADDR["isLandmarkRelationType"], landmark_relation_type))
    g.add((landmark_relation_uri, np.ADDR["locatum"], locatum_uri))
    for rel_uri in relatum_uris:
        g.add((landmark_relation_uri, np.ADDR["relatum"], rel_uri))

######### Change / Event management #########
# Functions to manage with changes and events

def create_event(g:Graph, event_uri:URIRef):
    g.add((event_uri, RDF.type, np.ADDR["Event"]))

def create_event_with_time(g:Graph, event_uri:URIRef, time_uri:URIRef):
    create_event(g, event_uri)
    add_time_to_resource(g, event_uri, time_uri)

def create_change(g:Graph, change_uri:URIRef, change_type_uri:URIRef, change_class="Change"):
    g.add((change_uri, RDF.type, np.ADDR[change_class]))
    if change_type_uri is not None:
        g.add((change_uri, np.ADDR["isChangeType"], change_type_uri))

def create_change_event_relation(g:Graph, change_uri:URIRef, event_uri:URIRef):
    g.add((change_uri, np.ADDR["dependsOn"], event_uri))

def create_attribute_change(g:Graph, change_uri:URIRef, change_type_uri:URIRef, attribute_uri:URIRef,
                            made_effective_versions_uris:list[URIRef]=[], outdated_versions_uris:list[URIRef]=[]):
    create_change(g, change_uri, change_type_uri, change_class="AttributeChange")
    g.add((change_uri, np.ADDR["appliedTo"], attribute_uri))

    for version in made_effective_versions_uris:
        g.add((change_uri, np.ADDR["makesEffective"], version))
    for version in outdated_versions_uris:
        g.add((change_uri, np.ADDR["outdates"], version))

def create_landmark_change(g:Graph, change_uri:URIRef, change_type_uri:URIRef, landmark_uri:URIRef):
    create_change(g, change_uri, change_type_uri, change_class="LandmarkChange")
    g.add((change_uri, np.ADDR["appliedTo"], landmark_uri))

def create_landmark_relation_change(g:Graph, change_uri:URIRef, change_type_uri:URIRef, landmark_relation_uri:URIRef):
    create_change(g, change_uri, change_type_uri, change_class="LandmarkRelationChange")
    g.add((change_uri, np.ADDR["appliedTo"], landmark_relation_uri))

def create_landmark_with_changes(g:Graph, landmark_uri:URIRef, label:str, lang:str, landmark_type:URIRef,
                                resource_namespace:Namespace):
    label_lit = gr.get_name_literal(label, lang)
    create_landmark(g, landmark_uri, label_lit, landmark_type)
    creation_change_uri, creation_event_uri = gr.generate_uri(resource_namespace, "CH"), gr.generate_uri(resource_namespace, "EV")
    dissolution_change_uri, dissolution_event_uri = gr.generate_uri(resource_namespace, "CH"), gr.generate_uri(resource_namespace, "EV")

    change_type_landmark_appearance = np.CTYPE["LandmarkAppearance"]
    change_type_landmark_disappearance = np.CTYPE["LandmarkDisappearance"]
    create_landmark_change(g, creation_change_uri, change_type_landmark_appearance, landmark_uri)
    create_landmark_change(g, dissolution_change_uri,change_type_landmark_disappearance, landmark_uri)
    create_event(g, creation_event_uri)
    create_event(g, dissolution_event_uri)
    create_change_event_relation(g, creation_change_uri, creation_event_uri)
    create_change_event_relation(g, dissolution_change_uri, dissolution_event_uri)

######### Attribute management #########
# Functions to manage with attributes of landmarks

def create_attribute(g:Graph, attribute_uri:URIRef, attribute_type:URIRef):
    g.add((attribute_uri, RDF.type, np.ADDR["Attribute"]))
    g.add((attribute_uri, np.ADDR["isAttributeType"], attribute_type))

def create_landmark_attribute(g:Graph, attribute_uri:URIRef, attribute_type_uri:URIRef, landmark_uri:URIRef):
    create_attribute(g, attribute_uri, attribute_type_uri)
    g.add((landmark_uri, np.ADDR["hasAttribute"], attribute_uri))

def create_attribute_version(g:Graph, attr_vers_uri:URIRef, vers_value:Literal):
    g.add((attr_vers_uri, RDF.type, np.ADDR["AttributeVersion"]))
    g.add((attr_vers_uri, np.ADDR["versionValue"], vers_value))

def add_version_to_attribute(g:Graph, attribute_uri:URIRef, attr_vers_uri:URIRef):
    g.add((attribute_uri, np.ADDR["hasAttributeVersion"], attr_vers_uri))

def create_landmark_attribute_and_version(g:Graph, landmark_uri:URIRef, attribute_uri:URIRef, attribute_type_uri:URIRef,
                                          attribute_version_uri:URIRef, attribute_version_value:Literal):
    create_landmark_attribute(g, attribute_uri, attribute_type_uri, landmark_uri)
    create_attribute_version(g, attribute_version_uri, attribute_version_value)
    add_version_to_attribute(g, attribute_uri, attribute_version_uri)

def create_attribute_version_with_changes(g:Graph, attribute_uri:URIRef, value:Literal, resource_namespace:Namespace,
                                          change_outdates_uri=None, change_makes_effective_uri=None):
    
    attr_vers_uri = gr.generate_uri(resource_namespace, "AV")
    create_attribute_version(g, attr_vers_uri, value)
    add_version_to_attribute(g, attribute_uri, attr_vers_uri)

    if change_makes_effective_uri is None:
        makes_effective_change_uri, makes_effective_event_uri = gr.generate_uri(resource_namespace, "CH"), gr.generate_uri(resource_namespace, "EV")
        create_attribute_change(g, makes_effective_change_uri, attribute_uri)
        create_event(g, makes_effective_event_uri)
        create_change_event_relation(g, makes_effective_change_uri, makes_effective_event_uri)
    if change_outdates_uri is None:
        outdates_change_uri, outdates_event_uri = gr.generate_uri(resource_namespace, "CH"), gr.generate_uri(resource_namespace, "EV")
        create_attribute_change(g, outdates_change_uri, attribute_uri)
        create_event(g, outdates_event_uri)
        create_change_event_relation(g, outdates_change_uri, outdates_event_uri)

    g.add((outdates_change_uri, np.ADDR["outdates"], attr_vers_uri))
    g.add((makes_effective_change_uri, np.ADDR["makesEffective"], attr_vers_uri))
    

######### Address management #########
# Function to manage with addresses 

def create_address(g:Graph, address_uri:URIRef, address_label:str, address_lang:str, address_segments_list:list[URIRef], target_uri:URIRef):
    label_lit = Literal(address_label, lang=address_lang)
    g.add((address_uri, RDF.type, np.ADDR["Address"]))
    g.add((address_uri, RDFS.label, label_lit))
    g.add((address_uri, np.ADDR["targets"], target_uri))
    g.add((address_uri, np.ADDR["firstStep"], address_segments_list[0]))

    prev_addr_seg = address_segments_list[0]
    for addr_seg in address_segments_list[1:]:
        g.add((prev_addr_seg, np.ADDR["nextStep"], addr_seg))
        prev_addr_seg = addr_seg


######### Time management #########
# Function to manage with temporal entities

def create_crisp_time_instant(g:Graph, time_uri:URIRef, time_stamp:Literal, time_calendar:URIRef, time_precision:URIRef):
    g.add((time_uri, RDF.type, np.ADDR["CrispTimeInstant"]))
    g.add((time_uri, np.ADDR["timeStamp"], time_stamp))
    g.add((time_uri, np.ADDR["timeCalendar"], time_calendar))
    g.add((time_uri, np.ADDR["timePrecision"], time_precision))

def create_crisp_time_interval(g:Graph, time_uri:URIRef, start_time_uri:URIRef, end_time_uri:URIRef):
    g.add((time_uri, RDF.type, np.ADDR["CrispTimeInterval"]))
    g.add((time_uri, np.ADDR["hasBeginning"], start_time_uri))
    g.add((time_uri, np.ADDR["hasEnd"], end_time_uri))

def add_time_to_resource(g:Graph, resource_uri:URIRef, time_uri):
    g.add((resource_uri, np.ADDR["hasTime"], time_uri))

def add_validity_time_interval_to_landmark(g:Graph, lm_uri:URIRef, time_description:dict):
    start_time_stamp, start_time_calendar, start_time_precision = tp.get_time_instant_elements(time_description.get("start_time"))
    end_time_stamp, end_time_calendar, end_time_precision = tp.get_time_instant_elements(time_description.get("end_time"))
    time_interval_uri, start_time_uri, end_time_uri = gr.generate_uri(np.FACTOIDS, "TI"), gr.generate_uri(np.FACTOIDS, "TI"), gr.generate_uri(np.FACTOIDS, "TI")

    create_crisp_time_instant(g, start_time_uri, start_time_stamp, start_time_calendar, start_time_precision)
    create_crisp_time_instant(g, end_time_uri, end_time_stamp, end_time_calendar, end_time_precision)
    create_crisp_time_interval(g, time_interval_uri, start_time_uri, end_time_uri)
    add_time_to_resource(g, lm_uri, time_interval_uri)


######### Source management #########
# Function to manage with sources and provenance

def create_prov_entity(g:Graph, prov_uri:URIRef):
    g.add((prov_uri, RDF.type, np.PROV.Entity))

def add_provenance_to_resource(g:Graph, resource_uri:URIRef, prov_uri:URIRef):
    g.add((resource_uri, np.PROV.wasDerivedFrom, prov_uri))

########################################## Refine data ##########################################

#Refine data by adding hidden label of alt label for example

def add_other_labels_for_resource(g:Graph, res_uri:URIRef, res_label_value:str, res_label_lang:str, res_type_uri:URIRef):
    if res_type_uri == np.LTYPE["Thoroughfare"]:
        res_label_type = "thoroughfare"
    elif res_type_uri in [np.LTYPE["Municipality"], np.LTYPE["District"]]:
        res_label_type = "area"
    elif res_type_uri in [np.LTYPE["HouseNumber"],np.LTYPE["StreetNumber"],np.LTYPE["DistrictNumber"],np.LTYPE["PostalCodeArea"]]:
        res_label_type = "housenumber"
    else:
        res_label_type = None

    # Adding alternative and hidden labels
    pref_label, hidden_label = sp.normalize_and_simplify_name_version(res_label_value, res_label_type, res_label_lang)

    if pref_label is not None:
        pref_label_lit = Literal(pref_label, lang=res_label_lang)
        g.add((res_uri, SKOS.prefLabel, pref_label_lit))

    if hidden_label is not None:
        hidden_label_lit = Literal(hidden_label, lang=res_label_lang)
        g.add((res_uri, SKOS.hiddenLabel, hidden_label_lit))