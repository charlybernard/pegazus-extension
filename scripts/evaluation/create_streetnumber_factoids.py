from uuid import uuid4
import json
import pandas as pd
import os
from rdflib import URIRef
from scripts.evaluation import data_from_sparql_queries as dfsq
from scripts.evaluation import evaluation_aux as ea


def create_streetnumber_fragmentary_descriptions(
    graphdb_url:URIRef,
    repository_name:str,
    facts_named_graph_name:str,
    data_folder:str,
    data_sources_folder:str,
    geometry_settings:dict
):

    epsg_code = geometry_settings.get("epsg_code", "EPSG:2154")
    max_distance = geometry_settings.get("max_distance", 10)

    # Get file paths
    sn_labels_file = os.path.join(data_folder, "streetnumber_labels_final_graph.csv")
    sn_attr_version_valid_times_file = os.path.join(data_folder, "streetnumber_attr_geom_version_valid_times_final_graph.csv")
    sn_attr_version_values_file = os.path.join(data_folder, "streetnumber_attr_geom_version_values_final_graph.csv")
    sn_attr_change_valid_times_file = os.path.join(data_folder, "streetnumber_attr_geom_change_valid_times_final_graph.csv")
    sn_version_descriptions_file = os.path.join(data_sources_folder, "fragmentary_states_streetnumbers.json")
    sn_change_descriptions_file = os.path.join(data_sources_folder, "fragmentary_events_streetnumbers.json")

    # Create the CSV files from the SPARQL queries
    dfsq.select_streetnumbers_labels(graphdb_url, repository_name, facts_named_graph_name, sn_labels_file)
    dfsq.select_streetnumbers_attr_geom_version_valid_times(graphdb_url, repository_name, facts_named_graph_name, sn_attr_version_valid_times_file)
    dfsq.select_streetnumbers_attr_geom_version_values(graphdb_url, repository_name, facts_named_graph_name, sn_attr_version_values_file)
    dfsq.select_streetnumbers_attr_geom_change_valid_times(graphdb_url, repository_name, facts_named_graph_name, sn_attr_change_valid_times_file)

    # Read the CSV files
    labels = pd.read_csv(sn_labels_file)
    version_valid_times = pd.read_csv(sn_attr_version_valid_times_file)
    version_values = pd.read_csv(sn_attr_version_values_file)
    changes_valid_times = pd.read_csv(sn_attr_change_valid_times_file)

    # Take a sample of 60% of the data
    versions_sample = version_valid_times.sample(frac=0.6)
    changes_sample = changes_valid_times.sample(frac=0.6)

    # For each street number, take a random sample of 1 label
    random_labels_for_versions = labels.groupby("sn").sample(n=1, random_state=42)
    random_labels_for_changes = labels.groupby("sn").sample(n=1, random_state=42)

    #####################################################################################

    # Group all version values by their version, and create a list of version values for each version
    grouped_version_values = version_values.groupby("attrVersion")["versionValue"].apply(list).to_dict()
    version_values_dict = ea.get_random_geometry_for_street_number(grouped_version_values, epsg_code, max_distance=max_distance)

    # Generate new valid time contained in old valid time for each version value
    ea.generate_random_dates_for_versions(versions_sample)

    # Generate new changes coherent with generated ones 
    ea.generate_random_dates_for_changes(changes_sample)

    # Add a new column "versionValue" to the version_valid_times dataframe
    versions_sample["versionValue"] = versions_sample["attrVersion"].map(version_values_dict)

    # Merge the two dataframes on the "sn" column
    out_versions = pd.merge(versions_sample, random_labels_for_versions, on="sn", how="inner")
    out_changes = pd.merge(changes_sample, random_labels_for_changes, on="sn", how="inner")

    ####################################################################################

    versions_desc = create_version_descriptions(out_versions)
    changes_desc = create_change_descriptions(out_changes)

    # Exporter dans un fichier JSON
    with open(sn_version_descriptions_file, 'w', encoding='utf-8') as f:
        json.dump(versions_desc, f, ensure_ascii=False, indent=4)

    # Exporter dans un fichier JSON
    with open(sn_change_descriptions_file, 'w', encoding='utf-8') as f:
        json.dump(changes_desc, f, ensure_ascii=False, indent=4)

#####################################################################################################

def create_version_descriptions(factoids):
    source = {
        "uri": "http://example.org/factoids",
        "label": "Factoïdes générés pour les numéros de rue",
        "lang": "fr"
    }
    all_descriptions = {"landmarks": [], "relations": [], "source": source}

    for _, row in factoids.iterrows():
        lm_descriptions, lr_description = create_street_number_state_description(
            sn_label=row["snLabel"],
            th_label=row["thLabel"],
            geom_value=row["versionValue"],
            start_time_stamp=row["startTime"],
            end_time_stamp=row["endTime"],
            lang="fr",
            provenance_uri=row["attrVersion"],
        )
        all_descriptions["landmarks"] += lm_descriptions
        all_descriptions["relations"].append(lr_description)
    
    return all_descriptions

def create_change_descriptions(factoids):
    all_descriptions = []
    for _, row in factoids.iterrows():
        desc = create_streetnumber_attr_geom_change_descriptions(
            sn_label=row["snLabel"],
            th_label=row["thLabel"],
            time_stamp=row["time"],
            lang="fr",
            provenance_uri=row["change"],
        )
        all_descriptions.append(desc)
    
    return {"events": all_descriptions}


def create_streetnumber_attr_geom_change_descriptions(sn_label, th_label, time_stamp, lang, provenance_uri):
    description = {
        "landmarks": [
            {
                "id": 1,
                "label": sn_label,
                "type": "street_number",
                "changes": [
                    {
                    "on": "attribute",
                    "attribute": "geometry"
                    }
                ]
            },
            {
                "id": 2,
                "label": th_label,
                "type": "thoroughfare",
                "lang": lang,
            },
        ],
        "relations": [
            {
                "type": "belongs",
                "id": 1,
                "locatum": 1,
                "relatum": [2],
            },
        ],
        "lang": lang,
        "time": {
        "stamp": time_stamp,
        "calendar": "gregorian",
        "precision": "day"
      },
      "provenance": {
            "uri": provenance_uri,
            "label": f"Factoïde issu d'un changement de géométrie de '{sn_label}, {th_label}'",
            "lang": "fr"
      }
    }
    
    return description
    

def create_street_number_state_description(sn_label, th_label, geom_value, start_time_stamp, end_time_stamp, lang, provenance_uri):
    sn_uuid, th_uuid = str(uuid4()), str(uuid4())

    time_description = {
            "start": {
                "stamp": start_time_stamp,
                "calendar": "gregorian",
                "precision": "day"
            },
            "end": {
                "stamp": end_time_stamp,
                "calendar": "gregorian",
                "precision": "day"
            }
        }

    lm_descriptions = [
        {
            "id": sn_uuid,
            "label": sn_label,
            "type": "street_number",
            "attributes": {
                "geometry": {
                    "value": geom_value,
                    "datatype": "wkt_literal"
                },
                "name": {
                    "value": sn_label
                }
            },
            "time": time_description,
            "provenance": {
                "uri": provenance_uri,
                "label": f"Factoïde issu d'une version de géométrie de '{sn_label}, {th_label}'",
                "lang": "fr"
            }
        },
        {
            "id": th_uuid,
            "label": th_label,
            "lang": "fr",
            "type": "thoroughfare",
            "attributes": {
                "name": {
                    "value": th_label,
                    "lang": lang,
                }
            },
            "time": time_description,
            "provenance": {
                "uri": provenance_uri,
            }
        }
        ]

    lr_description = {
        "type": "belongs",
        "id": 1,
        "locatum": sn_uuid,
        "relatum": [th_uuid],
        "time": time_description,
    }

    return lm_descriptions, lr_description