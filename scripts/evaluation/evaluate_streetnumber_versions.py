# Import required libraries
import pandas as pd
import os
from rdflib import URIRef
from scripts.evaluation import data_from_sparql_queries as dfsq
from scripts.evaluation import evaluation_aux as ea
from scripts.utils import get_configs as gc

########################################################################################################################
# Main execution
########################################################################################################################

def run_version_evaluation(
        graphdb_url: URIRef, repository_name:str, facts_named_graph_name:str,
        data_folder:str, links_folder: str):
    """Full pipeline for evaluating street number versions against ground truth."""
    
    # Define paths
    paths = define_paths(data_folder, links_folder)

    # Get source mapping
    source_mapping = get_source_mapping()

    # Extract facts from RDF graph
    extract_facts(
        graphdb_url,
        repository_name,
        facts_named_graph_name,
        paths["facts_graph_file"]
    )

    # Evaluate versions and print results
    version_quality_metrics = evaluate_versions(
        paths["facts_graph_file"],
        paths["links_ground_truth_file"],
        paths["sn_without_link_gt_file"],
        source_mapping
    )
    # print(paths["facts_graph_file"])
    # print(paths["links_ground_truth_file"])
    # print(paths["sn_without_link_gt_file"])

    return version_quality_metrics

#########################################################################################################################
# Print Functions
#########################################################################################################################

def print_version_quality_metrics(version_quality_metrics: list) -> None:
    """
    Pretty-print the version quality metrics with explanation.

    Parameters
    ----------
    version_quality_metrics : list of dicts
        Example: [{'true': 6, 'false': 30, 'total': 36, 'IoU': 0.167}, 
                  {'true': 36, 'false': 0, 'total': 36, 'IoU': 1.0}]
        First dict -> Number of Versions Metric
        Second dict -> Sources Metric
    """

    metric_names = ["Number of Versions Metric", "Sources Metric"]

    print("\n=== Version Quality Metrics ===\n")
    print(f"{'Metric':<30} | {'True':>5} | {'False':>5} | {'Total':>5} | {'IoU':>5}")
    print("-" * 60)

    for name, metric in zip(metric_names, version_quality_metrics):
        print(f"{name:<30} | "
              f"{metric.get('true', '-') :>5} | "
              f"{metric.get('false', '-') :>5} | "
              f"{metric.get('total', '-') :>5} | "
              f"{round(metric.get('IoU', 0), 3) :>5}")

    # ----------------------
    # Explanation
    # ----------------------
    print("\n### Version Quality Metrics Explanation\n")
    print("1. Number of Versions Metric")
    print("   - Measures whether the reconstructed street number (SN) has the same number of versions as the reference (ground-truth) data.")
    print("   - For each SN:")
    print("     - true  -> number of versions matches the reference")
    print("     - false -> number of versions does not match")
    print("   - total -> total number of street numbers evaluated")
    print("   - IoU   -> fraction of street numbers with the correct number of versions\n")

    print("2. Sources Metric")
    print("   - Measures whether the sources associated with each version match the reference, ignoring a specified fragmentary source label")
    print("   - For each SN:")
    print("     - true  -> all versions have matching sources")
    print("     - false -> at least one version has mismatched sources")
    print("   - total -> total number of street numbers evaluated")
    print("   - IoU   -> fraction of street numbers with correctly matching sources\n")

    print("These metrics help assess how well the reconstructed versions replicate both the temporal granularity (number of versions) and the source provenance of the original data.")


########################################################################################################################
# Functions
########################################################################################################################

def load_configurations(proj_config_file: str):
    """
    Load project configurations and graph-related settings.

    Parameters
    ----------
    proj_config_file : str
        Path to the project configuration INI file.

    Returns
    -------
    dict
        Dictionary containing graph settings including GraphDB URL, repository name, and named graph.
    """
    graphs_table_settings = gc.get_graph_settings(proj_config_file)
    graphdb_url = URIRef(graphs_table_settings['graphdb_url'])
    repository_name = graphs_table_settings['repository_name']
    facts_named_graph_name = graphs_table_settings['facts_named_graph_name']

    return {
        "graphdb_url": graphdb_url,
        "repository_name": repository_name,
        "facts_named_graph_name": facts_named_graph_name,
    }


def define_paths(data_folder:str, links_folder: str):
    """
    Define folder paths and CSV file locations for evaluation data.

    Returns
    -------
    dict
        Dictionary containing paths for data folder, links folder, fact graph CSV, and ground truth files.
    """

    return {
        "data_folder": os.path.abspath(data_folder),
        "links_folder": os.path.abspath(links_folder),
        "facts_graph_file": os.path.join(links_folder, "versions_and_sources_from_unmodified_graph.csv"),
        "links_ground_truth_file": os.path.join(links_folder, "links_ground_truth.csv"),
        "sn_without_link_gt_file": os.path.join(links_folder, "sn_without_link_ground_truth.csv")
    }


def get_source_mapping():
    """
    Return a mapping of source identifiers to human-readable labels and order.

    Returns
    -------
    dict
        Mapping dictionary for address sources.
    """
    return {
        "cadastre_paris_1807_adresses": {"order": 1, "label": "Adresses du cadastre général de Paris de 1807"},
        "atlas_vasserot_1810_adresses": {"order": 2, "label": "Cadastre de Paris par îlot : 1810-1836"},
        "atlas_jacoubet_1836_adresses": {"order": 3, "label": "Atlas de la ville de Paris de Jacoubet de 1836"},
        "atlas_municipal_1888_adresses": {"order": 4, "label": "Adresses du plan de l'atlas municipal de 1888"},
        "ban_adresses": {"order": 5, "label": "Base Adresse Nationale"},
        "osm_adresses": {"order": 6, "label": "OpenStreetMap"},
    }


def extract_facts(graphdb_url: URIRef, repository_name: str, facts_named_graph_name: str, facts_graph_file: str):
    """
    Extract attributes, geometries, versions, and sources for street numbers from the RDF graph.

    Parameters
    ----------
    graphdb_url : URIRef
        Base URI of the GraphDB SPARQL endpoint.
    repository_name : str
        Name of the repository containing the RDF graph.
    facts_named_graph_name : str
        Named graph containing the RDF facts.
    facts_graph_file : str
        Path to the CSV file where the extracted facts will be saved.
    """
    # print(graphdb_url, repository_name, facts_named_graph_name, facts_graph_file)
    # print("--- Extracting street number versions and sources from RDF graph ---")
    dfsq.select_streetnumbers_attr_geom_version_and_sources(
        graphdb_url,
        repository_name,
        facts_named_graph_name,
        facts_graph_file
    )


def evaluate_versions(facts_graph_file: str, links_ground_truth_file: str, sn_without_link_gt_file: str, source_mapping: dict):
    """
    Evaluate reconstructed street number versions against the ground truth.

    Parameters
    ----------
    facts_graph_file : str
        Path to the CSV file containing the extracted facts.
    links_ground_truth_file : str
        CSV file with ground truth links.
    sn_without_link_gt_file : str
        CSV file with street numbers that have no link in the ground truth.
    source_mapping : dict
        Mapping of source identifiers to labels and order.

    Returns
    -------
    tuple
        Tuple containing overall quality metrics and per-source breakdown.
    """
    # Load ground truth
    sn_gt_version_sources = ea.get_ground_truth_version_sources(
        links_ground_truth_file,
        sn_without_link_gt_file,
        source_mapping
    )

    # Load extracted facts
    df_facts_graph = pd.read_csv(facts_graph_file)

    # Get all unmodified street number versions
    unmodified_sn = ea.get_sources_for_versions(df_facts_graph, None)

    # Compute evaluation metrics
    return ea.get_graph_quality_from_attribute_versions(
        unmodified_sn,
        sn_gt_version_sources,
        None,
        union=True
    )
