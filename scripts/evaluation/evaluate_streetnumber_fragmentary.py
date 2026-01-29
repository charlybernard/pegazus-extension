import pandas as pd
import os
from rdflib import URIRef
from scripts.evaluation import data_from_sparql_queries as dfsq
from scripts.evaluation import evaluation_aux as ea

# ----------------------------------------------------------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------------------------------------------------------

def run_fragmentary_evaluation(
    data_folder: str,
    graphdb_url: URIRef,
    repository: str,
    facts_named_graph_name: str,
    facts_states_named_graph_name: str,
    facts_states_events_named_graph_name: str,
    fragmentary_source_label: str
) -> dict:
    """
    Run the full evaluation pipeline comparing a reference graph with
    graphs enriched with fragmentary states and with fragmentary states
    and events.

    Returns a structured dictionary containing all quality indicators,
    without performing any output or side effects.

    Returns
    -------
    dict
        Evaluation results structured by evaluation type and enrichment
        strategy.
    """

    # ---------- Data extraction ----------
    df_ref_versions, df_ref_changes = extract_versions_and_changes(
        graphdb_url,
        repository,
        facts_named_graph_name,
        os.path.join(data_folder, "versions_and_sources_from_unmodified_graph.csv"),
        os.path.join(data_folder, "changes_and_times_from_unmodified_graph.csv")
    )

    df_states_versions, df_states_changes = extract_versions_and_changes(
        graphdb_url,
        repository,
        facts_states_named_graph_name,
        os.path.join(data_folder, "versions_and_sources_from_states_graph.csv"),
        os.path.join(data_folder, "changes_and_times_from_states_graph.csv")
    )

    df_states_events_versions, df_states_events_changes = extract_versions_and_changes(
        graphdb_url,
        repository,
        facts_states_events_named_graph_name,
        os.path.join(data_folder, "versions_and_sources_from_states_and_events_graph.csv"),
        os.path.join(data_folder, "changes_and_times_from_states_and_events_graph.csv")
    )

    # ---------- Version stability ----------
    version_quality_states = evaluate_version_stability(
        df_ref_versions,
        df_states_versions,
        fragmentary_source_label
    )

    version_quality_states_events = evaluate_version_stability(
        df_ref_versions,
        df_states_events_versions,
        fragmentary_source_label
    )

    # ---------- Change time stability ----------
    change_quality_states = evaluate_change_time_stability(
        df_ref_changes,
        df_states_changes
    )

    change_quality_states_events = evaluate_change_time_stability(
        df_ref_changes,
        df_states_events_changes
    )

    # print("-----------------------------")
    # print(results["version_stability"]["states"]["unchanged"])
    # print(results["version_stability"]["states"]["modified"])
    # print("-----------------------------")
    # print(results["version_stability"]["states_and_events"]["unchanged"])
    # print(results["version_stability"]["states_and_events"]["modified"])
    # print("###########################################")
    # print(results["change_time_stability"]["states"]['preserved'])
    # print(results["change_time_stability"]["states"]['strict'])
    # print(results["change_time_stability"]["states"]['consistent'])
    # print("-----------------------------")
    # print(results["change_time_stability"]["states_and_events"]['preserved'])
    # print(results["change_time_stability"]["states_and_events"]['strict'])
    # print(results["change_time_stability"]["states_and_events"]['consistent'])

    return {
        "version_stability": {
            "states": {
                "unchanged": version_quality_states[0],
                "modified": version_quality_states[1]
            },
            "states_and_events": {
                "unchanged": version_quality_states_events[0],
                "modified": version_quality_states_events[1]
            }
        },
        "change_time_stability": {
            "states": {
                'preserved': change_quality_states[0],
                'strict': change_quality_states[1],
                'consistent': change_quality_states[2]
            },
            "states_and_events": {
                'preserved': change_quality_states_events[0],
                'strict': change_quality_states_events[1],
                'consistent': change_quality_states_events[2]
            }
        }
    }

###################################################################################################################

def print_evaluation_tables(results: dict) -> None:
    """
    Print two separate ASCII tables for:
    1. Version stability
    2. Change time stability

    All columns: True, False, Total, IoU

    At the end, prints a concise explanation of temporal aspects with updated terms:
    - Preserved (was Identical)
    - Strict (was Shifted)
    - Consistent (was Missing)
    """

    def _fmt(value, key="true"):
        """Extract a scalar from a dict or return '-' if missing"""
        if isinstance(value, dict):
            return str(round(value.get(key, 3), 3)) if key == "IoU" else str(value.get(key, "-"))
        return str(value)

    # ----------------------
    # Version Stability Table
    # ----------------------
    print("\n=== Version Stability ===\n")
    print(f"{'Strategy':<30} | {'Total':>5} | {'True':>5} | {'False':>5} | {'IoU':>5}")
    print("-" * 65)

    for strategy, key in [("Fragmentary states", "states"),
                          ("Fragmentary states + events", "states_and_events")]:
        unchanged = results['version_stability'][key]['unchanged']
        modified = results['version_stability'][key]['modified']
        total = unchanged.get('total', '-')  # assume unchanged total
        iou = unchanged.get('IoU', '-')

        print(f"{strategy:<30} | "
              f"{total:>5} | "
              f"{_fmt(unchanged, 'true'):>5} | "
              f"{_fmt(modified, 'false'):>5} | "
              f"{_fmt(unchanged, 'IoU'):>5}")

    # ----------------------
    # Change Time Stability Table
    # ----------------------
    print("\n=== Change Time Stability ===\n")
    print(f"{'Strategy':<30} | {'True':>5} | {'False':>5} | {'Total':>5} | {'IoU':>5} | {'Aspect':<15}")
    print("-" * 80)

    # Print three rows per strategy for the temporal scores
    for strategy, key in [("Fragmentary states", "states"),
                          ("Fragmentary states + events", "states_and_events")]:
        scores = [
            results['change_time_stability'][key]['preserved'],  # Preserved
            results['change_time_stability'][key]['strict'],     # Strict
            results['change_time_stability'][key]['consistent']  # Consistent
        ]
        aspects = ['Preserved', 'Strict', 'Consistent']

        for score, aspect in zip(scores, aspects):
            print(f"{strategy:<30} | "
                  f"{_fmt(score, 'true'):>5} | "
                  f"{_fmt(score, 'false'):>5} | "
                  f"{_fmt(score, 'total'):>5} | "
                  f"{_fmt(score, 'IoU'):>5} | "
                  f"{aspect:<15}")

    # ----------------------
    # Explanation of temporal aspects
    # ----------------------
    print("\nExplanation of temporal aspects:")
    print("  Preserved  : preservation of the number of changes; for each house number, the number of changes must be identical")
    print("  Strict     : strict identity of dates whenever possible; each change must match an identical date or an identical interval")
    print("  Consistent : temporal consistency of inferred dates with respect to the initial intervals; each point-in-time date must be included in the corresponding interval")
    print("               In unmodified_sn, if a change is associated with [t_min, t_max], its counterpart in modified_sn must satisfy t_min <= t <= t_max")


#########################################################################################################################

def extract_versions_and_changes(
    graphdb_url: URIRef,
    repository_name: str,
    named_graph_name: str,
    versions_file: str,
    changes_file: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract attribute geometry versions and change times for street numbers
    from a given repository and named graph.

    Parameters
    ----------
    graphdb_url : URIRef
        URL of the GraphDB endpoint.
    repository_name : str
        Name of the GraphDB repository.
    named_graph_name : str
        Name of the named graph to query.
    versions_file : str
        Path to the CSV file where attribute versions and sources are stored.
    changes_file : str
        Path to the CSV file where attribute change times are stored.

    Returns
    -------
    tuple[pandas.DataFrame, pandas.DataFrame]
        - DataFrame of attribute versions and sources
        - DataFrame of attribute change times
    """
    dfsq.select_streetnumbers_attr_geom_version_and_sources(
        graphdb_url,
        repository_name,
        named_graph_name,
        versions_file
    )

    dfsq.select_streetnumbers_attr_geom_change_times(
        graphdb_url,
        repository_name,
        named_graph_name,
        changes_file
    )

    df_versions = pd.read_csv(versions_file)
    df_changes = pd.read_csv(changes_file)

    return df_versions, df_changes


def evaluate_version_stability(
    df_versions_reference: pd.DataFrame,
    df_versions_modified: pd.DataFrame,
    fragmentary_source_label: str
) -> tuple:
    """
    Evaluate whether inserting fragmentary states or events alters
    the composition of geometry attribute versions.

    The evaluation checks if versions resulting from the same merged
    sources remain unchanged after adding fragmentary knowledge.

    Parameters
    ----------
    df_versions_reference : pandas.DataFrame
        Versions extracted from the reference (unmodified) graph.
    df_versions_modified : pandas.DataFrame
        Versions extracted from the modified graph.
    fragmentary_source_label : str
        Label identifying fragmentary sources in the provenance.

    Returns
    -------
    tuple
        Quality indicators returned by
        `get_graph_quality_from_attribute_versions`.
    """
    reference_sources = ea.get_sources_for_versions(
        df_versions_reference,
        None
    )

    modified_sources = ea.get_sources_for_versions(
        df_versions_modified,
        fragmentary_source_label
    )

    return ea.get_graph_quality_from_attribute_versions(
        reference_sources,
        modified_sources,
        fragmentary_source_label
    )


def evaluate_change_time_stability(
    df_changes_reference: pd.DataFrame,
    df_changes_modified: pd.DataFrame
) -> tuple:
    """
    Evaluate whether inserting fragmentary states or events modifies
    the temporal localization of attribute changes.

    The evaluation checks if detected changes occur at the same
    instants in the reference and modified graphs.

    Parameters
    ----------
    df_changes_reference : pandas.DataFrame
        Change times extracted from the reference (unmodified) graph.
    df_changes_modified : pandas.DataFrame
        Change times extracted from the modified graph.

    Returns
    -------
    tuple
        Quality indicators returned by
        `get_graph_quality_from_attribute_changes`.
    """
    reference_times = ea.get_times_for_changes(df_changes_reference)
    modified_times = ea.get_times_for_changes(df_changes_modified)

    return ea.get_graph_quality_from_attribute_changes(
        reference_times,
        modified_times
    )