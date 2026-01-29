import pandas as pd
import numpy as np
import math
from uuid import uuid4
from dateutil import parser
from datetime import datetime
import random
from scripts.utils import geom_processing as gp

################# Generate a random geometry for each street number #################

def get_random_geometry_for_street_number(values, epsg_code, max_distance=5):
    """
    For each version value, generate a random geometry within max_distance meters of the centroid.
    Args:
        values: dict of version values.
        epsg_code: EPSG code for CRS.
        max_distance: max distance in meters for random geometry.
    Returns:
        dict with new geometries for each version.
    """
    crs_to_uri = gp.get_crs_dict().get(epsg_code)
    geom_transformers = gp.get_useful_transformers_for_to_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
    new_values = {}
    for version, version_value_list in values.items():
        geom = gp.get_point_around_wkt_literal_geoms(version_value_list, crs_to_uri, geom_transformers, max_distance=max_distance)
        wkt_geom = geom.strip()
        new_values[version] = wkt_geom

    return new_values

################# Generate new valid time contained in old valid time for each version value #################

def generate_random_dates_for_versions(versions):
    """
    Generate random start and end dates for each version, contained within the original interval.
    Args:
        versions: DataFrame with 'startTime' and 'endTime' columns.
    """
    for idx, row in versions.iterrows():
        start_time_col_name = 'startTime'
        end_time_col_name = 'endTime'

        # Parse start and end times
        start = parser.parse(row[start_time_col_name])
        end = parser.parse(row[end_time_col_name])

        # Convert to timestamps for random generation
        start_ts = start.timestamp()
        end_ts = end.timestamp()

        t1_ts = get_random_date_between_interval(start_ts, end_ts)
        t2_ts = t1_ts
        loop_nb, max_loop = 0, 10
        # Ensure t1_ts and t2_ts are not equal
        while t1_ts == t2_ts or loop_nb <= max_loop:
            t2_ts = get_random_date_between_interval(start_ts, end_ts)
            loop_nb += 1

        # Sort to ensure t1 < t2
        t1, t2 = sorted([t1_ts, t2_ts])

        start = t1.isoformat() + 'T00:00:00Z'
        end = t2.isoformat() + 'T00:00:00Z'

        if start == end:
            print(f"Warning! start time and end time have the same value: {start}")

        versions.at[idx, start_time_col_name] = start
        versions.at[idx, end_time_col_name] = end

def get_random_date_between_interval(ts1, ts2):
    """
    Returns a date (without time), randomly between two timestamps.
    Args:
        ts1: start timestamp.
        ts2: end timestamp.
    Returns:
        datetime.date object.
    """
    ts = random.uniform(ts1, ts2)
    return datetime.fromtimestamp(ts).date()

################# Generate new changes coherent with generated ones #################

def generate_random_dates_for_changes(changes):
    """
    Generate random dates for changes, ensuring coherence with existing times.
    Args:
        changes: DataFrame with 'time', 'timeAfter', 'timeBefore' columns.
    """
    for idx, row in changes.iterrows():
        time_col_name = 'time'
        time_after_col_name = 'timeAfter'
        time_before_col_name = 'timeBefore'

        # Parse times, handle missing values
        try:
            time = parser.parse(row[time_col_name])
        except:
            time = None

        try:
            time_after = parser.parse(row[time_after_col_name])
        except:
            time_after = None

        try:
            time_before = parser.parse(row[time_before_col_name])
        except:
            time_before = None

        # Choose the most appropriate time value
        if time is not None:
            final_time = time

        elif None not in [time_before, time_after]:
            final_time = random.uniform(time_after, time_before)

        elif time_after is not None:
            final_time = time_after
        
        elif time_before is not None:
            final_time = time_before
        
        else:
            final_time = np.nan

        if final_time is not None:
            final_time = final_time.date().isoformat() + 'T00:00:00Z'

        changes.at[idx, time_col_name] = final_time

##############################################################################

def get_sources_for_versions(df, frag_source_label):
    """
    Build a dictionary mapping street numbers to their versions and sources.
    Args:
        df: DataFrame with columns 'label', 'sn', 'attrVersion', 'sourceLabel'.
        frag_source_label: label to filter sources.
    Returns:
        dict: {sn_label: {attr_version: set(source_labels)}}
    """
    
    df = df.dropna(subset=['label']) # Ensure 'label' is not NaN

    if frag_source_label is not None:
        unique_sn_labels = df[df["sourceLabel"] == frag_source_label]["label"].unique()
    else:
        unique_sn_labels = df["label"].unique()
    sources_for_versions = {sn: {} for sn in set(unique_sn_labels)}

    for _, row in df.iterrows():
        sn = row["sn"]
        sn_label = row["label"]
        attr_version = row["attrVersion"]
        source_label = row["sourceLabel"]

        if sn_label in unique_sn_labels:
            if attr_version not in sources_for_versions[sn_label]:
                sources_for_versions[sn_label][attr_version] = set()
            sources_for_versions[sn_label][attr_version].add(source_label)

    return sources_for_versions

def get_times_for_changes(df):
    """
    Build a dictionary mapping street numbers to their change times.
    Args:
        df: DataFrame with columns 'label', 'timeDay', 'timeAfterDay', 'timeBeforeDay'.
    Returns:
        dict: {sn_label: [[time, time_after, time_before], ...]}
    """
    unique_sn_labels = df["label"].unique()
    times_for_changes = {sn: [] for sn in set(unique_sn_labels)}

    for _, row in df.iterrows():
        sn_label = row["label"]
        time = row["timeDay"]
        time_after = row["timeAfterDay"]
        time_before = row["timeBeforeDay"]

        if sn_label in unique_sn_labels:
            times_for_changes[sn_label].append([time, time_after, time_before])

    return times_for_changes

##############################################################################

def get_graph_quality_from_attribute_versions(unmodified_sn, modified_sn, frag_source_label, union=False):
    """
    Evaluate the quality of reconstructed street number attribute versions by comparing them
    to a reference (unmodified) version set.

    The function compares two aspects for each street number (SN):
    1. Number of versions: whether the reconstructed SN has the same number of versions as the reference.
    2. Sources for each version: whether the set of sources for each version matches the reference,
       ignoring a specified fragment source label.

    Args:
        unmodified_sn (dict): Reference dictionary of street numbers with their original versions and sources.
            Format: {street_number: {version_id: set(sources)}}
        modified_sn (dict): Dictionary of street numbers with reconstructed/modified versions and sources.
            Same format as `unmodified_sn`.
        frag_source_label (str): Source label to ignore during comparison (e.g., a fragmentary or synthetic source).
        union (bool, optional): If True, consider all street numbers in the union of keys from both
            dictionaries. Defaults to False (only compares street numbers present in `modified_sn`).

    Returns:
        list of dict: Two dictionaries summarizing evaluation metrics:
        [
            {
                "true": count of street numbers with correct number of versions,
                "false": count of street numbers with incorrect number of versions,
                "total": total number of street numbers evaluated,
                "IoU": fraction of street numbers with correct number of versions (Intersection over Union)
            },
            {
                "true": count of street numbers where sources for all versions match,
                "false": count of street numbers where at least one version has mismatched sources,
                "total": total number of street numbers evaluated,
                "IoU": fraction of street numbers with matching sources (Intersection over Union)
            }
        ]

    Explanation of metrics:
        - Number of versions metric: Measures whether the reconstructed street number has the same
          number of temporal or attribute versions as the reference. True if counts match, false otherwise.
        - Sources metric: Measures whether the sources associated with each version (ignoring
          `frag_source_label`) exactly match the reference. True if all versions match, false otherwise.
    """
    
    # Determine which street numbers to evaluate
    if union:
        all_sn = set(unmodified_sn) | set(modified_sn)  # Union of all street numbers
        nb_versions_eval = {sn: False for sn in all_sn}  # Initialize results for number of versions
        sources_eval = {sn: False for sn in all_sn}      # Initialize results for sources
    else:
        nb_versions_eval, sources_eval = {}, {}

    # Compare each modified street number with the reference
    for sn, versions in modified_sn.items():
        unmodified_versions = unmodified_sn.get(sn)

        # If the street number is missing in reference or has a different number of versions
        if unmodified_versions is None or len(versions) != len(unmodified_versions):
            same_nb_versions, same_sources = False, False
        else:
            same_nb_versions, same_sources = True, True

            # Compare sources for each version
            for version, sources in versions.items():
                has_similar_sources = False
                for unmodified_version, unmodified_sources in unmodified_versions.items():
                    # Ignore the fragmentary source label
                    subset1 = sources.copy()
                    subset1.discard(frag_source_label)
                    subset2 = unmodified_sources.copy()
                    subset2.discard(frag_source_label)
                    
                    if subset1 == subset2:
                        has_similar_sources = True

                if not has_similar_sources:
                    same_sources = False

        # Store evaluation results for this street number
        sources_eval[sn] = same_sources
        nb_versions_eval[sn] = same_nb_versions

    # Aggregate results
    nb_true_sources = sum(sources_eval.values())
    nb_false_sources = len(sources_eval) - nb_true_sources
    nb_true_nb_versions = sum(nb_versions_eval.values())
    nb_false_nb_versions = len(nb_versions_eval) - nb_true_nb_versions

    # Metrics dictionaries
    sn_with_versions_with_good_sources = {
        "true": nb_true_sources,
        "false": nb_false_sources,
        "total": len(sources_eval),
        "IoU": nb_true_sources / len(sources_eval)  # Intersection over Union
    }

    sn_with_good_nb_of_versions = {
        "true": nb_true_nb_versions,
        "false": nb_false_nb_versions,
        "total": len(nb_versions_eval),
        "IoU": nb_true_nb_versions / len(nb_versions_eval)  # Intersection over Union
    }

    return [sn_with_good_nb_of_versions, sn_with_versions_with_good_sources]


def get_graph_quality_from_attribute_changes(unmodified_sn, modified_sn):
    """
    Compares temporal attribute changes between a reference graph (unmodified_sn) and a modified / reconstructed graph (modified_sn).

    Each house number is associated with a list of temporal changes, represented as triplets: [t, t_min, t_max]

    where:
    - t     : point-in-time date
    - t_min : lower temporal bound
    - t_max : upper temporal bound

    If t is defined (non-null), it represents the exact date of the change;
    the other values are null (or ignored if non-null): [t, None, None].
    Otherwise, the change is represented by one of the following intervals:
    - [t_min, t_max], i.e. [None, t_min, t_max];
    - ]-inf, t_max], i.e. [None, None, t_max];
    - [t_min, +inf[, i.e. [None, t_min, None].

    The function evaluates the quality of the modified graph according to
    three criteria:
    1) preservation of the number of changes: for each house number, the number of changes must be identical;
    2) strict identity of dates whenever possible: each change must match an identical date or an identical interval;
    3) temporal consistency of inferred dates with respect to the initial intervals: each point-in-time date must be included in the corresponding interval.
    In unmodified_sn, if a change is associated with a temporal interval [t_min, t_max],
    then its counterpart in modified_sn must have a temporal value satisfying t_min <= t <= t_max.

    Args:
        unmodified_sn (dict): temporal changes derived from source data.
        modified_sn (dict): temporal changes after processing / inference.

    Returns:
        list[dict]: three dictionaries containing global metrics
                    (true / false / total / IoU) for each criterion.
    """


    # Dictionnaires d’évaluation par numéro de voie
    nb_changes_eval = {}       # même nombre de changements
    same_times_eval = {}       # mêmes dates exactes
    coherent_times_eval = {}   # cohérence temporelle

    # Parcours de chaque numéro de voie modifié
    for sn, changes in modified_sn.items():
        unmodified_changes = unmodified_sn.get(sn)
 
        # --- CRITÈRE 1 : même nombre de changements ---
        if len(changes) != len(unmodified_changes):
            same_nb_changes = False
            same_changes = False
            coherent_changes = False

        else:
            same_nb_changes = True
            same_changes = True
            coherent_changes = True

            # Parcours des changements reconstruits
            for change in changes:
                has_similar_changes = False
                has_coherent_changes = False

                # Comparaison avec chaque changement de référence
                for unmodified_change in unmodified_changes:

                    # Normalisation des nan en None pour faciliter les comparaisons
                    cg = [None if math.isnan(x) else x for x in change]
                    unmodified_cg = [None if math.isnan(x) else x for x in unmodified_change]

                    # --- Cas 1 : date ponctuelle strictement identique ---
                    if cg[0] is not None and cg[0] == unmodified_cg[0]:
                        has_similar_changes = True
                        has_coherent_changes = True

                    # --- Cas 2 : date ponctuelle incluse dans un intervalle ---
                    elif cg[0] is not None:
                        t = cg[0]
                        t_min, t_max = unmodified_cg[1], unmodified_cg[2]

                        if (
                            (t_min is None or t >= t_min)
                            and (t_max is None or t <= t_max)
                        ):
                            has_coherent_changes = True

                    # --- Cas 3 : intervalle temporel strictement identique ---
                    elif cg[0] is None and cg[1:] == unmodified_cg[1:]:
                        has_similar_changes = True
                        has_coherent_changes = True

                # Si aucun changement strictement identique n’a été trouvé
                if not has_similar_changes:
                    same_changes = False

                # Si aucun changement cohérent n’a été trouvé
                if not has_coherent_changes:
                    coherent_changes = False

        # print(f"Evaluating street number: {sn}")
        # print(f"Modified changes: {changes}")
        # print(f"Unmodified changes: {unmodified_changes}")

        # print(same_nb_changes, same_changes, coherent_changes)
        # print("---------------------------------------------------")

        # Stockage des résultats pour le numéro de voie courant
        nb_changes_eval[sn] = same_nb_changes
        same_times_eval[sn] = same_changes
        coherent_times_eval[sn] = coherent_changes

    # --- Agrégation globale des résultats ---

    nb_true_nb_changes = sum(nb_changes_eval.values())
    nb_false_nb_changes = len(nb_changes_eval) - nb_true_nb_changes

    nb_true_same_changes = sum(same_times_eval.values())
    nb_false_same_changes = len(same_times_eval) - nb_true_same_changes

    nb_true_coherent_changes = sum(coherent_times_eval.values())
    nb_false_coherent_changes = len(coherent_times_eval) - nb_true_coherent_changes

    # Métriques finales (avec IoU assimilé à un taux de conformité)
    sn_with_good_nb_of_changes = {
        "true": nb_true_nb_changes,
        "false": nb_false_nb_changes,
        "total": len(nb_changes_eval),
        "IoU": nb_true_nb_changes / len(nb_changes_eval)
    }

    sn_with_changes_with_same_times = {
        "true": nb_true_same_changes,
        "false": nb_false_same_changes,
        "total": len(same_times_eval),
        "IoU": nb_true_same_changes / len(same_times_eval)
    }

    sn_with_changes_with_coherent_times = {
        "true": nb_true_coherent_changes,
        "false": nb_false_coherent_changes,
        "total": len(coherent_times_eval),
        "IoU": nb_true_coherent_changes / len(coherent_times_eval)
    }

    return [
        sn_with_good_nb_of_changes,
        sn_with_changes_with_same_times,
        sn_with_changes_with_coherent_times
    ]


###############################################################################

def get_ground_truth_version_sources(links_ground_truth_file, sn_without_link_ground_truth_file, source_mapping):
    """
    Get ground truth version sources from links and unlinked street numbers.
    Args:
        links_ground_truth_file: CSV file with links.
        sn_without_link_ground_truth_file: CSV file with unlinked street numbers.
        source_mapping: dict mapping sources to labels and orders.
    Returns:
        dict: ground truth grouped links for all street numbers.
    """
    d1 = get_ground_truth_version_sources_from_links(links_ground_truth_file, source_mapping)
    d2 = get_ground_truth_version_sources_from_unlinked_streetnumbers(sn_without_link_ground_truth_file, source_mapping)

    return d1|d2

def get_ground_truth_version_sources_from_links(links_ground_truth_file, source_mapping):
    """
    Get ground truth version sources from linked street numbers.
    Args:
        links_ground_truth_file: CSV file with links.
        source_mapping: dict mapping sources to labels and orders.
    Returns:
        dict: grouped links for each street number.
    """
    order_to_label = {v["order"]: v["label"] for v in source_mapping.values()}

    df = pd.read_csv(links_ground_truth_file)
    df = df.dropna(subset=['simplified_label']) # Ensure 'simplified_label' is not NaN

    unique_sn_labels = df["simplified_label"].unique()
    ground_truth_links = {sn: [] for sn in set(unique_sn_labels)}
    ground_truth_grouped_links = {}
    for _, row in df.iterrows():
        label = row["simplified_label"]
        table_from = row["from_source"]
        table_to = row["to_source"]
        are_similar_geom = row["similar_geom"]
        order_from = source_mapping[table_from]["order"]
        order_to = source_mapping[table_to]["order"]
        ground_truth_links[label].append((order_from, order_to, are_similar_geom))

    for sn in ground_truth_links:
        links = ground_truth_links[sn]
        links.sort()
        
        # List to store groups of sources
        groups = []

        # Start a group with the first element
        current_group = [links[0][0]]

        for source, target, value in links:
            if value:
                # If the link is true, continue in the group
                current_group.append(target)
            else:
                # Otherwise, end the group and start a new one
                groups.append(current_group)
                current_group = [target]

        # Don't forget to add the last group
        groups.append(current_group)
        
        groupes_with_labels = {str(uuid4()): {order_to_label[o] for o in group} for group in groups}

        ground_truth_grouped_links[sn] = groupes_with_labels

    return ground_truth_grouped_links

def get_ground_truth_version_sources_from_unlinked_streetnumbers(sn_without_link_ground_truth_file, source_mapping):
    """
    Get ground truth version sources from unlinked street numbers.
    Args:
        sn_without_link_ground_truth_file: CSV file with unlinked street numbers.
        source_mapping: dict mapping sources to labels.
    Returns:
        dict: grouped links for each unlinked street number.
    """
    df = pd.read_csv(sn_without_link_ground_truth_file)
    df = df.dropna(subset=['simplified_label']) # Ensure 'simplified_label' is not NaN

    ground_truth_grouped_links = {}
    for _, row in df.iterrows():
        label = row["simplified_label"]
        source = row["source"]
        source_label = source_mapping[source].get("label")
        group = {str(uuid4()):{source_label}}
        ground_truth_grouped_links[label] = group
    
    return ground_truth_grouped_links