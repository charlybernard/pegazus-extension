from rdflib import URIRef
from scripts.graph_construction import graphdb as gd
from scripts.graph_construction import attribute_version_comparisons as avc
from scripts.graph_construction import multi_sources_processing as msp
from scripts.graph_construction import resource_rooting as rr
from scripts.graph_construction import evolution_construction as ec


def build_fact_graph_from_sources(
    graphdb_url: URIRef,
    repository_name: str,
    facts_named_graph_uri: URIRef,
    inter_sources_name_graph_uri: URIRef,
    labels_named_graph_uri: URIRef,
    pref_hidden_labels_ttl_file: str,
    comparison_settings: dict,
    comp_named_graph_name: str,
    comp_tmp_file: str,
    tmp_named_graph_uri: URIRef
):

    """
    Build a fact graph and reconstruct the evolution of geographical entities
    from multiple heterogeneous source graphs.

    This function implements the full pipeline for:
    1. Enriching factoids with preferred and hidden labels which are stored in the labels named graph.
    2. Linking factoids to facts across sources in fact named graph.
    3. Comparing attribute versions coming from different sources, storing results in comparison named graph.
    4. Initializing missing appearance and disappearance changes for landmarks.
    5. Splitting overlapping versions into elementary versions and changes.
    6. Reconstructing coherent attribute version evolutions.

    The resulting RDF graphs describe a consolidated fact graph and
    temporally structured evolutions of entities inferred from multi-source data.

    Parameters
    ----------
    graphdb_url : URIRef
        URL of the GraphDB instance.
    repository_name : str
        Name of the GraphDB repository.
    facts_named_graph_uri : URIRef
        URI of the named graph containing facts and factoids.
    inter_sources_named_graph_uri : URIRef
        URI of the named graph used to store inter-source links.
    labels_named_graph_uri : URIRef
        URI of the named graph containing labels.
    pref_hidden_labels_ttl_file : str
        Path to a TTL file defining preferred and hidden labels.
    comparison_settings : dict
        Settings for attribute version comparison, including:
        - "geom_similarity_coef": float
            Coefficient threshold for geometric similarity.
        - "geom_buffer_radius": float
            Buffer radius for geometric comparison.
        - "geom_crs_uri": URIRef
            Coordinate reference system URI for geometric operations.
    comp_named_graph_name : str
        Name of the named graph used to store comparison results.
    comp_tmp_file : str
        Temporary file used during attribute version comparison.
    tmp_named_graph_uri : URIRef
        URI of a temporary named graph used during construction steps.

    Returns
    -------
    None
        The function operates by creating and modifying RDF named graphs
        in the GraphDB repository.
    """

    # ------------------------------------------------------------------
    # 1. Enrich factoids with preferred and hidden labels
    # ------------------------------------------------------------------
    msp.add_pref_and_hidden_labels_for_elements(
        graphdb_url,
        repository_name,
        labels_named_graph_uri,
        pref_hidden_labels_ttl_file
    )

    # ------------------------------------------------------------------
    # 2. Link factoids with facts across sources
    # ------------------------------------------------------------------
    rr.link_factoids_with_facts(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_name_graph_uri
    )

    # ------------------------------------------------------------------
    # 3. Compare attribute versions from different sources
    # ------------------------------------------------------------------

    avc.compare_attribute_versions(
        graphdb_url,
        repository_name,
        comp_named_graph_name,
        comp_tmp_file,
        comparison_settings
    )

    # ------------------------------------------------------------------
    # 4. Initialize missing appearance and disappearance changes
    # ------------------------------------------------------------------
    # After importing all factoids, some landmark appearance/disappearance
    # changes may be missing because they are not explicitly described
    # in source factoids.
    #
    # - Appearance is assumed to occur before the earliest reference date.
    # - Disappearance is assumed to occur after the latest reference date.
    ec.initialize_missing_changes_and_events_for_landmarks(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_name_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 5. Split overlapping versions into elementary versions and changes
    # ------------------------------------------------------------------
    gd.remove_named_graph_from_uri(tmp_named_graph_uri)

    ec.get_elementary_versions_and_changes(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 6. Reconstruct attribute version evolutions
    # ------------------------------------------------------------------
    # This step:
    # - removes empty attribute versions,
    # - removes versions whose changes are not linked to any trace,
    # - merges successive similar attribute versions.
    ec.get_attribute_version_evolution_from_elementary_elements(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_name_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 7. Cleanup temporary named graph
    # ------------------------------------------------------------------
    gd.remove_named_graph_from_uri(tmp_named_graph_uri)
