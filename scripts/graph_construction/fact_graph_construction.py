from rdflib import URIRef
from scripts.graph_construction import graphdb as gd
from scripts.graph_construction import attribute_version_comparisons as avc
from scripts.graph_construction import multi_sources_processing as msp
from scripts.graph_construction import resource_rooting as rr
from scripts.graph_construction import evolution_construction as ec


def build_fact_graph_from_sources(
    graphdb_url: URIRef,
    repository_name: str,
    facts_named_graph_name: str,
    facts_named_graph_name_label: str,
    meta_named_graph_name: str,
    inter_sources_named_graph_name: str,
    labels_named_graph_name: str,
    pref_hidden_labels_ttl_file: str,
    tmp_named_graph_name: str,
    comp_named_graph_name: str,
    comp_tmp_file: str,
    comparison_settings: dict,
    lang: str = None
):
    """
    Build a consolidated fact graph and reconstruct the temporal evolution of 
    geographical entities from multiple heterogeneous source graphs.

    This function executes the full pipeline of:
    
    1. Adding preferred and hidden labels to factoids from a labels named graph.
    2. Linking factoids to facts across source graphs in the fact named graph.
    3. Comparing attribute versions from different sources and storing results
       in a comparison named graph.
    4. Initializing missing appearance and disappearance events for landmarks.
    5. Splitting overlapping attribute versions into elementary versions and changes.
    6. Reconstructing coherent attribute version evolutions.

    The resulting RDF graphs describe a consolidated fact graph and 
    temporally structured evolutions of entities inferred from multi-source data.

    Parameters
    ----------
    graphdb_url : URIRef
        URL of the GraphDB instance.
    repository_name : str
        Name of the GraphDB repository.
    facts_named_graph_name : str
        Name of the named graph containing facts and factoids.
    facts_named_graph_name_label : str
        Human-readable label for the facts named graph.
    meta_named_graph_name : str
        Name of the named graph storing meta information.
    inter_sources_named_graph_name : str
        Name of the named graph used to store inter-source links.
    labels_named_graph_name : str
        Name of the named graph containing preferred and hidden labels.
    pref_hidden_labels_ttl_file : str
        Path to a TTL file defining preferred and hidden labels.
    tmp_named_graph_name : str
        Name of a temporary named graph used during processing steps.
    comp_named_graph_name : str
        Name of the named graph storing comparison results.
    comp_tmp_file : str
        Temporary file used during attribute version comparison.
    comparison_settings : dict
        Settings for comparing attribute versions, including:
        - "geom_similarity_coef": float, coefficient threshold for geometric similarity.
        - "geom_buffer_radius": float, buffer radius for geometric comparison.
        - "geom_crs_uri": URIRef, CRS URI for geometric operations.
    lang : str, optional
        Language code for labels (default is "fr" for French).

    Returns
    -------
    None
        The function creates and modifies RDF named graphs in the GraphDB repository.
    """

    # ------------------------------------------------------------------
    # Construct URIs for all named graphs
    # ------------------------------------------------------------------
    facts_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    meta_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, meta_named_graph_name)
    inter_sources_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, inter_sources_named_graph_name)
    labels_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, labels_named_graph_name)
    tmp_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, tmp_named_graph_name)
    comp_named_graph_uri = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, comp_named_graph_name)

    # ------------------------------------------------------------------
    # Add the facts named graph to the repository and associate meta info
    # ------------------------------------------------------------------
    msp.add_final_named_graph_to_repository(
        graphdb_url,
        repository_name,
        meta_named_graph_name,
        facts_named_graph_name,
        facts_named_graph_name_label,
        lang=lang
    )

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
    # 2. Link factoids with facts across source graphs
    # ------------------------------------------------------------------
    rr.link_factoids_with_facts(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 3. Compare attribute versions from different sources
    # ------------------------------------------------------------------
    avc.compare_attribute_versions(
        graphdb_url,
        repository_name,
        comp_named_graph_uri,
        comp_tmp_file,
        comparison_settings
    )

    # ------------------------------------------------------------------
    # 4. Initialize missing appearance and disappearance events for landmarks
    # ------------------------------------------------------------------
    # Appearance is assumed to occur before the earliest reference date.
    # Disappearance is assumed to occur after the latest reference date.
    ec.initialize_missing_changes_and_events_for_landmarks(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_named_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 5. Split overlapping versions into elementary versions and changes
    # ------------------------------------------------------------------
    gd.remove_named_graph_from_uri(tmp_named_graph_uri)  # Clean temp graph before use

    ec.get_elementary_versions_and_changes(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 6. Reconstruct coherent attribute version evolutions
    # ------------------------------------------------------------------
    ec.get_attribute_version_evolution_from_elementary_elements(
        graphdb_url,
        repository_name,
        facts_named_graph_uri,
        inter_sources_named_graph_uri,
        tmp_named_graph_uri
    )

    # ------------------------------------------------------------------
    # 7. Cleanup temporary named graph
    # ------------------------------------------------------------------
    gd.remove_named_graph_from_uri(tmp_named_graph_uri)


def build_fact_graph_excluding_named_graph_sources(
        graphdb_url, repository_name, facts_named_graph_name, facts_named_graph_name_label, named_graph_sources_to_exclude,
        inter_sources_named_graph_name, labels_named_graph_name, pref_hidden_labels_ttl_file,
        meta_named_graph_name, tmp_named_graph_name, comp_named_graph_name, comp_tmp_file, comparison_settings, lang="fr"):
    """
    Build a facts graph excluding the given sources.
    
    :param named_graph_sources_to_exclude: list named graph sources to exclude from the fact graph construction. The named graphs will be deactivated in the meta named graph during the construction process, then re-activated at the end of the process.
    """

    # 1. Activate all sources in the meta named graph
    msp.set_all_named_graphs_active(
        graphdb_url, repository_name, meta_named_graph_name,
        active=True, graph_type="source"
    )
    
    # 2. Deactivate the selected sources
    for source in named_graph_sources_to_exclude:
        msp.set_named_graph_active(
            graphdb_url, repository_name, source,
            meta_named_graph_name, active=False
        )
    
    # 3. Build the facts graph
    build_fact_graph_from_sources(
        graphdb_url, repository_name, facts_named_graph_name, facts_named_graph_name_label,
        meta_named_graph_name, inter_sources_named_graph_name,
        labels_named_graph_name, pref_hidden_labels_ttl_file,
        tmp_named_graph_name,
        comp_named_graph_name, comp_tmp_file, comparison_settings, lang=lang
    )
    