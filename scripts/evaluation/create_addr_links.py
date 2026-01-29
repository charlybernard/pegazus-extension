import os
from scripts.utils import get_configs as gc
from scripts.utils.db_utils import PostgresManager
from scripts.evaluation import create_addresses_table as cat
from scripts.evaluation import add_labels_for_addresses_table as alfat
from scripts.evaluation import create_links_table as clt
from scripts.evaluation import extract_addr_links as eal

def create_links(
    db_config_file: str,
    proj_config_file: str,
    sources_settings: list,
    ban_settings: dict,
    osm_settings: dict,
    source_names: list,
    links_folder: str
):
    """
    Orchestrates the full pipeline for creating and linking historical addresses 
    in a PostGIS/PostgreSQL database.

    Steps performed:
        1. Loads configuration files for database and project settings.
        2. Creates the main addresses table if it doesn't exist.
        3. Inserts address features from multiple sources:
           - Historical GeoJSON sources
           - BAN CSV source
           - OSM CSV source
        4. Creates links between similar addresses based on label similarity and spatial proximity.
        5. Closes the database connection.

    Parameters:
    -----------
    db_config_file : str
        Path to the database configuration INI file (contains credentials, schema names, etc.).
    proj_config_file : str
        Path to the project configuration INI file (contains project-specific parameters like buffer radius).
    sources_settings : list
        List of dictionaries describing historical GeoJSON sources, with keys such as:
        'source_name', 'file', 'number_prop', 'street_name_prop', 'epsg_code'.
    ban_settings : dict
        Dictionary with settings for the BAN CSV source, including file path, property names, and EPSG code.
    osm_settings : dict
        Dictionary with settings for the OSM CSV source, including file paths, property names, and EPSG code.
    source_names : list
        List of source names to include when creating links between similar addresses.
    links_folder : str
        Path to the folder where link ground truth files will be saved.

    Returns:
    --------
    None
        Prints progress messages and completes the pipeline.
    """
    
    # Load database and project configurations
    # pm: PostgresManager object for database operations
    # addr_table_settings: settings for the addresses table
    # links_table_settings: settings for the links table
    # graphs_table_settings: general graph-related settings (e.g., buffer radius)
    pm, addr_table_settings, links_table_settings, graphs_table_settings = load_configs(
        db_config_file, proj_config_file
    )
    
    # Retrieve maximum distance for linking addresses (default = 10 meters)
    max_distance = graphs_table_settings.get('geom_buffer_radius', 10)

    # Create the main addresses table in the database
    create_addresses_table(pm, addr_table_settings)

    # Insert addresses from all sources (historical, BAN, OSM)
    insert_address_sources(pm, addr_table_settings, sources_settings, ban_settings, osm_settings)

    # Create links between similar addresses based on label similarity and spatial proximity
    create_and_fill_links(pm, addr_table_settings, links_table_settings, source_names, max_distance, links_folder)

    # Close the database connection
    pm.close()
    
    print("Process completed successfully.")


#########################################################################################################################

def load_configs(db_config_file, proj_config_file):

    pm = PostgresManager(db_config_file)

    addr_table_settings = gc.get_addresses_table_settings(db_config_file)
    links_table_settings = gc.get_links_table_settings(db_config_file)
    graphs_table_settings = gc.get_graph_settings(proj_config_file)

    return pm, addr_table_settings, links_table_settings, graphs_table_settings

def create_addresses_table(pm, addr_table_settings):

    pm.create_postgis_extension()

    cat.create_streetnumbers_table(
        pm,
        addr_table_settings['schema_name'],
        addr_table_settings['table_name'],
        addr_table_settings['id_col'],
        addr_table_settings['number_col'],
        addr_table_settings['street_name_col'],
        addr_table_settings['source_col'],
        addr_table_settings['geom_col'],
        addr_table_settings['geom_type'],
        addr_table_settings['epsg_code']
    )

    print(f"Created table `{addr_table_settings['schema_name']}.{addr_table_settings['table_name']}`.")

def insert_address_sources(pm, addr_table_settings, sources_settings, ban_settings, osm_settings):

    table_name = f"{addr_table_settings['schema_name']}.{addr_table_settings['table_name']}"

    # GeoJSON historiques
    for source in sources_settings:
        cat.insert_geojson_features_in_streetnumber_table(
            pm, source['file'], table_name, source['source_name'],
            addr_table_settings['source_col'], addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
            source['number_prop'], source['street_name_prop'],
            from_epsg=source['epsg_code'], to_epsg=addr_table_settings['epsg_code']
        )

    # BAN
    cat.insert_ban_features_in_streetnumber_table(
        pm, ban_settings['file'], table_name, ban_settings['source_name'],
        addr_table_settings['source_col'],
        addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
        ban_settings['number_prop'], ban_settings['repetition_prop'],
        ban_settings['street_name_prop'], ban_settings['lat_prop'], ban_settings['lon_prop'],
        from_epsg=ban_settings['epsg_code'], to_epsg=addr_table_settings['epsg_code']
    )

    # OSM
    cat.insert_osm_features_in_streetnumber_table(
        pm, osm_settings['file'], osm_settings['hn_file'], osm_settings['join_prop'],
        table_name, osm_settings['source_name'], addr_table_settings['source_col'],
        addr_table_settings['number_col'], addr_table_settings['street_name_col'], addr_table_settings['geom_col'],
        osm_settings['number_prop'], osm_settings['street_name_prop'], osm_settings['geom_prop'],
        from_epsg=osm_settings['epsg_code'], to_epsg=addr_table_settings['epsg_code']
    )

    # Ajouter colonnes de labels
    alfat.add_label_columns_for_table(
        pm,
        addr_table_settings['schema_name'], addr_table_settings['table_name'],
        addr_table_settings['id_col'],
        addr_table_settings['number_col'], addr_table_settings['street_name_col'],
        addr_table_settings['simplified_label_col'], addr_table_settings['normalized_label_col'],
        exceptions=None
    )

    print(f"Inserted features and added label columns in table `{table_name}`.")

def create_and_fill_links(pm, addr_table_settings, links_table_settings, source_names, max_distance, links_folder):
    clt.create_links_table(
        pm,
        links_table_settings['schema_name'], links_table_settings['table_name'],
        links_table_settings['id_col'], links_table_settings['id_from_col'], links_table_settings['id_to_col'],
        links_table_settings['source_from_col'], links_table_settings['source_to_col'],
        links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
        links_table_settings['geom_col'], links_table_settings['geom_type'], links_table_settings['epsg_code'],
    )

    clt.create_links_between_similar_addresses(
        pm,
        links_table_settings['schema_name'], links_table_settings['table_name'],
        addr_table_settings['schema_name'], addr_table_settings['table_name'],
        links_table_settings['id_from_col'], links_table_settings['id_to_col'],
        links_table_settings['source_from_col'], links_table_settings['source_to_col'],
        links_table_settings['geom_col'], links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
        addr_table_settings['id_col'], addr_table_settings['source_col'], addr_table_settings['geom_col'], addr_table_settings['simplified_label_col'],
        source_names,
        links_epsg_code=links_table_settings['epsg_code'], addr_epsg_code=addr_table_settings['epsg_code'], max_distance=max_distance
    )

    clt.get_successive_geom_links(
        pm,
        links_table_settings['schema_name'], links_table_settings['table_name'],
        source_names,
        links_table_settings['id_from_col'], links_table_settings['source_from_col'],
        links_table_settings['source_to_col'], links_table_settings['successive_geom_col']
    )

    print(f"Created and updated links table `{links_table_settings['schema_name']}.{links_table_settings['table_name']}`.")

    # Export ground truth
    links_ground_truth = os.path.join(links_folder, "links_ground_truth.csv")
    sn_without_link_ground_truth = os.path.join(links_folder, "sn_without_link_ground_truth.csv")

    eal.extract_ground_truth_links(
        pm,
        links_table_settings['schema_name'], links_table_settings['table_name'],
        addr_table_settings['schema_name'], addr_table_settings['table_name'],
        links_table_settings['source_from_col'], links_table_settings['source_to_col'],
        links_table_settings['id_from_col'],
        links_table_settings['similar_geom_col'], links_table_settings['successive_geom_col'],
        addr_table_settings['id_col'], addr_table_settings['simplified_label_col'],
        links_ground_truth
    )

    eal.extract_streetnumbers_without_link(
        pm,
        links_table_settings['schema_name'], links_table_settings['table_name'],
        addr_table_settings['schema_name'], addr_table_settings['table_name'],
        links_table_settings['id_from_col'], links_table_settings['id_to_col'],
        addr_table_settings['id_col'], addr_table_settings['source_col'], addr_table_settings['simplified_label_col'],
        sn_without_link_ground_truth
    )

    print(f"Exported ground truth and street numbers without link.")
