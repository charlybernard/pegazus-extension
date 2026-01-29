import json
import pandas as pd
from shapely.geometry import shape

def create_streetnumbers_table(
    pm, schema_name, table_name,
    id_col, number_col, street_name_col, source_col, geom_col,
    geom_type="Point", epsg_code=4326
):
    """
    Creates a PostgreSQL table for storing house number data with geometry.

    If the schema does not exist, it is created. The table is dropped and recreated.

    Args:
        pm (PostgresManager): Instance to execute queries.
        schema_name (str): Name of the schema (can be None).
        table_name (str): Name of the table.
        id_col (str): Column name for primary key.
        number_col (str): Column name for house number.
        street_name_col (str): Column name for street name.
        source_col (str): Column name for data source.
        geom_col (str): Column name for geometry.
        geom_type (str): Geometry type (default is 'Point').
        epsg_code (int): EPSG code for the spatial reference system.
    """

    pm.create_schema(schema_name)

    query = f"""
    DROP TABLE IF EXISTS {schema_name}.{table_name};
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {id_col} SERIAL PRIMARY KEY,
        {number_col} TEXT,
        {street_name_col} TEXT,
        {source_col} TEXT,
        {geom_col} GEOMETRY({geom_type}, {epsg_code})
    );
    """
    pm.execute_query(query)

def insert_geojson_features_in_streetnumber_table(
        pm, geojson_file, table_name, source_name,
        source_name_col, number_col, street_name_col, geom_col,
        number_prop, street_name_prop,
        from_epsg=4326, to_epsg=4326):
    """
    Parses a GeoJSON file and inserts each feature as a row in the house number table.

    Args:
        pm (PostgresManager): Instance for executing SQL queries.
        geojson_file (str): Path to the GeoJSON file.
        table_name (str): Target table for insertion.
        source_name (str): Name of the data source.
        source_name_col, number_col, street_name_col, geom_col (str): Column names.
        number_prop (str): Property in the GeoJSON for the house number.
        street_name_prop (str): Property in the GeoJSON for the street name.
        from_epsg (int): EPSG code of input geometries.
        to_epsg (int): EPSG code for transformation into the database.
    """
    with open(geojson_file) as f:
        data = json.load(f)

    for feature in data['features']:
        props = feature['properties']
        geom = shape(feature['geometry'])
        wkt_geom = geom.centroid.wkt

        number = props[number_prop] if pd.notna(props[number_prop]) else ""
        number = str(number).replace("'", "''")
        street_name = props[street_name_prop] if pd.notna(props[street_name_prop]) else ""
        street_name = str(street_name).replace("'", "''")

        source_name_val = f"'{source_name}'" if source_name else "NULL"
        number_val = f"'{number}'" if number else "NULL"
        street_name_val = f"'{street_name}'" if street_name else "NULL"

        query = f"""
        INSERT INTO {table_name} ({source_name_col}, {number_col}, {street_name_col}, {geom_col})
        VALUES ({source_name_val}, {number_val}, {street_name_val}, ST_Transform(ST_GeomFromText('{wkt_geom}', {from_epsg}), {to_epsg}));
        """
        pm.execute_query(query)


def insert_ban_features_in_streetnumber_table(
        pm, ban_file, table_name, source_name,
        source_name_col, number_col, street_name_col, geom_col,
        number_prop, repetition_prop, street_name_prop, lat_prop, lon_prop,
        from_epsg=4326, to_epsg=4326, ban_file_sep=";"):
    """
    Reads a CSV file (BAN format) and inserts address points into the house number table.

    Args:
        pm (PostgresManager): Instance to execute SQL queries.
        ban_file (str): Path to the BAN CSV file.
        table_name (str): Name of the database table.
        source_name (str): Source name for the dataset.
        source_name_col, number_col, street_name_col, geom_col (str): Column names.
        number_prop, repetition_prop, street_name_prop, lat_prop, lon_prop (str): Property/column names in the CSV.
        from_epsg (int): Original coordinate system EPSG.
        to_epsg (int): Target coordinate system EPSG.
        ban_file_sep (str): Separator for the CSV file.
    """
    df = pd.read_csv(ban_file, sep=ban_file_sep)

    for _, row in df.iterrows():
        insert_ban_feature_in_streetnumber_table(
            pm, row, table_name, source_name,
            source_name_col, number_col, street_name_col, geom_col,
            number_prop, repetition_prop, street_name_prop, lat_prop, lon_prop,
            from_epsg=from_epsg, to_epsg=to_epsg
        )


def insert_ban_feature_in_streetnumber_table(
        pm, ban_row, table_name, source_name,
        source_name_col, number_col, street_name_col, geom_col,
        number_prop, repetition_prop, street_name_prop, lat_prop, lon_prop,
        from_epsg=4326, to_epsg=4326):
    """
    Inserts a single address feature (BAN format) into the house number table.

    Args:
        pm (PostgresManager): SQL executor instance.
        ban_row (pd.Series): One row from the BAN CSV.
        table_name (str): Name of the destination table.
        source_name (str): Source label.
        ... (str): Various column/property names for BAN structure.
        from_epsg (int): EPSG code of input coordinates.
        to_epsg (int): EPSG code for output coordinates.
    """
    wkt_geom = f"POINT({ban_row[lon_prop]} {ban_row[lat_prop]})"
    source_name_val = f"'{source_name}'" if source_name else "NULL"

    repetition = ban_row[repetition_prop] if pd.notna(ban_row[repetition_prop]) else ""
    repetition = str(repetition).replace("'", "''")
    number = ban_row[number_prop] if pd.notna(ban_row[number_prop]) else ""
    number = str(number).replace("'", "''")
    street_name = ban_row[street_name_prop] if pd.notna(ban_row[street_name_prop]) else ""
    street_name = str(street_name).replace("'", "''")

    number_val = f"'{number}{repetition}'" if number else "NULL"
    street_name_val = f"'{street_name}'" if street_name else "NULL"

    query = f"""
    INSERT INTO {table_name} ({source_name_col}, {number_col}, {street_name_col}, {geom_col})
    VALUES ({source_name_val}, {number_val}, {street_name_val}, ST_Transform(ST_GeomFromText('{wkt_geom}', {from_epsg}), {to_epsg}));
    """
    pm.execute_query(query)


def insert_osm_features_in_streetnumber_table(
        pm, osm_file, osm_hn_file, join_prop, table_name, source_name,
        source_name_col, number_col, street_name_col, geom_col,
        number_prop, street_name_prop, geom_prop,
        from_epsg=4326, to_epsg=4326, osm_file_sep=","):
    """
    Loads and merges two OpenStreetMap CSV files, then inserts merged address data into the table.

    Args:
        pm (PostgresManager): SQL executor instance.
        osm_file (str): Path to OSM street data.
        osm_hn_file (str): Path to OSM house number data.
        join_prop (str): Join key between the two files.
        source_name (str): Source name label.
        ... (str): Column names and geometry settings.
    """
    osm = pd.read_csv(osm_file, sep=osm_file_sep)
    osm_hn = pd.read_csv(osm_hn_file, sep=osm_file_sep)
    osm_merged = pd.merge(osm, osm_hn, on=join_prop)

    for _, row in osm_merged.iterrows():
        insert_osm_feature_in_streetnumber_table(
            pm, row, table_name, source_name,
            source_name_col, number_col, street_name_col, geom_col,
            number_prop, street_name_prop, geom_prop,
            from_epsg=from_epsg, to_epsg=to_epsg
        )


def insert_osm_feature_in_streetnumber_table(
        pm, osm_row, table_name, source_name,
        source_name_col, number_col, street_name_col, geom_col,
        number_prop, street_name_prop, geom_prop,
        from_epsg=4326, to_epsg=4326):
    """
    Inserts a single row of OSM address data into the house number table.

    Args:
        pm (PostgresManager): SQL executor instance.
        osm_row (pd.Series): One row from the merged OSM dataset.
        table_name (str): Destination table.
        source_name (str): Name of data source.
        ... (str): Column and property names for insertion.
    """
    wkt_geom = osm_row[geom_prop]
    source_name_val = f"'{source_name}'" if source_name else "NULL"

    number = osm_row[number_prop] if pd.notna(osm_row[number_prop]) else ""
    number = str(number).replace("'", "''")
    street_name = osm_row[street_name_prop] if pd.notna(osm_row[street_name_prop]) else ""
    street_name = str(street_name).replace("'", "''")

    number_val = f"'{number}'" if number else "NULL"
    street_name_val = f"'{street_name}'" if street_name else "NULL"

    query = f"""
    INSERT INTO {table_name} ({source_name_col}, {number_col}, {street_name_col}, {geom_col})
    VALUES ({source_name_val}, {number_val}, {street_name_val}, ST_Transform(ST_GeomFromText('{wkt_geom}', {from_epsg}), {to_epsg}));
    """
    pm.execute_query(query)
