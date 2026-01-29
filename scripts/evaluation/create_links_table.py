import itertools

def create_links_table(
    pm, schema_name, table_name,
    id_col, from_id_col, to_id_col,
    from_source_col, to_source_col,
    similar_geom_col, succesive_geom_col,
    geom_col, geom_type="LineString", epsg_code=4326
    ):
    """
    Creates a links table in the specified schema with the given parameters.
    If the schema does not exist, it is created. The table is dropped and recreated.
    Args:
        pm (PostgresManager): Instance to execute queries.
        schema_name (str): Name of the schema (can be None).
        table_name (str): Name of the table.
        id_col (str): Column name for primary key.
        from_id_col (str): Column name for the ID from the first source.
        to_id_col (str): Column name for the ID from the second source.
        from_source_col (str): Column name for the source of the first ID.
        to_source_col (str): Column name for the source of the second ID.
        similar_geom_col (str): Column name indicating if geometries are similar.
        succesive_geom_col (str): Column name indicating if geometries are successive.
        geom_col (str): Column name for geometry.
        geom_type (str): Geometry type (default is 'LineString').
        epsg_code (int): EPSG code for the spatial reference system.
    """

    query = f"""
    DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {id_col} SERIAL PRIMARY KEY,
        {from_id_col} INTEGER,
        {to_id_col} INTEGER,
        {from_source_col} TEXT,
        {to_source_col} TEXT,
        {similar_geom_col} BOOLEAN DEFAULT FALSE,
        {succesive_geom_col} BOOLEAN DEFAULT FALSE,
        {geom_col} GEOMETRY({geom_type}, {epsg_code})
    );
    """

    pm.execute_query(query)

def create_links_between_similar_addresses(
    pm,
    links_schema_name, links_table_name,
    addr_schema_name, addr_table_name,
    links_id_col_from, links_id_col_to,
    links_source_from_col, links_source_to_col,
    links_geom_col,
    links_similar_geom_col, links_succesive_geom_col,
    addr_id_col, addr_source_col, addr_geom_col, addr_simp_label_col,
    source_names,
    links_epsg_code=4326, addr_epsg_code=4326, max_distance=5):

    pairs = list(itertools.combinations(source_names, 2))
    for pair in pairs:
        source_from_name, source_to_name = pair[0], pair[1]


        create_links_between_similar_addresses_from_source_pair(
            pm,
            links_schema_name, links_table_name,
            addr_schema_name, addr_table_name,
            links_id_col_from, links_id_col_to,
            links_source_from_col, links_source_to_col,
            links_geom_col,
            links_similar_geom_col, links_succesive_geom_col,
            addr_id_col, addr_source_col, addr_geom_col, addr_simp_label_col,
            source_from_name, source_to_name,
            links_epsg_code=links_epsg_code, addr_epsg_code=addr_epsg_code, max_distance=max_distance
        )


def create_links_between_similar_addresses_from_source_pair(
        pm,
        links_schema_name, links_table_name,
        addr_schema_name, addr_table_name,
        links_id_col_from, links_id_col_to,
        links_source_from_col, links_source_to_col,
        links_geom_col,
        links_similar_geom_col, links_succesive_geom_col,
        addr_id_col, addr_source_col, addr_geom_col, addr_simp_label_col,
        source_from_name, source_to_name,
        links_epsg_code=4326, addr_epsg_code=4326, max_distance=5):


    query1 = f""" 
    INSERT INTO {links_schema_name}.{links_table_name}
    (\"{links_id_col_from}\", \"{links_id_col_to}\", \"{links_source_from_col}\", \"{links_source_to_col}\", \"{links_geom_col}\")
    SELECT
        t1.{addr_id_col},
        t2.{addr_id_col},
        t1.{addr_source_col},
        t2.{addr_source_col},
        ST_SetSRID(
            ST_MakeLine(
                ST_Transform(ST_Centroid(t1.{addr_geom_col}), {addr_epsg_code}),
                ST_Transform(ST_Centroid(t2.{addr_geom_col}), {addr_epsg_code})
            ), {links_epsg_code}
        )
    FROM {addr_schema_name}.{addr_table_name} AS t1, {addr_schema_name}.{addr_table_name} AS t2
    WHERE
    t1.{addr_simp_label_col} = t2.{addr_simp_label_col} AND
    t1.{addr_source_col} = '{source_from_name}' AND
    t2.{addr_source_col} = '{source_to_name}'  ;
    """

    query2 = f"""
    UPDATE {links_schema_name}.{links_table_name}
    SET {links_similar_geom_col} = (ST_Length(ST_Transform("{links_geom_col}", {links_epsg_code})) < {max_distance});
    """
    
    pm.execute_query(query1)
    pm.execute_query(query2)


def get_successive_geom_links(
        pm, schema_name, table_name, source_names,
        id_from_col, source_from_col, source_to_col, succesive_geom_col):
    """
    Get the links that are marked to be kept.
    """

    pairs = list(itertools.combinations(source_names, 2))

    for pair in pairs:
        source_from_name, source_to_name = pair[0], pair[1]
        get_successive_geom_links_from_table_pair(pm, schema_name, table_name, source_from_name, source_to_name, id_from_col, source_from_col, source_to_col, succesive_geom_col)

def get_successive_geom_links_from_table_pair(
        pm, schema_name, table_name,
        source_from_name, source_to_name,
        id_from_col, source_from_col,
        source_to_col, succesive_geom_col):
    """
    Get the links that are marked to be kept.
    """

    query = f"""
    UPDATE {schema_name}.{table_name}
    SET {succesive_geom_col} = TRUE
    WHERE {source_from_col} = '{source_from_name}' AND {source_to_col} = '{source_to_name}' AND {id_from_col} NOT IN (
        SELECT DISTINCT {id_from_col}
        FROM {schema_name}.{table_name}
        WHERE {succesive_geom_col} AND {source_from_col} = '{source_from_name}'
    )
    """
    pm.execute_query(query)