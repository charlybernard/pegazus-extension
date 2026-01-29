from scripts.utils import str_processing as sp
import csv
import itertools
import pandas as pd

def get_postgis_table_geom_settings(conn, schema_name, table_name):
    cur = conn.cursor()

    # Requête pour extraire la colonne de l'id
    cur.execute("""
    SELECT 
    kcu.column_name
    FROM 
    information_schema.table_constraints AS tc
    JOIN 
    information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    WHERE 
    tc.table_name = %s AND tc.table_schema = %s
    """, (table_name, schema_name))
    
    try:
        id_col = cur.fetchone()[0]
    except:
        id_col = None

    # Requête pour extraire la colonne de géométrie et le srid
    cur.execute("""
    SELECT f_geometry_column
    FROM geometry_columns
    WHERE f_table_schema = %s AND f_table_name = %s
    """, (schema_name, table_name))

    try:
        geom_col = cur.fetchone()[0]
    except:
        geom_col = None

    cur.close()

    return id_col, geom_col

def create_normalised_label_for_streetnumbers(conn, schema_name, table_name, norm_label_col, th_attr_col, sn_attr_col, add_sn_attr_col):
    cur = conn.cursor()

    # Add the normalised label column if it doesn't exist
    cur.execute(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS {norm_label_col} TEXT;")
    conn.commit()

    # Build the concatenation logic
    concat_expr = f"COALESCE(CAST(\"{th_attr_col}\" AS TEXT), '') || ', ' || "
    concat_expr += f"COALESCE(CAST(\"{sn_attr_col}\" AS TEXT), '')"
    if add_sn_attr_col:
        concat_expr += f" || COALESCE(CAST(\"{add_sn_attr_col}\" AS TEXT), '')"

    # Update the normalised label column with concatenated values
    cur.execute(f"""
    UPDATE {schema_name}.{table_name}
    SET {norm_label_col} = TRIM({concat_expr});
    """)

    conn.commit()
    cur.close()

def create_simplified_label_for_streetnumbers(conn, schema_name, table,
                                              id_col, simp_label_col, th_attr_col, sn_attr_col, add_sn_attr_col=None, exceptions=None):
    cur = conn.cursor()

    cur.execute(f"ALTER TABLE {schema_name}.{table} ADD COLUMN IF NOT EXISTS {simp_label_col} TEXT;")
    conn.commit()

    columns = [id_col, th_attr_col, sn_attr_col]
    if add_sn_attr_col is not None:
        columns.append(add_sn_attr_col)
    columns = [f"\"{x}\"" for x in columns]
    query_cols = ", ".join(columns)

    cur.execute(f"""
    SELECT {query_cols}
    FROM {schema_name}.{table}
    """)

    all_queries = []

    for row in cur.fetchall():
        id_val, th_val, sn_val = row[0], row[1], row[2]
        try:
            add_sn_val = row[3]
        except:
            add_sn_val = None

        update_query = create_update_query_to_add_simplified_name(schema_name, table, id_val, th_val, sn_val, add_sn_val, id_col, simp_label_col, exceptions)
        all_queries.append(update_query)

    full_query = ";".join(all_queries)
    cur.execute(full_query)

    cur.close()
    conn.commit()

def create_update_query_to_add_simplified_name(schema_name, table, id_val, th_val, sn_val, add_sn_val, id_col, simp_label_col, exceptions):
    th_label = str(th_val) if th_val is not None else th_val
    sn_label = str(sn_val) if sn_val is not None else sn_val
    add_sn_label = str(add_sn_val) if add_sn_val is not None else add_sn_val

    if None not in [sn_label, add_sn_label]:
        sn_label += add_sn_label

    simp_label = get_address_label_from_street_and_number(sn_label, th_label, exceptions)

    if simp_label is not None:
        update_query = f"UPDATE {schema_name}.{table} SET \"{simp_label_col}\"='{simp_label}' WHERE \"{id_col}\"='{id_val}'"
    else:
        update_query = f"UPDATE {schema_name}.{table} SET \"{simp_label_col}\"=NULL WHERE \"{id_col}\"='{id_val}'"        
    return update_query

def get_exceptions(exceptions):
    exceptions_dict = {}
    for name_to_replace, replacing_name in exceptions:
        name_to_replace_label = sp.simplify_french_name_version(name_to_replace, "thoroughfare")
        replacing_name_label = sp.simplify_french_name_version(replacing_name, "thoroughfare")
        exceptions_dict[name_to_replace_label] = replacing_name_label
        
    return exceptions_dict

def add_name_columns_for_multiple_tables(conn, tables_settings, schema_name, simp_label_col, norm_label_col, exceptions):
    exceptions_dict = get_exceptions(exceptions)

    # Create simplified labels for tables
    for table_set in tables_settings:
        add_name_columns_for_table(table_set, conn, schema_name, simp_label_col, norm_label_col, exceptions_dict)
        table_name = table_set["name"]
        print(f"{table_name} processed")

def add_name_columns_for_table(table_settings: dict, conn, schema_name, simp_label_col, norm_label_col, exceptions=None):
    table_name = table_settings.get("name")
    th_attr_col = table_settings.get("th_attr_col")
    sn_attr_col = table_settings.get("sn_attr_col")
    add_sn_attr_col = table_settings.get("add_sn_attr_col")
    
    id_col, _ = get_postgis_table_geom_settings(conn, schema_name, table_name)
    create_simplified_label_for_streetnumbers(conn, schema_name, table_name, id_col, simp_label_col, th_attr_col, sn_attr_col, add_sn_attr_col, exceptions)
    create_normalised_label_for_streetnumbers(conn, schema_name, table_name, norm_label_col, th_attr_col, sn_attr_col, add_sn_attr_col)

def get_address_label_from_street_and_number(number:str, street_label:str, exceptions:dict):
    if None in [number, street_label]:
        return None
    
    sn_label = sp.simplify_nolang_name_version(number, "number")
    _, th_label = sp.normalize_and_simplify_name_version(street_label, "thoroughfare", "fr")

    # If th_label is in exceptions, it must be remplaced by the related exception
    exc_th_label = exceptions.get(th_label)
    if exc_th_label is not None:
        th_label = exc_th_label

    simp_label = f"{th_label}||{sn_label}"
    
    return simp_label

def create_links_table(conn, schema_name, links_table_name,
                       id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                       geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col, epsg_code, overwrite=False):
    
    query = ""
    if overwrite:
        query += f"""DROP TABLE IF EXISTS {schema_name}.{links_table_name} CASCADE ;"""
        
    query += f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{links_table_name} (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "{id_table_from_col}" TEXT,
    "{table_name_from_col}" TEXT,
    "{id_table_to_col}" TEXT,
    "{table_name_to_col}" TEXT,
    "{validated_col}" INTEGER DEFAULT 1 CHECK ("{validated_col}" IN (-1, 0, 1)),
    "{method_col}" TEXT DEFAULT 'manual',
    "{creation_date_col}" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "{to_keep_col}" BOOLEAN DEFAULT TRUE,
    "{similar_geom_col}" BOOLEAN DEFAULT FALSE,
    "{geom_col}" geometry(LineString, {epsg_code})
    );
    
    ALTER TABLE {schema_name}.{links_table_name}
    ALTER COLUMN "{geom_col}" TYPE geometry(LineString, {epsg_code})
    USING "{geom_col}";
    """
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def create_links_table_from_multiple_tables(conn, tables_settings, schema_name, links_table_name,
                                            id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                            geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col, 
                                            simp_label_col, norm_label_col, default_epsg_code, max_distance):
    # # Create links between tables
    table_pairs = list(itertools.combinations(tables_settings, 2))
    for pair in table_pairs:
        table_settings_from, table_settings_to = pair[0], pair[1]
        table_name_from, table_name_to = table_settings_from["name"], table_settings_to["name"]
        create_links_between_similar_addresses(table_settings_from, table_settings_to, conn, schema_name, links_table_name,
                                                id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                                geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col,
                                                default_epsg_code, simp_label_col,
                                                max_distance=max_distance)
        create_views_for_table_pair(conn, schema_name, links_table_name, table_name_from, table_name_to,
                                    id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                    geom_col, validated_col, norm_label_col)
        print(f"Links between {table_name_from} and {table_name_to} created")


def create_links_between_similar_addresses(table_settings_from, table_settings_to, conn, schema_name, links_table_name,
                                          id_col_from, id_col_to, table_name_from_col, table_name_to_col,
                                          geom_col, validated_col, to_keep_col, similar_geom_col, method_col, creation_date_col,
                                          epsg_code, simp_label_col, max_distance=5):
    table_name_from = table_settings_from.get("name")
    table_name_to = table_settings_to.get("name")

    id_col_1, geom_col_1 = get_postgis_table_geom_settings(conn, schema_name, table_name_from)
    id_col_2, geom_col_2 = get_postgis_table_geom_settings(conn, schema_name, table_name_to)

    query1 = f""" 
    INSERT INTO {schema_name}.{links_table_name}
    (\"{id_col_from}\", \"{id_col_to}\", \"{table_name_from_col}\", \"{table_name_to_col}\", \"{geom_col}\", \"{validated_col}\", \"{to_keep_col}\", \"{method_col}\")
    SELECT
        t1.{id_col_1},
        t2.{id_col_2},
        '{table_name_from}',
        '{table_name_to}',
        ST_SetSRID(
            ST_MakeLine(
                ST_Transform(ST_Centroid(t1.{geom_col_1}), {epsg_code}),
                ST_Transform(ST_Centroid(t2.{geom_col_2}), {epsg_code})
            ), {epsg_code}
        ),
        0,
        FALSE,
        'automatic'
    FROM {schema_name}.{table_name_from} AS t1, {schema_name}.{table_name_to} AS t2
    WHERE t1.{simp_label_col} = t2.{simp_label_col};
    """

    query2 = f"""
    UPDATE {schema_name}.{links_table_name}
    SET {similar_geom_col} = (ST_Length(ST_Transform("{geom_col}", {epsg_code})) < {max_distance});
    """
    
    cur = conn.cursor()
    cur.execute(query1)
    cur.execute(query2)
    conn.commit()


def create_views_for_table_pair(conn, schema_name, links_table_name, from_table_name, to_table_name,
                                id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                geom_col, validated_col, norm_label_col):
    
    create_view_for_links_table(conn, schema_name, links_table_name, from_table_name, to_table_name,
                                id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                geom_col, validated_col)
    create_view_for_unlinked_addresses(conn, schema_name, links_table_name, from_table_name, from_table_name, to_table_name, "from",
                                       id_table_from_col, geom_col, norm_label_col)
    create_view_for_unlinked_addresses(conn, schema_name, links_table_name, to_table_name, from_table_name, to_table_name, "to",
                                       id_table_to_col, geom_col, norm_label_col)

def create_view_for_links_table(conn, schema_name, links_table_name, from_table_name, to_table_name,
                                id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                geom_col, validated_col):
    """
    Create a view for the links table with the specified parameters.
    """

    from_table_name_sanitised = from_table_name.replace("_adresses", "")
    to_table_name_sanitised = to_table_name.replace("_adresses", "")
    view_name = f"links_{from_table_name_sanitised}_{to_table_name_sanitised}"
    query = f"""
    CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
    SELECT 
        id,
        {links_table_name}.{id_table_from_col},
        {links_table_name}.{id_table_to_col},
        {links_table_name}.{geom_col},
        {links_table_name}.{validated_col}
    FROM {schema_name}.{links_table_name}
    WHERE {links_table_name}.{table_name_from_col} = '{from_table_name}' AND {links_table_name}.{table_name_to_col} = '{to_table_name}';
    """
    
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def create_view_for_unlinked_addresses(conn, schema_name, links_table_name, table_name, from_table_name, to_table_name, infix,
                                       id_table_col, geom_col, norm_label_col):
    """
    Create a view for unlinked addresses with the specified parameters.
    """

    id_col, geom_col = get_postgis_table_geom_settings(conn, schema_name, from_table_name)

    from_table_name_sanitised = from_table_name.replace("_adresses", "")
    to_table_name_sanitised = to_table_name.replace("_adresses", "")
    view_name = f"unlinked_{infix}_{from_table_name_sanitised}_{to_table_name_sanitised}"

    query = f"""
    CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
    SELECT {id_col} AS id, {geom_col} AS geom, {norm_label_col} AS name
    FROM  {schema_name}.{table_name}
    WHERE {id_col} NOT IN (
        SELECT {id_table_col}
        FROM {schema_name}.{links_table_name}
        WHERE table_from = '{from_table_name}' AND table_to = '{to_table_name}'
    )
    """

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

################################## Insertion of manual links from CSV ######################################

def insert_manual_links_from_csv(conn, schema_name, links_table_name, csv_file_path,
                                 id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                                 geom_col, validated_col, method_col, creation_date_col, epsg_code):
    """
    Insert manual links from a CSV file into the database.
    """
    
    values = get_values_from_csv(csv_file_path)
    insert_manual_links(conn, schema_name, links_table_name, values,
                        id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col, validated_col)
    add_geometries_to_links(conn, schema_name, links_table_name,
                            id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col, geom_col, epsg_code)
    
    
def insert_manual_links(conn, schema_name, links_table_name, values:list[dict],
                        id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col, method_col):
    """
    Insert manual links into the links table (geometries are not added for the moment)
    """
    
    if values is None or values == []:
        return None
    
    query = f"""
    INSERT INTO {schema_name}.{links_table_name} (\"{id_table_from_col}\", \"{id_table_to_col}\", \"{table_name_from_col}\", \"{table_name_to_col}\", \"{method_col}\")
    VALUES
    """

    for value in values:
        id1 = value.get(id_table_from_col)
        id2 = value.get(id_table_to_col)
        table1 = value.get(table_name_from_col)
        table2 = value.get(table_name_to_col)
        method = 'manual'
        query += f"""('{id1}', '{id2}', '{table1}', '{table2}', {method}),"""
    
    query = query[:-1] + ";"
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()

def add_geometries_to_links(conn, schema_name, links_table_name,
                            id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col, geom_col, epsg_code):
    """
    Add geometries to the links table based on the table pairs.
    """

    table_pairs = get_table_pairs(conn, schema_name, links_table_name)
    
    for pair in table_pairs:
        table1, table2 = pair[0], pair[1]
        id_col_1, geom_col_1 = get_postgis_table_geom_settings(conn, schema_name, table1)
        id_col_2, geom_col_2 = get_postgis_table_geom_settings(conn, schema_name, table2)

        query = f"""
        UPDATE {schema_name}.{links_table_name}
        SET {geom_col} = ST_SetSRID(
            ST_MakeLine(
                ST_Transform(ST_Centroid(t1.{geom_col_1}), {epsg_code}),
                ST_Transform(ST_Centroid(t2.{geom_col_2}), {epsg_code})
            ), {epsg_code}
        )
        FROM {schema_name}.{table1} AS t1, {schema_name}.{table2} AS t2
        WHERE {links_table_name}.{id_table_from_col} = t1.{id_col_1} AND {links_table_name}.{id_table_to_col} = t2.{id_col_2}
        AND {links_table_name}.{table_name_from_col} = '{table1}' AND {links_table_name}.{table_name_to_col} = '{table2}' AND {geom_col} IS NULL
        """
        
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

def get_values_from_csv(csv_file_path):
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        values = list(reader)

    return values

def get_table_pairs(conn, schema_name, links_table_name):
    """
    Get unique table pairs from the links table.
    """
    cur = conn.cursor()
    cur.execute(f"""
    SELECT DISTINCT table_from, table_to
    FROM {schema_name}.{links_table_name}
    """)
    
    table_pairs = set(cur.fetchall())
    cur.close()

    return table_pairs

################################################ 

def create_view_for_final_links(conn, schema_name, links_table_name, to_keep_col):
    """
    Create a view for final links that are kept.
    """
    view_name = f"final_links"

    query = f"""
    CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
    SELECT *
    FROM  {schema_name}.{links_table_name}
    WHERE {to_keep_col}
    """

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def get_links_to_keep(conn, schema_name, links_table_name, table_settings:list, id_from_col, table_from_col, table_to_col, to_keep_col):
    """
    Get the links that are marked to be kept.
    """

    table_pairs = generate_setting_pairs(table_settings)

    for pair in table_pairs:
        table_from_set, table_to_set = pair[0], pair[1]
        table_from_name = table_from_set.get("name")
        table_to_name = table_to_set.get("name")
        get_links_to_keep_from_table_pair(conn, schema_name, links_table_name, table_from_name, table_to_name, id_from_col, table_from_col, table_to_col, to_keep_col)

def get_links_to_keep_from_table_pair(conn, schema_name, links_table_name, table_from_name, table_to_name, id_from_col, table_from_col, table_to_col, to_keep_col):
    """
    Get the links that are marked to be kept.
    """

    query = f"""
    UPDATE {schema_name}.{links_table_name}
    SET {to_keep_col} = TRUE
    WHERE {table_from_col} = '{table_from_name}' AND {table_to_col} = '{table_to_name}' AND {id_from_col} NOT IN (
        SELECT DISTINCT {id_from_col}
        FROM {schema_name}.{links_table_name}
        WHERE {to_keep_col} AND {table_from_col} = '{table_from_name}'
    )
    """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

def generate_setting_pairs(settings_list):
    pairs = []
    n = len(settings_list)
    for k in range(1, n):
        for i in range(n - k):
            pairs.append((settings_list[i], settings_list[i + k]))
    return pairs

def extract_manual_links(conn, schema_name, links_table_name,
                      id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                      geom_col, validated_col, to_keep_col, method_col, creation_date_col, output_csv_path):
    query = f"""
        SELECT \"{id_table_from_col}\", \"{table_name_from_col}\", \"{id_table_to_col}\", \"{table_name_to_col}\", \"{validated_col}\", \"{method_col}\", \"{creation_date_col}\", \"{to_keep_col}\", ST_AsText({geom_col}) AS wkt_geom
        FROM {schema_name}.{links_table_name}
        WHERE "{method_col}" = 'manual'
    """

    cur = conn.cursor()
    cur.execute(query)

    # Récupère les résultats et les noms des colonnes
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    # Ferme le curseur proprement
    cur.close()

    # Mets les données dans un DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)

    print(f"Exported to {output_csv_path}")

def extract_to_keep_links(conn, tables_settings, schema_name, links_table_name,
                      id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                      geom_col, to_keep_col, similar_geom_col, simp_label_col, output_csv_path):
    cur = conn.cursor()

    # Mets les données dans un DataFrame
    df = pd.DataFrame()

    # Create links between tables
    table_pairs = list(itertools.combinations(tables_settings, 2))
    for pair in table_pairs:
        table_settings_from, table_settings_to = pair[0], pair[1]
        t1_name, t2_name = table_settings_from["name"], table_settings_to["name"]

        query = f"""
            SELECT
                lt.id AS id,
                lt.{id_table_from_col} AS {id_table_from_col},
                lt.{table_name_from_col} AS {table_name_from_col},
                lt.{id_table_to_col} AS {id_table_to_col},
                lt.{table_name_to_col} AS {table_name_to_col},
                lt.{similar_geom_col} AS {similar_geom_col},
                t1.normalised_label AS label_from,
                t2.normalised_label AS label_to,
                ST_AsText(ST_Transform(lt.{geom_col}, 2154)) AS {geom_col},
                ST_AsText(ST_Transform(t1.geom, 2154)) AS geom_from,
                ST_AsText(ST_Transform(t2.geom, 2154)) AS geom_to,
                t1.{simp_label_col} AS simp_label
                FROM
                {schema_name}.{links_table_name} AS lt,
                {schema_name}.{t1_name} AS t1,
                {schema_name}.{t2_name} AS t2
                WHERE
                lt.{to_keep_col} AND
                lt.{table_name_from_col} = '{t1_name}' AND
                lt.{table_name_to_col} = '{t2_name}' AND
                lt.{id_table_from_col} = t1.id AND
                lt.{id_table_to_col} = t2.id
        """

        cur.execute(query)

        # Récupère les résultats et les noms des colonnes
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        # Mets les données dans un DataFrame
        dfBis = pd.DataFrame(rows, columns=columns)
        df = pd.concat([df, dfBis], ignore_index=True)

    # Ferme le curseur proprement
    cur.close()

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)

    print(f"Exported to {output_csv_path}")

def extract_ground_truth_links(conn, tables_settings, schema_name, links_table_name,
                      id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                      geom_col, to_keep_col, similar_geom_col, simp_label_col, output_csv_path):
    cur = conn.cursor()

    # Mets les données dans un DataFrame
    df = pd.DataFrame()

    # Create links between tables
    table_pairs = list(itertools.combinations(tables_settings, 2))
    for pair in table_pairs:
        table_settings_from, table_settings_to = pair[0], pair[1]
        t1_name, t2_name = table_settings_from["name"], table_settings_to["name"]

        query = f"""
            SELECT DISTINCT
                lt.{table_name_from_col} AS {table_name_from_col},
                lt.{table_name_to_col} AS {table_name_to_col},
                lt.{similar_geom_col} AS {similar_geom_col},
                t1.{simp_label_col} AS simp_label
                FROM
                {schema_name}.{links_table_name} AS lt,
                {schema_name}.{t1_name} AS t1,
                {schema_name}.{t2_name} AS t2
                WHERE
                lt.{to_keep_col} AND
                lt.{table_name_from_col} = '{t1_name}' AND
                lt.{table_name_to_col} = '{t2_name}' AND
                lt.{id_table_from_col} = t1.id AND
                lt.{id_table_to_col} = t2.id
        """

        cur.execute(query)

        # Récupère les résultats et les noms des colonnes
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        # Mets les données dans un DataFrame
        dfBis = pd.DataFrame(rows, columns=columns)
        df = pd.concat([df, dfBis], ignore_index=True)

    # Ferme le curseur proprement
    cur.close()

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)

    print(f"Exported to {output_csv_path}")


def extract_streetnumbers_without_link(conn, tables_settings, schema_name, links_table_name,
                      id_table_from_col, id_table_to_col, table_name_from_col, table_name_to_col,
                      geom_col, to_keep_col, similar_geom_col, simp_label_col, output_csv_path):
    cur = conn.cursor()

    # Mets les données dans un DataFrame
    df = pd.DataFrame()

    for table_set in tables_settings:
        table_name = table_set["name"]
        query = f"""
            SELECT DISTINCT
                '{table_name}' AS table, {simp_label_col} AS simp_label
            FROM {schema_name}.{table_name}
            WHERE id NOT IN (
                SELECT {id_table_from_col} FROM {schema_name}.{links_table_name} WHERE {table_name_from_col} = '{table_name}'
                UNION
                SELECT {id_table_to_col} FROM {schema_name}.{links_table_name} WHERE {table_name_to_col} = '{table_name}'
            ) AND {simp_label_col} IS NOT NULL ;
        """

        cur.execute(query)

        # Récupère les résultats et les noms des colonnes
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        # Mets les données dans un DataFrame
        dfBis = pd.DataFrame(rows, columns=columns)
        df = pd.concat([df, dfBis], ignore_index=True)

    # Ferme le curseur proprement
    cur.close()

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)

    print(f"Exported to {output_csv_path}")
