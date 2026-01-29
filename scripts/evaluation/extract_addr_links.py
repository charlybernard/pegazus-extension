import pandas as pd

def extract_ground_truth_links(
        pm, 
        links_schema_name, links_table_name,
        addr_schema_name, addr_table_name,
        links_source_from_col, links_source_to_col,
        links_id_from_col, links_similar_geom_col, links_succesive_geom_col,
        addr_id_col, addr_simp_label_col,
        output_csv_path):

    query = f"""
        SELECT DISTINCT
            l.{links_source_from_col} AS {links_source_from_col},
            l.{links_source_to_col} AS {links_source_to_col},
            l.{links_similar_geom_col} AS {links_similar_geom_col},
            a.{addr_simp_label_col} AS {addr_simp_label_col}
            FROM
            {links_schema_name}.{links_table_name} AS l,
            {addr_schema_name}.{addr_table_name} AS a
            WHERE
            l.{links_succesive_geom_col} AND
            l.{links_id_from_col} = a.{addr_id_col}
    """

    rows = pm.fetch_all(query)
    columns = [links_source_from_col, links_source_to_col, links_similar_geom_col, addr_simp_label_col]

    # Mets les données dans un DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)

def extract_streetnumbers_without_link(
        pm, 
        links_schema_name, links_table_name,
        addr_schema_name, addr_table_name,
        links_id_from_col, links_id_to_col,
        addr_id_col, addr_source_col, addr_simp_label_col,
        output_csv_path):

    query = f"""
    SELECT DISTINCT a.{addr_source_col} AS {addr_source_col}, a.{addr_simp_label_col} AS {addr_simp_label_col}
    FROM {addr_schema_name}.{addr_table_name} a
    WHERE NOT EXISTS (
        SELECT 1
        FROM {links_schema_name}.{links_table_name} l
        WHERE l.{links_id_from_col} = a.{addr_id_col} OR l.{links_id_to_col} = a.{addr_id_col}
        );
    """

    rows = pm.fetch_all(query)
    columns = [addr_source_col, addr_simp_label_col]

    # Mets les données dans un DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Exporte en CSV
    df.to_csv(output_csv_path, index=False)