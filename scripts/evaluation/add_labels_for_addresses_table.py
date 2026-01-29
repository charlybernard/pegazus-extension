from scripts.utils import str_processing as sp

def add_label_columns_for_table(pm, schema_name, table_name, id_col, number_col, street_name_col, simp_label_col, norm_label_col, exceptions=None):
    create_simplified_label_for_streetnumbers(pm, schema_name, table_name, id_col, simp_label_col, number_col, street_name_col, exceptions)
    create_normalised_label_for_streetnumbers(pm, schema_name, table_name, norm_label_col, number_col, street_name_col)

def create_normalised_label_for_streetnumbers(
        pm, schema_name, table_name,
        norm_label_col, number_col, street_name_col):
    
    query = f"""
    ALTER TABLE {schema_name}.{table_name} DROP COLUMN IF EXISTS {norm_label_col} ;
    ALTER TABLE {schema_name}.{table_name}
    ADD COLUMN {norm_label_col} TEXT GENERATED ALWAYS AS ({number_col} || ', ' || {street_name_col}) STORED;
    """
    
    pm.execute_query(query)

def create_simplified_label_for_streetnumbers(
        pm, schema_name, table_name,
        id_col, simp_label_col, number_col, street_name_col,
        exceptions=None):

    pm.execute_query(f"""
        ALTER TABLE {schema_name}.{table_name} DROP COLUMN IF EXISTS {simp_label_col} ;
        ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS {simp_label_col} TEXT;
        
        """)

    results = pm.fetch_all(f"""SELECT {id_col}, {number_col}, {street_name_col} FROM {schema_name}.{table_name}""")
    all_queries = []

    for row in results:
        id_val, sn_val, th_val = row[0], row[1], row[2]

        update_query = create_update_query_to_add_simplified_name(schema_name, table_name, id_val, sn_val, th_val, id_col, simp_label_col, exceptions)
        all_queries.append(update_query)

    full_query = ";".join(all_queries)
    pm.execute_query(full_query)


def create_update_query_to_add_simplified_name(schema_name, table_name, id_val, sn_val, th_val, id_col, simp_label_col, exceptions):
    th_label = str(th_val) if th_val is not None else th_val
    sn_label = str(sn_val) if sn_val is not None else sn_val

    simp_label = get_address_label_from_street_and_number(sn_label, th_label, exceptions)

    if simp_label is not None:
        update_query = f"UPDATE {schema_name}.{table_name} SET \"{simp_label_col}\"='{simp_label}' WHERE \"{id_col}\"='{id_val}'"
    else:
        update_query = f"UPDATE {schema_name}.{table_name} SET \"{simp_label_col}\"=NULL WHERE \"{id_col}\"='{id_val}'"        
    return update_query

def get_address_label_from_street_and_number(number:str, street_label:str, exceptions:dict):
    if not isinstance(exceptions, dict):
        exceptions = {}
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