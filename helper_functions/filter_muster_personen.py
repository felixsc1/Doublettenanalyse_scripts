import pandas as pd

from helper_functions.filter_muster_organisationen import general_exclusion_criteria_personen



def filter_personen_connected_to_same_organisation(df_personen, df_organisationen):
    """
    A first step: Resulting groups will all be connected to the same organisation in some way.
    Later one can simply count "Verknuefpungsart", e.g. 1 Administrator + 1 Mitarbeiter.
    
    New cluster_id: If two or more members share organisation x they become 1_a, 
    if there are two or more others (still doubletten in terms of name/address) connected to organisation x' they become 1_b, etc.
    """
    output_df = pd.DataFrame()
    alphabet = list('abcdefghijklmnopqrstuvwxyz')
    
    grouped = df_personen.groupby('cluster_id')
    
    for cluster_id, group in grouped:
        unique_ids = group['VerknuepftesObjektID_list'].apply(lambda x: x[0]).unique()
        suffix_index = 0  # Initialize suffix index
        
        for unique_id in unique_ids:
            subgroup = group[group['VerknuepftesObjektID_list'].apply(lambda x: x[0] == unique_id)]
            # Check if VerknuepftesObjektID matches any ReferenceID in df_organisationen
            if df_organisationen['ReferenceID'].isin([unique_id]).any():
                if len(subgroup) >= 2:
                    new_cluster_id = f"{cluster_id}_{alphabet[suffix_index]}"
                    subgroup['cluster_id'] = new_cluster_id
                    output_df = pd.concat([output_df, subgroup])
                    suffix_index += 1  # Increment only when adding a subgroup
            else:
                for _, row in subgroup.iterrows():
                    print(f"Warning: {row['Name']} has no matching organisation for {row['VerknuepftesObjektID']}")
    
    # Reset index for clarity
    output_df.reset_index(drop=True, inplace=True)
    
    # Ensure the cluster_id column is the only one or appropriately named
    if 'modified_cluster_id' in output_df.columns:
        output_df.drop(columns=['modified_cluster_id'], inplace=True)
    
    return output_df


def split_groups_mitarbeiter_admnistrator(df):
    """
    Input is a dataframe of personendoubletten, connected to the same organisation as either Mitarbeiter or Administrator.
    Output is a dict with three dataframes: only_Mitarbeiter, one_Administrator, multiple_Administrator.
    """
    # Initialize empty DataFrames for each category
    df_mitarbeiter_only = pd.DataFrame()
    df_one_administrator = pd.DataFrame()
    df_multiple_administrator = pd.DataFrame()

    # Group by 'cluster_id'
    for _, group in df.groupby('cluster_id'):
        # Check the 'Verknuepfungsart_list' for each group
        admin_count = sum('Administrator' in verknuepfungsart_list for verknuepfungsart_list in group['Verknuepfungsart_list'])
        
        if admin_count == 0:
            df_mitarbeiter_only = pd.concat([df_mitarbeiter_only, group])
        elif admin_count == 1:
            df_one_administrator = pd.concat([df_one_administrator, group])
        else:  # More than one 'Administrator'
            df_multiple_administrator = pd.concat([df_multiple_administrator, group])

    # Create output dictionary, omitting empty DataFrames
    output_dict = {}
    if not df_mitarbeiter_only.empty:
        output_dict['only_Mitarbeiter'] = df_mitarbeiter_only
    if not df_one_administrator.empty:
        output_dict['one_Administrator'] = df_one_administrator
    if not df_multiple_administrator.empty:
        output_dict['multiple_Administrator'] = df_multiple_administrator

    return output_dict


