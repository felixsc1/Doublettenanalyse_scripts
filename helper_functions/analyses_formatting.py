import pandas as pd
from .hardcoded_values import produkte_dict

def renumber_pairs(column):
    # To recognize clusters I have a continous cluster-id, e.g. 1,1,2,2,3,3, but due to filtering there are some gaps, 1,1,3,3, ...
    # this will just renumber it to 1,1,2,2
    # numbers dont even have to be in ascending order, e.g. if i sorted dataframe by name before and cluster_ids become 3,3,1,1,2,2
    # will just respect the order of appearance and re-number starting with 1.
    mapping = {}
    new_id = 1

    for val in column:
        if val not in mapping:
            mapping[val] = new_id
            new_id += 1

    new_values = [mapping[val] for val in column]
    return new_values



def renumber_and_sort_alphanumeric(df, column='cluster_id'):    # Split the column into two
    # Split the column into two
    df['num'] = df[column].str.split('_').str[0]
    df['alpha'] = df[column].str.split('_').str[1]
    df['num'] = df['num'].astype(int)

    # Sort the dataframe
    df.sort_values(['num', 'alpha'], inplace=True)

    # Create a new column with rank
    df['rank'] = df['num'].rank(method='dense').astype(int)

    # Combine 'rank' and 'alpha' to get the desired format
    df[column] = df['rank'].astype(str) + '_' + df['alpha']

    # Drop the temporary columns
    df.drop(['num', 'alpha', 'rank'], axis=1, inplace=True)

    return df


def set_master_flag(df):
    if "cluster_id" not in df.columns:
        raise ValueError("DataFrame does not contain 'cluster_id' column.")

    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()

    # Sort by 'score' and 'CreatedAt' in descending order for processing
    sorted_df = result_df.sort_values(by=["score", "CreatedAt"], ascending=[False, False])

    # Group by 'cluster_id' and mark the first row in each group as 'X' and others as ''
    sorted_df["master"] = sorted_df.groupby("cluster_id").cumcount().map({0: "X"}).fillna("")

    # Handle the case where all rows have the same 'cluster_id'
    if sorted_df["master"].eq("").all():
        sorted_df["master"].iloc[0] = "X"  # Mark the first row as 'master'

    # Assign the 'master' column directly to the original DataFrame
    result_df["master"] = sorted_df["master"]

    return result_df


def add_organisationsrollen_string_columns(df1, df2):
    """
    Inputs are df_organisationen and organisationsrollen_df.
    Adds a column "Organisationsrollen", a list where each element is a string like "Inhaber (FDA)"
    and a corresponding column "Organisationrollen_ProduktID" with the corresponding ProduktID for which this organisation is Inhaber.
    """
    # Initialize the new columns with empty lists
    df1['Organisationsrollen'] = [[] for _ in range(len(df1))]
    df1['Organisationrollen_ProduktID'] = [[] for _ in range(len(df1))]

    # Iterate over each row in df1
    for idx, row in df1.iterrows():
        # Check for matches in df2
        for ref_id in ['Inhaber_RefID', 'Rechnungsempfaenger_RefID', 'Korrespondenzempfaenger_RefID']:
            matched_rows = df2[df2[ref_id] == row['ReferenceID']]
            for _, matched_row in matched_rows.iterrows():
                role = ref_id.split('_')[0]  # Extracts the role from the column name
                fullid_value = produkte_dict.get(matched_row['FullID'], matched_row['FullID'])
                df1.at[idx, 'Organisationsrollen'].append(f"{role} ({fullid_value})")
                df1.at[idx, 'Organisationrollen_ProduktID'].append(matched_row['Produkt_RefID'])

    return df1


def filter_verknuepfungen(row):
    """
    For Organisations-analyses we only want to show Verknüpfungen to Personen. 
    This filters out anything from the list that is not 'Mitarbeiter' or 'Administrator'
    """
    filtered_indices = [i for i, x in enumerate(row['Verknuepfungsart_list']) if x in ['Mitarbeiter', 'Administrator']]
    row['Verknuepfungsart_list'] = [row['Verknuepfungsart_list'][i] for i in filtered_indices]
    row['VerknuepftesObjektID_list'] = [row['VerknuepftesObjektID_list'][i] for i in filtered_indices]
    row['VerknuepftesObjekt_list'] = [row['VerknuepftesObjekt_list'][i] for i in filtered_indices]
    return row


def final_touch(df, cols_to_keep, two_roles=False, alphanumeric=False):
    """
    For a single dataframe. 
    newer analyses have alphanumeric cluster_ids (1_a, 1_b, 2_a, etc.), so we need to sort them differently.
    """
    if "cluster_id" not in df.columns:
        raise ValueError("DataFrame does not contain 'cluster_id' column.")
    
    df = df.apply(filter_verknuepfungen, axis=1)
    df = set_master_flag(df)
    df = df[cols_to_keep]
    df["score"] = df["score"].astype(int)
    
    if two_roles or alphanumeric:
        df = renumber_and_sort_alphanumeric(df.copy(), column='cluster_id')
    else:
        df["cluster_id"] = df["cluster_id"].astype(int)
        df["cluster_id"] = renumber_pairs(df["cluster_id"])
        df.sort_values(by="cluster_id", inplace=True)
        
    df.reset_index(drop=True, inplace=True)
    return df


def final_touch_batch(df_dict, cols_to_keep, two_roles=False, alphanumeric=False):
    """
    Processes any number of dataframes at once.
    Expects a dictionary, with the Description as key and the dataframe or nested dictionary of dataframes as value, as well as the columns to keep.
    Returns a dictionary with key = description and value = dataframe or nested dictionary of dataframes.
    """
    result_dict = {}
    for produktname, value in df_dict.items():
        if isinstance(value, dict):  # Check if the value is a nested dictionary
            nested_result = {}
            for name, df in value.items():
                nested_result[name] = final_touch(df, cols_to_keep, two_roles, alphanumeric=alphanumeric)
            result_dict[produktname] = nested_result
        else:  # If the value is a single dataframe
            result_dict[produktname] = final_touch(value, cols_to_keep, two_roles, alphanumeric=alphanumeric)

    return result_dict
