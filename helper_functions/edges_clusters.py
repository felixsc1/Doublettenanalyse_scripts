import os
import pickle

from helper_functions.hardcoded_values import produkte_dict
from .file_io_functions import load_data
import pandas as pd
import networkx as nx


def match_organizations_internally_simplified(df, personen=False):
    # Currently used in production.
    rows_list = []

    # Handling 'VerknuepftesObjektID_list' and 'Verknuepfungsart_list'
    for i, row in df.iterrows():
        source = row["ReferenceID"]
        targets = row["VerknuepftesObjektID_list"]
        match_types = row["Verknuepfungsart_list"]

        if not all(pd.isna(x) for x in targets) and not all(
            pd.isna(x) for x in match_types
        ):
            for target, match_type in zip(targets, match_types):
                if not pd.isna(target) and not pd.isna(match_type):
                    if target in df["ReferenceID"].values:
                        new_row = {
                            "source": source,
                            "target": target,
                            "match_type": match_type,
                        }
                        rows_list.append(new_row)

    # Optimized handling of 'Telefonnummer', 'EMailAdresse', 'Name', and 'Adresse'
    if not personen:
        columns_to_check = [
            ("Telefon", "Telefonnummer"),
            ("Email", "EMailAdresse"),
            ("Name", "Name_Zeile2"),
            ("Adresse", "address_full"),
        ]
    else:
        columns_to_check = [
            ("Telefon", "Telefonnummer"),
            ("Email", "EMailAdresse"),
            ("Name", "Name"),
            ("Adresse", "address_full"),
        ]

    for contact_type, column_name in columns_to_check:
        # Remove rows with NA values (during cleanup empty strings and "nan" must have been replaced)
        df.replace("", pd.NA, inplace=True)
        valid_contacts = df.loc[df[column_name].notna()]

        # Self merge to find matching rows
        merged = valid_contacts.merge(
            valid_contacts[[column_name, "ReferenceID"]], on=column_name
        )
        merged = merged[merged["ReferenceID_x"] != merged["ReferenceID_y"]]
        merged = merged.rename(
            columns={"ReferenceID_x": "source", "ReferenceID_y": "target"}
        )
        merged["match_type"] = contact_type

        # Append the results to the list
        rows_list.extend(merged[["source", "target", "match_type"]].to_dict("records"))

    output_df = pd.DataFrame(rows_list)

    return output_df


def organisationsrollen_group_aggregate(df):
    # Input is das raw xlsx vom Organisationsrollen query.
    # Benutzt dictionary oben Produkt_typ als string, Count für eine Kombination aus typ/inh./rechempf/korrempf und liste der produkt-objekte zu generieren.
    grouped_df = (
        df.groupby(
            [
                "Inhaber_RefID",
                "Rechnungsempfaenger_RefID",
                "Korrespondenzempfaenger_RefID",
                "FullID",
            ]
        )
        .agg(
            Produkt_count=pd.NamedAgg(column="Produkt_RefID", aggfunc="size"),
            Produkte=pd.NamedAgg(column="ProduktObj", aggfunc=list),
            **{
                col: pd.NamedAgg(column=col, aggfunc="first")
                for col in df.columns
                if col
                not in [
                    "Inhaber_RefID",
                    "Rechnungsempfaenger_RefID",
                    "Korrespondenzempfaenger_RefID",
                    "Produkt_RefID",
                    "FullID",
                    "ProduktObj",
                ]
            },
        )
        .reset_index()
    )

    # Create 'Produkt_typ' by mapping 'FullID' through the data dictionary
    grouped_df["Produkt_typ"] = grouped_df["FullID"].map(produkte_dict)

    return grouped_df


def generate_edge_list_from_orginationsrollen_aggregate(df):
    """
    "source" eines edges ist kombination aus liste der objekte+produkttyp newline count. um eindeutig zu sein.
    Für eine source gibt es jeweils 3 row / Targets (inh. rechempf. korrempf.) mit RefID und label.
    diese edge list kann dann mit internen endge list der organisationen concateniert werden.
    """
    edge_list = []
    for _, row in df.iterrows():
        source = (
            str(row["Produkte"])
            + str(row["Produkt_typ"])
            + "\n"
            + str([row["Produkt_count"]][0])
        )
        for target, target_type in [
            (row["Rechnungsempfaenger_RefID"], "Rechnungsempfaenger"),
            (row["Korrespondenzempfaenger_RefID"], "Korrespondenzempfaenger"),
            (row["Inhaber_RefID"], "Inhaber"),
        ]:
            edge_list.append(
                {"source": source, "target": target, "match_type": target_type}
            )

    # Create a DataFrame from the edge list
    edges_df = pd.DataFrame(edge_list)
    return edges_df

def match_organizations_between_dataframes(d1, df2):
    # Very similar to match_organizations_internally_simplified, but checks if target is present in df2.
    # Currently only finds VerknuepftesObjekt edges (no name, address, etc.)
    rows_list = []

    # Handling 'VerknuepftesObjektID_list' and 'Verknuepfungsart_list'
    for i, row in d1.iterrows():
        source = row["ReferenceID"]
        targets = row["VerknuepftesObjektID_list"]
        match_types = row["Verknuepfungsart_list"]

        if not all(pd.isna(x) for x in targets) and not all(
            pd.isna(x) for x in match_types
        ):
            for target, match_type in zip(targets, match_types):
                if not pd.isna(target) and not pd.isna(match_type):
                    if target in df2["ReferenceID"].values:
                        new_row = {
                            "source": source,
                            "target": target,
                            "match_type": match_type,
                        }
                        rows_list.append(new_row)

    output_df = pd.DataFrame(rows_list)

    return output_df


def cleanup_edges_df(df):
    """
    Merges edges with "Name", "Telefon", "Email", "Adresse" into one.
    Adds bidirectional flag.
    """
    # Directly set 'bidirectional' to True for specific match types
    df["bidirectional"] = df["match_type"].isin(["Name", "Telefon", "Email", "Adresse"])

    # Step 1: Create a column with sorted tuples of 'source' and 'target'
    df["sorted_edge"] = df.apply(
        lambda row: tuple(sorted([row["source"], row["target"]])), axis=1
    )

    # Group by 'sorted_edge' and 'match_type', and aggregate
    df = (
        df.groupby(["sorted_edge", "match_type"])
        .agg({"source": "first", "target": "first", "bidirectional": "first"})
        .reset_index()
    )

    # Step 3: Remove the 'sorted_edge' column
    df.drop("sorted_edge", axis=1, inplace=True)

    # Splitting DataFrame based on 'match_type'
    merge_df = df[df["match_type"].isin(["Name", "Telefon", "Email", "Adresse"])]
    keep_df = df[~df["match_type"].isin(["Name", "Telefon", "Email", "Adresse"])]

    # Grouping rows by 'source', 'target', and 'bidirectional', aggregating 'match_type'
    merge_df = (
        merge_df.groupby(["source", "target", "bidirectional"])
        .agg({"match_type": lambda x: ", ".join(sorted(set(x)))})
        .reset_index()
    )

    # Concatenate both DataFrames back together
    output_df = pd.concat([merge_df, keep_df]).reset_index(drop=True)
    return output_df


def find_clusters_all(df, special_nodes, skip_singular_clusters=False):
    """
    Here we use the networkx package for graph-based analyses.
    Input is the df generated by the function "match_organizations_internally",
    or any df that has a source, target and label column.
    Special_nodes is a set of nodes that should not be considered as central nodes nor included in cluster sizes.
    Note: this finds all clusters, i.e. nodes that just have any kind of connection. They are not necessarily Doubletten!
    """
    # Create a new graph from edge list
    G = nx.from_pandas_edgelist(
        df, "source", "target", edge_attr="match_type", create_using=nx.Graph()
    )

    # Find connected components
    connected_components = nx.connected_components(G)

    # Collect connected components (clusters) in a list
    clusters = []
    for i, component in enumerate(connected_components):
        # note: if singular clusters are skipped, i count may not be continuous.
        subgraph = G.subgraph(component)

        # Filter out the special nodes
        filtered_nodes = [node for node in component if node not in special_nodes]

        # Skip clusters of size 1
        if (len(filtered_nodes) < 2) and skip_singular_clusters:
            continue

        # Finding the most central node based on degree
        central_node = (
            max((node for node in subgraph.degree(filtered_nodes)), key=lambda x: x[1])[
                0
            ]
            if filtered_nodes
            else None
        )
        cluster_size = len(filtered_nodes)

        clusters.append(
            {
                "cluster_id": i,
                "nodes": list(component),
                "cluster_size": cluster_size,
                "central_node": central_node,
            }
        )

    # Convert to DataFrame for better visualization and further analysis
    cluster_df = pd.DataFrame(clusters)

    return cluster_df


def create_edges_and_clusters(file_paths):
    # Main function that calls all those above. Finds ALL clusters that are connected (not just Dubletten), used for visualization.
    
    # Assuming pickle file was created by raw_cleanup()
    with open(
        "data/calculated/personen_organisationen_dfs_processed.pickle",
        "rb",
    ) as file:
        dfs = pickle.load(file)
    df_personen = dfs["personen"]
    df_organisationen = dfs["organisationen"]

    # helper functions to map ReferenceIDs to hyperlinks
    def create_link_mapping(df):
        return dict(zip(df["ReferenceID"], df["Objekt_link"]))

    personen_link_map = create_link_mapping(df_personen)
    organisationen_link_map = create_link_mapping(df_organisationen)

    def generate_links_for_cluster(node_list):
        links = []
        for node in node_list:
            link = personen_link_map.get(node) or organisationen_link_map.get(node)
            links.append(link if link is not None else None)
        return links

    edges_organisationen = match_organizations_internally_simplified(df_organisationen)

    organisationsrollen_df = load_data(file_paths["organisationsrollen"])
    edges_organisationsrollen = organisationsrollen_group_aggregate(
        organisationsrollen_df
    )
    edges_organisationsrollen = generate_edge_list_from_orginationsrollen_aggregate(
        edges_organisationsrollen
    )

    edges_personen = match_organizations_internally_simplified(
        df_personen, personen=True
    )

    edges_personen_to_organisationen = match_organizations_between_dataframes(
        df_personen, df_organisationen
    )

    # TODO: Personenrollen-edges hinzufügen. Im GraphViewer app bereits vorhanden!

    # Summarize and clean up everything.
    edge_list = []
    edge_list.append(edges_organisationen)
    edge_list.append(edges_personen)
    edge_list.append(edges_organisationsrollen)
    edge_list.append(edges_personen_to_organisationen)
    all_edges = pd.concat(edge_list, ignore_index=True)

    all_edges = cleanup_edges_df(all_edges)

    special_nodes = set(
        edges_organisationsrollen["source"].unique()
    )  # should not count towards cluster sizes or be central nodes.

    all_clusters = find_clusters_all(
        all_edges, special_nodes, skip_singular_clusters=False
    )

    # add new link column with list of links corresponding to list of nodes
    all_clusters["link"] = all_clusters["nodes"].apply(generate_links_for_cluster)

    # Store dataframes as pickle
    dfs = {"edges": all_edges, "clusters": all_clusters}
    # Create the directory if it doesn't exist
    directory = "data/calculated"
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "edges_clusters_dfs.pickle"), "wb") as file:
        pickle.dump(dfs, file)

    return


def abbreviate_first_name(name):
    name = " ".join(name.split())  # Remove extra spaces
    # name = name.title()  # Standardize case
    parts = name.split()
    if len(parts) > 1 and not parts[0].endswith('.'):
        parts[0] = parts[0][0] + '.'  # Replace the first name with its abbreviation
    return ' '.join(parts)


def find_name_adresse_doubletten(df, organisationen=True, abbreviated_first_name=False):
    """
    A cluster here is just any group of organizations with exact match in Name and Adresse (email irrelevant). Used for Doubletten analyses.
    """
    # Group by 'Name' and 'address_full', and assign cluster_id
    if organisationen:
        df['cluster_id'] = df.groupby(['Name_Zeile2', 'address_full']).ngroup()
    else:
        if abbreviated_first_name:
            df["Name_abbrev"] = df["Name"].apply(abbreviate_first_name)
            df['cluster_id'] = df.groupby(['Name_abbrev', 'address_full']).ngroup()
        else:
            df['cluster_id'] = df.groupby(['Name', 'address_full']).ngroup()

    # Keep only groups with at least 2 identical rows
    df = df[df.groupby('cluster_id')['cluster_id'].transform('size') > 1]

    if organisationen:
        df.sort_values(by='Name_Zeile2', inplace=True)
    else:
        df.sort_values(by='Name', inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df