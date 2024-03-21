import pandas as pd
import multiprocessing
from functools import partial

from helper_functions.analyses_formatting import set_master_flag
from .hardcoded_values import produkte_dict_name_first


def general_exclusion_criteria(
    df, no_Produkte=True, no_Geschaeftspartner=True, no_Servicerole=True
):
    # Apply conditions to each row
    if no_Produkte:
        df = df[((df["Produkt_Inhaber"] == 0) & (df["Produkt_Adressant"] == 0))]
    if no_Geschaeftspartner:
        df = df[~df["Geschaeftspartner_list"].apply(lambda gp: len(gp) != 0)]
    if no_Servicerole:
        df = df[df["Servicerole_count"] == 0]

    # Filter out groups with less than 2 members
    return df[df.groupby("cluster_id")["cluster_id"].transform("size") >= 2]


def general_exclusion_criteria_personen(
    df,
    no_Produkte=True,
    no_Geschaeftspartner=True,
    no_Servicerole=True,
    only_physisch=False,
    only_mitarbeiter=True,
):
    """
    Removes entries that don't match a certain criterion. If this results in a group of Doubletten having only one member, the whole group is removed.
    Note we currently look at Personen that ONLY have a single Verknüpfung to one organisation. In future cleanup may want to relax that, though only 15 cases in doubletten (300 total).
    """
    if no_Produkte:
        df = df[df["Produkt_rolle"].apply(lambda x: len(x) == 0)]
    if no_Geschaeftspartner:
        df = df[df["Geschaeftspartner_list"].apply(lambda gp: len(gp) == 0)]
    if no_Servicerole:
        df = df[df["Servicerole_string"] == ""]

    if only_physisch:
        df = df[df["Versandart"] == "Physisch"]
    else:
        # Is there anything other than pysisch or portal, and do we want to include this at some point?
        df = df[(df["Versandart"] == "Physisch") | (df["Versandart"] == "Portal")]

    if only_mitarbeiter:
        df = df[
            df["Verknuepfungsart_list"].apply(
                lambda x: len(x) == 1 and x[0] == "Mitarbeiter"
            )
        ]
    else:
        # Alternative: Can have one Mitarbeiter or one Administrator connection
        df = df[
            df["Verknuepfungsart_list"].apply(
                lambda x: len(x) == 1
                and (x[0] == "Mitarbeiter" or x[0] == "Administrator")
            )
        ]

    # Remove groups with only one member left after filtering
    df = df.groupby("cluster_id").filter(lambda x: len(x) > 1)

    return df


def FDA_servicerole(df):
    """
    Alle Doubletten haben selbe Versandart.
    Genau eine der Doubletten hat Servicerolle "FDA".
    Alle anderen Doubletten haben keine Servicerolle.
    """

    def filter_groups(group):
        fda_count = (group["Servicerole_string"] == "FDA").sum()
        empty_count = (group["Servicerole_string"] == "").sum()
        same_versandart = group["Versandart"].nunique() == 1

        return (fda_count == 1) and (empty_count == len(group) - 1) and same_versandart

    groups = [group for _, group in df.groupby("cluster_id") if filter_groups(group)]

    if not groups:
        print("Warning: No groups meet the criteria.")
        return pd.DataFrame()

    return pd.concat(groups)


def add_singular_produkte_columns_group(group, organisationsrollen_df, produkt):
    """
    Inputs are simply all groups (cluster_id). If any group has no Produkete of the specified type, the newly created columns will just be empty.
    By running cleanup_produkte_columns() after this one, we eventually get the clusters for the specific produkt as expected.
    """
    # Find the FullID corresponding to the given produkt
    full_id = produkte_dict_name_first.get(produkt, None)

    if not full_id:
        raise ValueError(f"Produkt '{produkt}' not found in produkte_dict")

    roles = [
        "Inhaber_RefID",
        "Rechnungsempfaenger_RefID",
        "Korrespondenzempfaenger_RefID",
    ]

    role_columns = {
        "Inhaber_RefID": ("Inhaber_Objekt", "Inhaber_ProduktID"),
        "Rechnungsempfaenger_RefID": ("Rechempf_Objekt", "Rechempf_ProduktID"),
        "Korrespondenzempfaenger_RefID": ("Korrempf_Objekt", "Korrempf_ProduktID"),
    }

    # Initialize new columns if they don't exist
    for column in role_columns.values():
        for col in column:
            if col not in group.columns:
                group[col] = [[] for _ in range(len(group))]

    # Iterate over each row in the group
    for index, row in group.iterrows():
        ref_id = row["ReferenceID"]

        # Filter rows in organisationsrollen_df based on ref_id and roles
        filtered_df = organisationsrollen_df[
            organisationsrollen_df[roles].apply(lambda x: ref_id in x.values, axis=1)
        ]

        for role in roles:
            # Filter for the specific role
            role_filtered_df = filtered_df[filtered_df[role] == ref_id]

            # Check if the role exists for the specified FullID
            if not role_filtered_df[role_filtered_df["FullID"] == full_id].empty:
                objekt_col, produktid_col = role_columns[role]
                group.at[index, objekt_col] = role_filtered_df["ProduktObj"].tolist()
                group.at[index, produktid_col] = role_filtered_df[
                    "Produkt_RefID"
                ].tolist()

    return group


def add_singular_produkte_columns_group_simplified(
    group, filtered_organisationsrollen_df
):
    # if organisationsrollen_df is already pre-filtered

    roles = [
        "Inhaber_RefID",
        "Rechnungsempfaenger_RefID",
        "Korrespondenzempfaenger_RefID",
    ]
    role_columns = {
        "Inhaber_RefID": ("Inhaber_Objekt", "Inhaber_ProduktID"),
        "Rechnungsempfaenger_RefID": ("Rechempf_Objekt", "Rechempf_ProduktID"),
        "Korrespondenzempfaenger_RefID": ("Korrempf_Objekt", "Korrempf_ProduktID"),
    }

    # Initialize new columns if they don't exist
    for column in role_columns.values():
        for col in column:
            if col not in group.columns:
                group[col] = [[] for _ in range(len(group))]

    # Iterate over each row in the group
    for index, row in group.iterrows():
        ref_id = row["ReferenceID"]

        # Filter rows in filtered_organisationsrollen_df based on ref_id and roles
        filtered_df = filtered_organisationsrollen_df[
            filtered_organisationsrollen_df[roles].apply(
                lambda x: ref_id in x.values, axis=1
            )
        ]

        for role in roles:
            # Filter for the specific role
            role_filtered_df = filtered_df[filtered_df[role] == ref_id]

            # Update the group DataFrame
            if not role_filtered_df.empty:
                objekt_col, produktid_col = role_columns[role]
                group.at[index, objekt_col] = role_filtered_df["ProduktObj"].tolist()
                group.at[index, produktid_col] = role_filtered_df[
                    "Produkt_RefID"
                ].tolist()

    return group


def cleanup_produkte_columns(df):
    """
    To be executed after add_singular_produkte_columns_group()
    input df still has groups with empty lists in Inhaber_Objekt etc., which will be removed here.
    Also if any group member has e.g. a produkt listed as Inhaber, but that produkt is nowhere listed as korrempf/rechempf in the group, discards the whole group.
    It does not however care how the products are distributed (e.g. all roles for one org, or distributed across 3)
    """

    def check_group(group):
        # Check if Inhaber_Objekt and Korrempf_Objekt columns are empty in the entire group
        if (
            group["Inhaber_Objekt"].apply(len).sum() == 0
            and group["Korrempf_Objekt"].apply(len).sum() == 0
        ):
            return False  # Mark group for removal

        # Flatten the lists in each column and count occurrences
        all_inhaber = [item for sublist in group["Inhaber_Objekt"] for item in sublist]
        all_korrempf = [
            item for sublist in group["Korrempf_Objekt"] for item in sublist
        ]

        all_rechempf = [
            item for sublist in group["Rechempf_Objekt"] for item in sublist
        ]
        unique_items = set(all_inhaber + all_rechempf + all_korrempf)

        # Check if each item appears in the required columns
        for item in unique_items:
            if all_inhaber.count(item) != 1 or all_korrempf.count(item) != 1:
                return False  # Mark group for removal

            if all_rechempf.count(item) != 1:
                return False  # Mark group for removal if Rechempf_Objekt is considered

        return True  # Keep the group

    # Apply the check to each group and filter the DataFrame
    filtered_df = df.groupby("cluster_id").filter(check_group)

    return filtered_df


def cleanup_produkte_columns_only_2_roles(df):
    """
    Modified to filter out groups where an element appears in exactly two of the three roles.
    Groups where all three roles are empty lists or where an element only appears under one role or in all three roles are removed.
    """

    def check_group(group):
        # Check if all three columns are empty lists in the entire group
        if (
            all(group["Inhaber_Objekt"].apply(len) == 0)
            and all(group["Korrempf_Objekt"].apply(len) == 0)
            and all(group["Rechempf_Objekt"].apply(len) == 0)
        ):
            return False  # Mark group for removal

        # Flatten the lists in each column
        all_inhaber = [item for sublist in group["Inhaber_Objekt"] for item in sublist]
        all_korrempf = [
            item for sublist in group["Korrempf_Objekt"] for item in sublist
        ]
        all_rechempf = [
            item for sublist in group["Rechempf_Objekt"] for item in sublist
        ]

        unique_items = set(all_inhaber + all_rechempf + all_korrempf)

        # Check if each item appears in exactly two of the three roles
        for item in unique_items:
            count = 0
            if item in all_inhaber:
                count += 1
            if item in all_korrempf:
                count += 1
            if item in all_rechempf:
                count += 1

            if count != 2:  # Remove group if item is in 1 or 3 roles
                return False

        return True  # Keep the group if all items are in exactly two roles

    # Apply the check to each group and filter the DataFrame
    filtered_df = df.groupby("cluster_id").filter(check_group)

    return filtered_df


# !!!!  next several functions are all related to parallel processing.  !!!


def worker_function(group_data, filtered_organisationsrollen_df):
    group, data = group_data
    return add_singular_produkte_columns_group_simplified(
        data, filtered_organisationsrollen_df
    )


def parallel_apply(
    grouped_df, func, filtered_organisationsrollen_df, num_processes=None
):
    # Create a list of tuples (group_key, group_data)
    group_list = [(group, data) for group, data in grouped_df]

    # Partial function with fixed arguments
    partial_func = partial(
        func, filtered_organisationsrollen_df=filtered_organisationsrollen_df
    )

    # Create a pool of processes
    pool = multiprocessing.Pool(processes=num_processes)

    # Map the function to the chunks and collect the results
    result_chunks = pool.map(partial_func, group_list)

    # Close the pool and wait for work to finish
    pool.close()
    pool.join()

    # Concatenate the results
    return pd.concat(result_chunks)


def get_product_information(df_input, organisationsrollen_df, produktname):
    # uses parallel processing to utilize 100% CPU, took about 13-16min for one produkt.

    # Pre-filter organisationsrollen_df by produktname
    full_id = produkte_dict_name_first.get(produktname, None)
    if not full_id:
        raise ValueError(f"Produkt '{produktname}' not found in produkte_dict")

    filtered_organisationsrollen_df = organisationsrollen_df[
        organisationsrollen_df["FullID"] == full_id
    ]

    grouped_df = df_input.groupby("cluster_id")
    result_df = parallel_apply(
        grouped_df, worker_function, filtered_organisationsrollen_df
    ).reset_index(drop=True)

    return result_df


def batch_process_produkte(df, organisationsrollen_df, produktnamen):
    """
    First calls get_product_information() to get Produkt information for every row (slow part).
    Then calls two variations of cleanup functions that check if any group of Doubletten has either all 3 roles for a product or only 2 roles.
    After this we still have a single dataframe for one product. Further processing below is to split it up into different "muster".
    """
    result_3 = {}
    result_2 = {}
    for produktname in produktnamen:
        product_info = get_product_information(df, organisationsrollen_df, produktname)
        print(f"✅ Done with {produktname}")
        cleaned_product_info_3 = cleanup_produkte_columns(product_info)
        cleaned_product_info_2 = cleanup_produkte_columns_only_2_roles(product_info)

        if cleaned_product_info_3.empty:
            print(f"❌ No Doubletten with 3 roles found!")
        else:
            result_3[produktname] = cleaned_product_info_3

        if cleaned_product_info_2.empty:
            print(f"❌ No Doubletten with 2 roles found!")
        else:
            result_2[produktname] = cleaned_product_info_2
    return result_3, result_2


# ---- Functions below for organizing Produkte/Organisationsrollenanalysen ---


# def split_produkte_groups(df):
#     """
#     Checks if a group/cluster of Doubletten contains all roles for the given product.
#     Organizes them into list with 4 dataframes.
#     """
#     df_inhaber_korrempf = pd.DataFrame(columns=df.columns)
#     df_inhaber_rechempf = pd.DataFrame(columns=df.columns)
#     df_korrempf_rechempf = pd.DataFrame(columns=df.columns)
#     df_remaining = pd.DataFrame(columns=df.columns)

#     for cluster_id, group in df.groupby("cluster_id"):
#         # Initialize sets to track common elements
#         common_inhaber_korrempf = set()
#         common_inhaber_rechempf = set()
#         common_korrempf_rechempf = set()

#         # Check each row for common elements
#         for _, row in group.iterrows():
#             inhaber = set(row["Inhaber_Objekt"])
#             rechempf = set(row["Rechempf_Objekt"])
#             korrempf = set(row["Korrempf_Objekt"])

#             if inhaber & korrempf and not inhaber & rechempf:
#                 common_inhaber_korrempf.update(inhaber & korrempf)
#             if inhaber & rechempf and not inhaber & korrempf:
#                 common_inhaber_rechempf.update(inhaber & rechempf)
#             if korrempf & rechempf and not korrempf & inhaber:
#                 common_korrempf_rechempf.update(korrempf & rechempf)

#         # Function to determine if row contains common elements (Just for sorting, the row that contains two common element shows first now)
#         def contains_common(row, common_elements):
#             return any(elem in row for elem in common_elements)

#         # Sort and classify the group based on the common elements found
#         if common_inhaber_korrempf:
#             group["sort_key"] = group["Inhaber_Objekt"].apply(
#                 lambda x: contains_common(x, common_inhaber_korrempf)
#             )
#             df_inhaber_korrempf = pd.concat(
#                 [
#                     df_inhaber_korrempf,
#                     group.sort_values(by="sort_key", ascending=False).drop(
#                         "sort_key", axis=1
#                     ),
#                 ]
#             )
#         elif common_inhaber_rechempf:
#             group["sort_key"] = group["Rechempf_Objekt"].apply(
#                 lambda x: contains_common(x, common_inhaber_rechempf)
#             )
#             df_inhaber_rechempf = pd.concat(
#                 [
#                     df_inhaber_rechempf,
#                     group.sort_values(by="sort_key", ascending=False).drop(
#                         "sort_key", axis=1
#                     ),
#                 ]
#             )
#         elif common_korrempf_rechempf:
#             group["sort_key"] = group["Korrempf_Objekt"].apply(
#                 lambda x: contains_common(x, common_korrempf_rechempf)
#             )
#             df_korrempf_rechempf = pd.concat(
#                 [
#                     df_korrempf_rechempf,
#                     group.sort_values(by="sort_key", ascending=False).drop(
#                         "sort_key", axis=1
#                     ),
#                 ]
#             )
#         else:
#             df_remaining = pd.concat([df_remaining, group])

#     df_list = []
#     df_list_names = []
#     if not df_inhaber_korrempf.empty:
#         df_list.append(df_inhaber_korrempf)
#         df_list_names.append("Rechempf_separat")
#     if not df_inhaber_rechempf.empty:
#         df_list.append(df_inhaber_rechempf)
#         df_list_names.append("Korrempf_separat")
#     if not df_korrempf_rechempf.empty:
#         df_list.append(df_korrempf_rechempf)
#         df_list_names.append("Inhaber_separat")
#     if not df_remaining.empty:
#         df_list.append(df_remaining)
#         df_list_names.append("Sonstige")

#     return df_list, df_list_names


def split_produkte_groups(df):
    """
    Commented-out version above works, but was meant for clusters of size 2 and has outdated numbering and filtering scheme.
    Checks if a a group contains all roles for the given product.
    Discards group members that have no role (or that have all three roles).
    Organizes them into list with 3 dataframes.
    """
    df_inhaber_separat = pd.DataFrame(columns=df.columns)
    df_rechempf_separat = pd.DataFrame(columns=df.columns)
    df_korrempf_separat = pd.DataFrame(columns=df.columns)

    alphabet = list(string.ascii_lowercase)
    suffixes = alphabet + [i + j for i in alphabet for j in alphabet]

    for cluster_id, group in df.groupby("cluster_id"):
        unique_elements = set()
        for member in group.itertuples():
            for col in ["Inhaber_Objekt", "Rechempf_Objekt", "Korrempf_Objekt"]:
                unique_elements.update(getattr(member, col))

        sorted_elements = sorted(map(str, unique_elements))
        suffix_dict = {
            elem: f"_{suffixes[i]}" for i, elem in enumerate(sorted_elements)
        }

        for element in unique_elements:
            element_roles = {
                col: set()
                for col in ["Inhaber_Objekt", "Rechempf_Objekt", "Korrempf_Objekt"]
            }
            for member in group.itertuples():
                for col in element_roles.keys():
                    if element in getattr(member, col):
                        element_roles[col].add(member.Index)

            # Ensure roles are distributed across exactly two members and all three roles are covered
            if (
                sum(len(roles) for roles in element_roles.values()) == 3
                and len({idx for roles in element_roles.values() for idx in roles}) == 2
            ):
                # Determine which member has which role(s)
                member_roles_count = {
                    member: sum(member in roles for roles in element_roles.values())
                    for member in group.index
                }

                # Find the member with a single role and two roles
                single_role_member = next(
                    (
                        member
                        for member, count in member_roles_count.items()
                        if count == 1
                    ),
                    None,
                )
                two_roles_member = next(
                    (
                        member
                        for member, count in member_roles_count.items()
                        if count == 2
                    ),
                    None,
                )

                # This checks which dataframe to append based on the role of the single_role_member
                if single_role_member is not None and two_roles_member is not None:
                    for role, members in element_roles.items():
                        if single_role_member in members:
                            single_role = role
                            break

                    for member in group.itertuples():
                        if member.Index in [single_role_member, two_roles_member]:
                            new_row = {
                                col: getattr(member, col)
                                for col in df.columns
                                if col
                                not in [
                                    "Inhaber_Objekt",
                                    "Rechempf_Objekt",
                                    "Korrempf_Objekt",
                                    "Inhaber_ProduktID",
                                    "Rechempf_ProduktID",
                                    "Korrempf_ProduktID",
                                ]
                            }
                            new_row["cluster_id"] = (
                                f"{cluster_id}{suffix_dict[str(element)]}"
                            )

                            for col, id_col in zip(
                                [
                                    "Inhaber_Objekt",
                                    "Rechempf_Objekt",
                                    "Korrempf_Objekt",
                                ],
                                [
                                    "Inhaber_ProduktID",
                                    "Rechempf_ProduktID",
                                    "Korrempf_ProduktID",
                                ],
                            ):
                                if element in getattr(member, col):
                                    index = getattr(member, col).index(element)
                                    new_row[col] = element
                                    new_row[id_col] = (
                                        getattr(member, id_col)[index]
                                        if index < len(getattr(member, id_col))
                                        else ""
                                    )
                                else:
                                    new_row[col] = ""
                                    new_row[id_col] = ""

                            new_row_df = pd.DataFrame([new_row])

                            # Assign to the correct dataframe based on the single_role
                            if single_role == "Inhaber_Objekt":
                                df_inhaber_separat = pd.concat(
                                    [df_inhaber_separat, new_row_df], ignore_index=True
                                )
                            elif single_role == "Rechempf_Objekt":
                                df_rechempf_separat = pd.concat(
                                    [df_rechempf_separat, new_row_df], ignore_index=True
                                )
                            elif single_role == "Korrempf_Objekt":
                                df_korrempf_separat = pd.concat(
                                    [df_korrempf_separat, new_row_df], ignore_index=True
                                )

    df_list = []
    df_list_names = []
    if not df_inhaber_separat.empty:
        df_list.append(df_inhaber_separat)
        df_list_names.append("Inhaber_Separat")
    if not df_rechempf_separat.empty:
        df_list.append(df_rechempf_separat)
        df_list_names.append("Rechempf_Separat")
    if not df_korrempf_separat.empty:
        df_list.append(df_korrempf_separat)
        df_list_names.append("KorrEmpf_Separat")

    return df_list, df_list_names


# def split_produkte_groups_two_roles(df):
#     """
#     Modified from split_produkte_groups().
#     Organizes the DataFrame into four new DataFrames based on common elements in two of the three roles.
#     """
#     # List columns that are expected to contain lists
#     list_columns = ['Inhaber_Objekt', 'Rechempf_Objekt', 'Korrempf_Objekt']

#     # Initialize the four DataFrames
#     df_inhaber_korrempf = pd.DataFrame(columns=df.columns)
#     df_inhaber_rechempf = pd.DataFrame(columns=df.columns)
#     df_korrempf_rechempf = pd.DataFrame(columns=df.columns)
#     df_remaining = pd.DataFrame(columns=df.columns)

#     for cluster_id, group in df.groupby("cluster_id"):
#         # Collect all unique elements from the list columns in the group
#         unique_elements = set()
#         for member in group.itertuples():
#             for col in list_columns:
#                 unique_elements.update(getattr(member, col))

#         # Create a suffix dictionary for each unique element
#         suffix_dict = {elem: f"_{chr(97+i)}" for i, elem in enumerate(sorted(unique_elements))}

#         for element in unique_elements:
#             for member in group.itertuples():
#                 new_row = {col: getattr(member, col) for col in df.columns if col not in list_columns}
#                 new_row['cluster_id'] = f"{cluster_id}{suffix_dict[element]}"

#                 for col in list_columns:
#                     new_row[col] = [element] if element in getattr(member, col) else []

#                 new_row_df = pd.DataFrame([new_row])

#                 if any(new_row['Inhaber_Objekt']) and any(new_row['Korrempf_Objekt']):
#                     df_inhaber_korrempf = pd.concat([df_inhaber_korrempf, new_row_df], ignore_index=True)
#                 elif any(new_row['Inhaber_Objekt']) and any(new_row['Rechempf_Objekt']):
#                     df_inhaber_rechempf = pd.concat([df_inhaber_rechempf, new_row_df], ignore_index=True)
#                 elif any(new_row['Korrempf_Objekt']) and any(new_row['Rechempf_Objekt']):
#                     df_korrempf_rechempf = pd.concat([df_korrempf_rechempf, new_row_df], ignore_index=True)
#                 else:
#                     df_remaining = pd.concat([df_remaining, new_row_df], ignore_index=True)


#     df_list = []
#     df_list_names = []
#     if not df_inhaber_korrempf.empty:
#         df_list.append(df_inhaber_korrempf)
#         df_list_names.append("Inhaber_KorrEmpf")
#     if not df_inhaber_rechempf.empty:
#         df_list.append(df_inhaber_rechempf)
#         df_list_names.append("Inhaber_RechEmpf")
#     if not df_korrempf_rechempf.empty:
#         df_list.append(df_korrempf_rechempf)
#         df_list_names.append("KorrEmpf_RechEmpf")
#     if not df_remaining.empty:
#         df_list.append(df_remaining)
#         df_list_names.append("Sonstige")

#     return df_list, df_list_names

import string


def split_produkte_groups_two_roles(df):
    """
    Organizes the DataFrame into four new DataFrames based on common elements in two of the three roles.
    Each unique element within a cluster_id group will produce two rows with the same suffix.
    Members without a role are removed (although they are still doubletten, but must be handled elsewhere).
    Corresponding ProduktID for each Objekt element is also placed in the new row.
    """
    # List columns that are expected to contain lists and their corresponding ProduktID columns
    objekt_columns = ["Inhaber_Objekt", "Rechempf_Objekt", "Korrempf_Objekt"]
    produktid_columns = [
        "Inhaber_ProduktID",
        "Rechempf_ProduktID",
        "Korrempf_ProduktID",
    ]
    list_columns = objekt_columns + produktid_columns

    # Initialize the four DataFrames
    df_inhaber_korrempf = pd.DataFrame(columns=df.columns)
    df_inhaber_rechempf = pd.DataFrame(columns=df.columns)
    df_korrempf_rechempf = pd.DataFrame(columns=df.columns)
    df_sonstige = pd.DataFrame(columns=df.columns)

    alphabet = list(string.ascii_lowercase)
    suffixes = alphabet + [i + j for i in alphabet for j in alphabet]

    for cluster_id, group in df.groupby("cluster_id"):
        unique_elements = set()
        for member in group.itertuples():
            for col in objekt_columns:
                unique_elements.update(getattr(member, col))

        # suffix_dict = {elem: f"_{suffixes[i]}" for i, elem in enumerate(sorted(unique_elements))}
        sorted_elements = sorted(map(str, unique_elements))
        suffix_dict = {
            elem: f"_{suffixes[i]}" for i, elem in enumerate(sorted_elements)
        }

        for element in unique_elements:
            element_roles = {col: set() for col in objekt_columns}
            for member in group.itertuples():
                for col in objekt_columns:
                    if element in getattr(member, col):
                        element_roles[col].add(member.Index)

            for member in group.itertuples():
                new_row = {
                    col: getattr(member, col)
                    for col in df.columns
                    if col not in list_columns
                }
                new_row["cluster_id"] = f"{cluster_id}{suffix_dict[str(element)]}"

                for col, id_col in zip(objekt_columns, produktid_columns):
                    if element in getattr(member, col):
                        index = getattr(member, col).index(element)
                        new_row[col] = element
                        new_row[id_col] = (
                            getattr(member, id_col)[index]
                            if index < len(getattr(member, id_col))
                            else ""
                        )
                    else:
                        new_row[col] = ""
                        new_row[id_col] = ""

                new_row_df = pd.DataFrame([new_row])

                # only process rows that actually have a role:
                if any(new_row[col] for col in objekt_columns):
                    # Determine the DataFrame to which the row should be assigned
                    if (
                        len(element_roles["Inhaber_Objekt"]) > 1
                        and len(element_roles["Rechempf_Objekt"]) > 1
                    ):
                        df_sonstige = pd.concat(
                            [df_sonstige, new_row_df], ignore_index=True
                        )
                    elif (
                        len(element_roles["Inhaber_Objekt"]) > 1
                        and len(element_roles["Korrempf_Objekt"]) > 1
                    ):
                        df_sonstige = pd.concat(
                            [df_sonstige, new_row_df], ignore_index=True
                        )
                    elif (
                        len(element_roles["Rechempf_Objekt"]) > 1
                        and len(element_roles["Korrempf_Objekt"]) > 1
                    ):
                        df_sonstige = pd.concat(
                            [df_sonstige, new_row_df], ignore_index=True
                        )
                    elif (
                        len(element_roles["Inhaber_Objekt"]) > 0
                        and len(element_roles["Rechempf_Objekt"]) > 0
                    ):
                        if new_row["Inhaber_Objekt"] and new_row["Rechempf_Objekt"]:
                            df_sonstige = pd.concat(
                                [df_sonstige, new_row_df], ignore_index=True
                            )
                        else:
                            df_inhaber_rechempf = pd.concat(
                                [df_inhaber_rechempf, new_row_df], ignore_index=True
                            )
                    elif (
                        len(element_roles["Inhaber_Objekt"]) > 0
                        and len(element_roles["Korrempf_Objekt"]) > 0
                    ):
                        if new_row["Inhaber_Objekt"] and new_row["Korrempf_Objekt"]:
                            df_sonstige = pd.concat(
                                [df_sonstige, new_row_df], ignore_index=True
                            )
                        else:
                            df_inhaber_korrempf = pd.concat(
                                [df_inhaber_korrempf, new_row_df], ignore_index=True
                            )
                    elif (
                        len(element_roles["Rechempf_Objekt"]) > 0
                        and len(element_roles["Korrempf_Objekt"]) > 0
                    ):
                        if new_row["Rechempf_Objekt"] and new_row["Korrempf_Objekt"]:
                            df_sonstige = pd.concat(
                                [df_sonstige, new_row_df], ignore_index=True
                            )
                        else:
                            df_korrempf_rechempf = pd.concat(
                                [df_korrempf_rechempf, new_row_df], ignore_index=True
                            )
                    else:
                        df_sonstige = pd.concat(
                            [df_sonstige, new_row_df], ignore_index=True
                        )

    # Ensure there are no single-member groups in df_sonstige
    df_sonstige = df_sonstige[df_sonstige.duplicated(subset="cluster_id", keep=False)]
    df_korrempf_rechempf = df_korrempf_rechempf[
        df_korrempf_rechempf.duplicated(subset="cluster_id", keep=False)
    ]
    df_inhaber_rechempf = df_inhaber_rechempf[
        df_inhaber_rechempf.duplicated(subset="cluster_id", keep=False)
    ]
    df_inhaber_korrempf = df_inhaber_korrempf[
        df_inhaber_korrempf.duplicated(subset="cluster_id", keep=False)
    ]

    df_list = []
    df_list_names = []
    if not df_inhaber_korrempf.empty:
        df_list.append(df_inhaber_korrempf)
        df_list_names.append("Inhaber_KorrEmpf")
    if not df_inhaber_rechempf.empty:
        df_list.append(df_inhaber_rechempf)
        df_list_names.append("Inhaber_RechEmpf")
    if not df_korrempf_rechempf.empty:
        df_list.append(df_korrempf_rechempf)
        df_list_names.append("KorrEmpf_RechEmpf")
    if not df_sonstige.empty:
        df_list.append(df_sonstige)
        df_list_names.append("Sonstige")

    return df_list, df_list_names


def split_produkte_groups_komplette_doublette(df):
    """
    Checks if cluster_id groups of input df have all three roles for a product for one member each.
    Discards additional members that have no role or groups that don't match the criteria.
    """
    # List columns that are expected to contain lists and their corresponding ProduktID columns
    objekt_columns = ["Inhaber_Objekt", "Rechempf_Objekt", "Korrempf_Objekt"]
    produktid_columns = [
        "Inhaber_ProduktID",
        "Rechempf_ProduktID",
        "Korrempf_ProduktID",
    ]
    list_columns = objekt_columns + produktid_columns

    # Initialize the two DataFrames
    df_komplette_doubletten = pd.DataFrame(columns=df.columns)
    df_sonstige = pd.DataFrame(columns=df.columns)

    alphabet = list(string.ascii_lowercase)
    suffixes = alphabet + [i + j for i in alphabet for j in alphabet]

    for cluster_id, group in df.groupby("cluster_id"):
        unique_elements = set()
        for member in group.itertuples():
            for col in objekt_columns:
                unique_elements.update(getattr(member, col))

        sorted_elements = sorted(map(str, unique_elements))
        suffix_dict = {
            elem: f"_{suffixes[i]}" for i, elem in enumerate(sorted_elements)
        }

        for element in unique_elements:
            element_roles = {col: set() for col in objekt_columns}
            for member in group.itertuples():
                for col in objekt_columns:
                    if element in getattr(member, col):
                        element_roles[col].add(member.Index)

            for member in group.itertuples():
                roles_per_member = {
                    member.Index: len(
                        [
                            col
                            for col in objekt_columns
                            if member.Index in element_roles[col]
                        ]
                    )
                    for member in group.itertuples()
                }
                if (
                    len(
                        [
                            role_count
                            for role_count in roles_per_member.values()
                            if role_count == 1
                        ]
                    )
                    == 3
                ):
                    # Check if member has a role for the given element
                    if any(
                        member.Index in element_roles[col] for col in objekt_columns
                    ):
                        new_row = {
                            col: getattr(member, col)
                            for col in df.columns
                            if col not in list_columns
                        }
                        new_row["cluster_id"] = (
                            f"{cluster_id}{suffix_dict[str(element)]}"
                        )

                        for col, id_col in zip(objekt_columns, produktid_columns):
                            if element in getattr(member, col):
                                index = getattr(member, col).index(element)
                                new_row[col] = element
                                new_row[id_col] = (
                                    getattr(member, id_col)[index]
                                    if index < len(getattr(member, id_col))
                                    else ""
                                )
                            else:
                                new_row[col] = ""
                                new_row[id_col] = ""

                        new_row_df = pd.DataFrame([new_row])
                        df_komplette_doubletten = pd.concat(
                            [df_komplette_doubletten, new_row_df], ignore_index=True
                        )

    df_list = []
    df_list_names = []
    if not df_komplette_doubletten.empty:
        df_list.append(df_komplette_doubletten)
        df_list_names.append("Komplette_Doubletten")

    return df_list, df_list_names


def reorder_format_produkte_columns(xx):
    """
    Creates 6 columns at the end of the dataframe, containing for each role the product name and its RefID.
    For split_produkte_groups_two_roles this is no longer needed.
    """

    def expand_row(row):
        # Finding the length of the longest list in the row among the specified list columns
        max_len = max(
            [len(row[col]) for col in list_columns if col in row] + [1]
        )  # Ensure at least length 1

        # Creating a dictionary to hold the expanded data
        expanded_data = {}

        # Expanding the list columns
        for col in list_columns:
            expanded_data[col] = (
                (row[col] + [""] * max_len)[:max_len] if col in row else [""] * max_len
            )

        # Expanding other columns by repeating their values
        for col in row.index:
            if col not in list_columns:
                expanded_data[col] = [row[col]] * max_len

        return pd.DataFrame(expanded_data)

    list_columns = [
        "Inhaber_Objekt",
        "Inhaber_ProduktID",
        "Rechempf_Objekt",
        "Rechempf_ProduktID",
        "Korrempf_Objekt",
        "Korrempf_ProduktID",
    ]
    for col in list_columns:
        xx[col] = xx[col].apply(lambda lst: [str(item) for item in lst])

    expanded_df = pd.concat([expand_row(row) for _, row in xx.iterrows()]).reset_index(
        drop=True
    )

    # Create an auxiliary column for sorting
    expanded_df["sort_key"] = expanded_df.apply(
        lambda x: x["Inhaber_Objekt"] or x["Rechempf_Objekt"] or x["Korrempf_Objekt"],
        axis=1,
    )

    # Sort by the new column and then by 'master'
    ordered_df = expanded_df.sort_values(
        by=["sort_key", "master"], ascending=[True, False]
    )
    ordered_df = ordered_df.replace({"nan": "", "None": "", pd.NA: ""})

    # Drop the auxiliary column
    ordered_df.drop("sort_key", axis=1, inplace=True)

    return ordered_df


# def organisationsrollen_filter_and_format(input_df, cluster_size=2, roles_per_product=3):
#     """
#     main function for organizing Organisationsrollenanalysen. Calls the functions above.
#     - Filter cluster size
#     - Organizes the clusters into several new dataframes (split_produkte_groups())
#     """
#     if cluster_size:
#         cluster_counts = input_df.groupby("cluster_id").size()
#         single_pair_clusters = cluster_counts[cluster_counts == cluster_size].index
#         output_df = input_df[input_df["cluster_id"].isin(single_pair_clusters)]
#     else:
#         output_df = input_df

#     if roles_per_product == 3:
#         if cluster_size == 2:
#             df_list, df_list_names = split_produkte_groups(output_df)
#         else:
#             df_list, df_list_names = split_produkte_groups_komplette_doublette(output_df)
#     elif roles_per_product == 2:
#         df_list, df_list_names = split_produkte_groups_two_roles(output_df)
#     else:
#         raise ValueError("roles_per_product must be 2 or 3")

#     if roles_per_product == 3 and cluster_size == 2:
#         df_list_reordered_columns = []
#         for df in df_list:
#             df_reordered = reorder_format_produkte_columns(df)
#             df_list_reordered_columns.append(df_reordered)
#     else:
#         df_list_reordered_columns = df_list

#     return df_list_reordered_columns, df_list_names


def organisationsrollen_filter_and_format(
    input_df, roles_per_product=3, rows_per_product=2
):
    """
    Update to commented-out version above: no longer need special reordering/format step.
    No longer accepts cluster_size parameter.
    main function for organizing Organisationsrollenanalysen. Calls the functions above.
    - Organizes the clusters into several new dataframes (split_produkte_groups())
    """
    if roles_per_product == 3:
        if rows_per_product == 2:
            df_list, df_list_names = split_produkte_groups(input_df)
        elif rows_per_product == 3:
            df_list, df_list_names = split_produkte_groups_komplette_doublette(input_df)
    elif roles_per_product == 2:
        df_list, df_list_names = split_produkte_groups_two_roles(input_df)
    else:
        raise ValueError("roles_per_product must be 2 or 3")

    return df_list, df_list_names


def organisationsrollen_filter_and_format_batch(
    df_dict, rows_per_product=2, roles_per_product=3
):
    """
    Processes any number of Produkte at once.
    Expects a dictionary, with the Produktname as key and the dataframe as value.
    Returns a dictionary with key = Produktname and value = list of dataframes (Inhaber_separat, etc.)
    """
    result_dict = {}
    for key, df in df_dict.items():
        df = set_master_flag(df)  # is needed for re-ordering
        dataframes, names = organisationsrollen_filter_and_format(
            df, rows_per_product=rows_per_product, roles_per_product=roles_per_product
        )

        # Check if dataframes and names lists are not empty and have the same length
        if dataframes and names and len(dataframes) == len(names):
            nested_dict = {
                names[i]: dataframe for i, dataframe in enumerate(dataframes)
            }
            result_dict[key] = nested_dict
        else:
            result_dict[key] = {}  # Or some other placeholder if no data is present

    return result_dict


# ---------  Filters for other analyses  ---------


def find_portal_vs_physisch_doublette(df, organisationen=False, strict_email=True):
    """
    Find doubletten (same name, adresse, email) that are present as physisch and portal
    (in case of personen irrespective of their connections to organisationen).
    If strict_email is True, require identical email. If False, relax this condition.
    """
    # Determine grouping columns based on strict_email and organisationen flags
    if organisationen:
        group_columns = ["Name_Zeile2", "address_full"]
    else:
        group_columns = ["Name", "address_full"]

    if strict_email:
        group_columns.append("EMailAdresse")

    # Group by specified columns and assign cluster_id
    df["cluster_id"] = df.groupby(group_columns).ngroup()

    # Apply different filtering logic based on strict_email
    if not strict_email:

        def filter_group(group):
            non_empty_emails = group[group["EMailAdresse"] != ""]["EMailAdresse"]
            if non_empty_emails.nunique() <= 1:
                return group
            else:
                return group[group["EMailAdresse"] == ""]

        df = df.groupby("cluster_id").apply(filter_group).reset_index(drop=True)

    # Keep only groups with at least 2 identical rows
    filtered_df = df[df.groupby("cluster_id")["cluster_id"].transform("size") > 1]

    # Check for at least one row with 'Versandart' == 'Portal' and at least one with 'Versandart' == 'Physisch' in each group
    valid_clusters = filtered_df.groupby("cluster_id").apply(
        lambda x: (x["Versandart"] == "Portal").any()
        and (x["Versandart"] == "Physisch").any()
    )

    # Filter out groups that don't meet both conditions
    final_df = filtered_df[
        filtered_df["cluster_id"].isin(valid_clusters[valid_clusters].index)
    ]

    return final_df


def find_email_doubletten(df, portal=True):
    """
    Simplified version of find_portal_vs_physisch_doublette().
    Doublette is simply defined by same email address.
    If portal=True, must have at least one member with Versandart == Portal, may or may not have others with Physisch.
    If portal=False, all must have Versandart == Physisch.
    They don't have to be connected to same organisation.
    """
    # We don't consider empty emails here
    df = df[df["EMailAdresse"] != ""]

    # Group by Email and assign cluster_id
    df["cluster_id"] = df.groupby("EMailAdresse").ngroup()

    # Keep only groups with at least 2 identical rows
    filtered_df = df[df.groupby("cluster_id")["cluster_id"].transform("size") > 1]

    # Check for at least one row with 'Versandart' == 'Portal' and at least one with 'Versandart' == 'Physisch' in each group
    if portal:
        valid_clusters = filtered_df.groupby("cluster_id").apply(
            lambda x: (x["Versandart"] == "Portal").any()
        )
        final_df = filtered_df[
            filtered_df["cluster_id"].isin(valid_clusters[valid_clusters].index)
        ]
    else:
        valid_clusters = filtered_df.groupby("cluster_id").apply(
            lambda x: (x["Versandart"] == "Physisch").all()
        )
        final_df = filtered_df[
            filtered_df["cluster_id"].isin(valid_clusters[valid_clusters].index)
        ]
    
    return final_df
