import os
import glob
import pandas as pd
import pickle


def get_most_recent_file(directory, pattern):
    """
    Helper function to get the most recent file matching a specific pattern.
    Returns a tuple of the file path and an error message.
    """
    # files = glob.glob(os.path.join(directory, pattern)) # ONLY data main folder
    files = glob.glob(
        os.path.join(directory, "**", pattern), recursive=True
    )  # data and all subfolders

    files = [
        f for f in files if "hyperlinks" not in f and not f.endswith("Zone.Identifier")
    ]
    files.sort(key=os.path.getmtime, reverse=True)

    if files:
        return files[0], False
    else:
        return None, f"{pattern}"


def load_data(file_name):
    if file_name.endswith(".xlsx"):
        return pd.read_excel(file_name)
    elif file_name.endswith(".csv"):
        return pd.read_csv(file_name)
    elif file_name.endswith(".pickle"):
        with open(file_name, "rb") as file:
            return pickle.load(file)
    else:
        raise ValueError("File not found or unsupported file format")


def load_processed_data(file_path="data/calculated/personen_organisationen_dfs_processed.pickle"):
    # data_dfs is a dictionary, with keys "personen" and "organisationen" providing the corresponding dataframes.
    try:
        with open(file_path, "rb") as file:
            data_dfs = pickle.load(file)
    except Exception as e:
        print("🚨 No data found or error in loading data: {e}")
    return data_dfs


def detect_raw_files(directory="data/"):
    """
    Expects query excel outputs in data/ and subfolders
    Returns the most recent files of different types as specified.
    Concatenates error messages if multiple files are missing.
    """

    # Dictionary to hold file patterns
    file_patterns = {
        "organisationen": "_EGov_Organisationen_Analyse*",
        "organisationsrollen": "_EGov_Organisationsrollenanalyse_MDG*",
        "organisationsrollenFDA": "_EGov_OrganisationsrollenanalyseFDA_MDG*",
        "organisationservicerolle": "_EGov_Organisationen_Servicerolle*",
        "personen": "_EGov_Personen_Analyse*",
        "personenservicerolle": "_EGov_Personen_Servicerolle*",
        "personenrollen": "_EGov_Personenrollenanalyse_MDG*",
        "Geschaeftspartern_Organisationen_BAFU": "*Geschaeftspartner*Organisationen*BAFU*",
        "Geschaeftspartern_Organisationen_BAZL": "*Geschaeftspartner*Organisationen*BAZL*",
        "Geschaeftspartern_Organisationen_BFE": "*Geschaeftspartner*Organisationen*BFE*",
        "Geschaeftspartern_Organisationen_ELCOM": "*Geschaeftspartner*Organisationen*ELCOM*",
        "Geschaeftspartern_Organisationen_POSTCOM": "*Geschaeftspartner*Organisationen*POSTCOM*",
        "Geschaeftspartern_Personen_BAFU": "*Geschaeftspartner*Personen*BAFU*",
        "Geschaeftspartern_Personen_BAZL": "*Geschaeftspartner*Personen*BAZL*",
        "Geschaeftspartern_Personen_BFE": "*Geschaeftspartner*Personen*BFE*",
        "Geschaeftspartern_Personen_ELCOM": "*Geschaeftspartner*Personen*ELCOM*",
        "Geschaeftspartern_Personen_POSTCOM": "*Geschaeftspartner*Personen*POSTCOM*",
    }

    # Initialize a list to collect error messages and a dict for the results
    error_messages = []
    result_files = {}

    # Loop through the dictionary to get the most recent files and collect errors
    for key, pattern in file_patterns.items():
        result, error = get_most_recent_file(directory, pattern=pattern)
        result_files[key] = result
        if error:
            error_messages.append(error)

    # Concatenate all error messages if any
    error_message = (
        "File missing: " + "; ".join(error_messages) if error_messages else False
    )

    return result_files, error_message


def save_results(data, filename):
    directory = "data/calculated"
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, filename), "wb") as file:
        pickle.dump(data, file)


def create_excel_files_from_nested_dict(nested_dict, output_dir="output"):
    """
    For output of Organisationsrollenanalyse:
    - top level keys are suffixes of the .xlsx files
    - keys of nested dicts are sheet names

    Error message "ValueError: seek of closed file" is expected and can be ignored
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file_suffix, sheet_dict in nested_dict.items():
        if any(not df.empty for df in sheet_dict.values()):
            file_name = f"{output_dir}/Organisationen_{file_suffix}.xlsx"
            with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
                for sheet_name, df in sheet_dict.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        print(
                            f"Empty dataframe for file '{file_suffix}', sheet '{sheet_name}'"
                        )
        else:
            print(
                f"No sheets created for file '{file_suffix}' as all dataframes are empty."
            )


def create_excel_file_from_dict(data_dict, output_file="output/Organisationen.xlsx"):
    """
    Creates a single Excel file with multiple sheets from a non-nested dict.
    Keys of the dict become sheet names.
    """
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, df in data_dict.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                print(f"Empty dataframe for sheet '{sheet_name}'")