import pandas as pd

def count_produktrollen_identische_sonstige(df, statistik_df):
    """
    Generiert Tabelle, die für jeden Produkt-typ anzeigt wieviele Produkte in allen drei Rollen identische 
    Inhaber, Rechnungsempfänger und Korrespondenzempfänger haben.
    Wie viele davon Doubletten beinhalten (d.h. mindestens zwei Rollen haben unterschiedliche IDs, sind aber doubletten)
    
    Benötigt organisationsrollen_df mit zusätzlichen Infos zu den Produkten (Produkt_Typ, Produkt_Name)
    statistik_df wird in der Organisationsrollenanalyse generiert.
    """
    results = []

    for produkt_name in df["Produkt_Name"].unique():
        subset = df[(df["Produkt_Name"] == produkt_name) & 
                    (df["Inhaber_Typ"] == "Organisation") & 
                    (df["Rechnungsempfaenger_Typ"] == "Organisation") & 
                    (df["Korrespondenzempfaenger_Typ"] == "Organisation")]

        identical_count = subset[
            subset.apply(
                lambda row: row["Inhaber_RefID"] == row["Rechnungsempfaenger_RefID"] == row["Korrespondenzempfaenger_RefID"],
                axis=1
            )
        ].shape[0]

        total_count = subset.shape[0]
        doubletten_row = statistik_df.loc[statistik_df["produkte"] == produkt_name, "Doubletten"]
        doubletten_count = doubletten_row.values[0] if not doubletten_row.empty else 0
        sonstige_count = total_count - identical_count - doubletten_count

        results.append({
            "Produkt_Name": produkt_name,
            "Identisch": identical_count,
            "Doubletten": doubletten_count,
            "Sonstige": sonstige_count,
            "Total": total_count,
        })
        
    results_df = pd.DataFrame(results)
        
        # Calculate sums for each Produkt_Typ
    produkt_typ_sums = df.groupby("Produkt_Typ").apply(
        lambda x: pd.Series({
            "Identisch": results_df.loc[results_df["Produkt_Name"].isin(x["Produkt_Name"]), "Identisch"].sum(),
            "Doubletten": results_df.loc[results_df["Produkt_Name"].isin(x["Produkt_Name"]), "Doubletten"].sum(),
            "Sonstige": results_df.loc[results_df["Produkt_Name"].isin(x["Produkt_Name"]), "Sonstige"].sum(),
            "Total": results_df.loc[results_df["Produkt_Name"].isin(x["Produkt_Name"]), "Total"].sum(),
        })
    ).reset_index()

    # Add a column for Produkt_Name with the value "Sum for Produkt_Typ"
    produkt_typ_sums["Produkt_Name"] = produkt_typ_sums["Produkt_Typ"] + " Summe"

    # Reorder columns to match results_df
    produkt_typ_sums = produkt_typ_sums[["Produkt_Name", "Identisch", "Doubletten", "Sonstige", "Total"]]

    # Concatenate the sums at the top of the results DataFrame
    final_df = pd.concat([produkt_typ_sums, results_df], ignore_index=True)

    return final_df