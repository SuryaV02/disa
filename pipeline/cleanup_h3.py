import numpy as np
import pandas as pd

from pipeline.utils import purge_outliers


def purge_h3_outliers(df):
    mask = df["h3_pesticide"] > 200000
    filtered_df = df[~mask]
    dropped_ids = df[mask]["hhid"].unique()
    for d_id in dropped_ids:
        print(f"Dropped HHID : {d_id}")
    return filtered_df


def calculate_h3_ownland(df):
    # First cleanup the individual land data
    updated_df = calculate_h3_plot1land(df)
    updated_df = calculate_h3_plot2land(updated_df)
    updated_df = calculate_h3_plot3land(updated_df)

    # Now compute the total land
    # 0's will fill in the blank records
    updated_df["h3_ownland"] = (
        df["h3_plot1land"] + df["h3_plot2land"] + df["h3_plot3land"]
    )

    return updated_df


def calculate_h3_farmsize(df):
    # We must calculate h3_ownland before calculating this
    df = calculate_h3_ownland(df)

    # Bin each farmer into categories: Marginal (<2.41 acres); Small (>2.41 but <4.94 acres); Medium (>4.94 but <9.88); Large (>9.88)
    bins = [0, 2.41, 4.94, 9.88, np.inf]
    labels = ["Marginal", "Small", "Medium", "Large"]
    df["h3_farmsize"] = pd.cut(df["h3_ownland"], bins=bins, labels=labels)
    return df


def calculate_h3_cultivateland(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents, 1 gunta = 2.5 cents
    df["h3_cultivateland"] = (
        df["h3_cultivateacre"].fillna(0)
        + df["h3_cultivatecent"].mul(0.01, fill_value=0)
        + df["h3_cultivateguntas"].mul(0.025, fill_value=0)
    )
    return df


def classify_h3_fullyorganic(df):
    # Note that these can't be created until you've calculated organic status for each plot individually!
    # Generate binary variable: fully organic if all plots of land reported by farmer are organic, not organic otherwise

    # Implement all dependent cleanup here
    df = calculate_h3_plotNorganic_calc(df)
    df.loc[
        (
            (df["h3_plot1organic_calc"] == "Organic")
            | (pd.isna(df["h3_plot1organic_calc"]))
        )
        & (
            (df["h3_plot2organic_calc"] == "Organic")
            | (pd.isna(df["h3_plot2organic_calc"].isna()))
        )
        & (
            (df["h3_plot3organic_calc"] == "Organic")
            | (pd.isna(df["h3_plot3organic_calc"].isna()))
        ),
        "h3_fullyorganic",
    ] = True
    return df


def calculate_h3_plot1land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot1land"] = (
        df["h3_plot1acre"].fillna(0)
        + df["h3_plot1cent"].mul(0.01, fill_value=0)
        + df["h3_plot1guntas"].mul(0.025, fill_value=0)
    )
    return df


def calculate_h3_plot2land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot2land"] = (
        df["h3_plot2acre"].fillna(0)
        + df["h3_plot2cent"].mul(0.01, fill_value=0)
        + df["h3_plot2guntas"].mul(0.025, fill_value=0)
    )
    return df


def calculate_h3_plotNorganic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot2fert and h3_plot2pest are not using synthetic fertilizers
    # or pesticides (option 1), NPM if not using synthetic pesticides, and all other households as 'Conventional'
    # TODO: Refine implementation, currently checking if only the main option is checked or not (h3_plot1fert___1)
    def classify_plotN(row, plot_number):
        synthetic_fert_column_name = f"h3_plot{plot_number}fert___1"
        classification_column_name = f"h3_plot{plot_number}organic_calc"
        fert_other_column_name = f"h3_plot{plot_number}fert_other"
        pesticide_column_name = f"h3_plot{plot_number}pest___1"
        # TODO: Unsure if the current logic accounts for the other category correctly
        if (
            (
                row[synthetic_fert_column_name] == 0
                or row[synthetic_fert_column_name] == "Unchecked"
            )
            and (
                row[fert_other_column_name] != "Checked"
                and row[fert_other_column_name] != 1
            )
            and (
                row[pesticide_column_name] != "Checked"
                or row[pesticide_column_name] != 1
            )
        ):  # Adding the extra condition incase its labeled data
            row[classification_column_name] = "Organic"
        elif (
            row[pesticide_column_name] != "Checked" or row[pesticide_column_name] != 1
        ):  # Don't care logic for fertilizer
            row[classification_column_name] = "NPM"
        else:
            row[classification_column_name] = "Conventional"

        return row

    df = df.apply(classify_plotN, plot_number=1, axis=1)
    df = df.apply(classify_plotN, plot_number=2, axis=1)
    df = df.apply(classify_plotN, plot_number=3, axis=1)

    return df


def calculate_h3_plot3land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot3land"] = (
        df["h3_plot3acre"].fillna(0)
        + df["h3_plot3cent"].mul(0.01, fill_value=0)
        + df["h3_plot3guntas"].mul(0.025, fill_value=0)
    )
    return df


def calcualte_h3_costofcult(df):
    # sum of all cost variables below, including h3_otherorganic and h3_madecost, and seed cost for all five crops
    """
    h3_soil
    h3_fertilizer
    h3_manure
    h3_pesticide
    h3_jeevamrutam
    h3_biocides
    h3_mechanicalpest
    h3_diesel
    h3_electricity
    h3_mulchmaterial
    h3_hlabour_mulch
    h3_hlabour_plough
    h3_hlabour_sow
    h3_hlabour_weed
    h3_hlabour_prune
    h3_hlabour_spray
    h3_hlabour_harvest
    h3_labor
    h3_irrigation
    h3_maintenance
    h3_machinery
    h3_lease
    """
    # Calculate total seed and seed treat costs first
    df = calculate_h3_seedcosttotal(df)
    df = calculate_h3_seedtreattotal(df)

    df["h3_costofcult"] = df[
        [
            "h3_soil",
            "h3_fertilizer",
            "h3_manure",
            "h3_pesticide",
            "h3_jeevamrutam",
            "h3_biocides",
            "h3_mechanicalpest",
            "h3_diesel",
            "h3_electricity",
            "h3_mulchmaterial",
            "h3_hlabour_mulch",
            "h3_hlabour_plough",
            "h3_hlabour_sow",
            "h3_hlabour_weed",
            "h3_hlabour_prune",
            "h3_hlabour_spray",
            "h3_hlabour_harvest",
            # "h3_labor",
            "h3_irrigation",
            "h3_maintenance",
            "h3_machinery",
            "h3_lease",
            "h3_otherorganic",
            "h3_madecost",
            "h3_seedcosttotal",
            "h3_seedtreattotalcost",
        ]
    ].sum(axis=1, skipna=True)
    return df


def calculate_h3_seedcosttotal(df):
    # sum of all seed costs (five columns, one for each crop): h3_cropxseedcost
    # TODO: Replace with regex later
    df["h3_seedcosttotal"] = (
        df["h3_crop1seedcost"].fillna(0)
        + df["h3_crop2seedcost"].fillna(0)
        + df["h3_crop3seedcost"].fillna(0)
        + df["h3_crop4seedcost"].fillna(0)
        + df["h3_crop5seedcost"].fillna(0)
    )
    return df


def calculate_h3_seedtreattotal(df):
    # sum of all seed treatment costs (five columns, one for each crop): h3_cropxseedtreatcost
    # TODO: Replace with regex later
    df["h3_seedtreattotalcost"] = (
        df["h3_crop1seedtreatcost"].fillna(0)
        + df["h3_crop2seedtreatcost"].fillna(0)
        + df["h3_crop3seedtreatcost"].fillna(0)
        + df["h3_crop4seedtreatcost"].fillna(0)
        + df["h3_crop5seedtreatcost"].fillna(0)
    )

    return df


def calculate_h3_cropNland(df):
    # TODO: Make this a regex version for allowing N crops: h3_crop1_acre

    df["h3_crop1land"] = (
        df["h3_crop1_acre"].fillna(0)
        + df["h3_crop1_cents"].mul(0.01, fill_value=0)
        + df["h3_crop1_guntas"].mul(0.025, fill_value=0)
    )

    df["h3_crop2land"] = (
        df["h3_crop2_acre"].fillna(0)
        + df["h3_crop2_cents"].mul(0.01, fill_value=0)
        + df["h3_crop2_guntas"].mul(0.025, fill_value=0)
    )

    df["h3_crop3land"] = (
        df["h3_crop3_acre"].fillna(0)
        + df["h3_crop3_cents"].mul(0.01, fill_value=0)
        + df["h3_crop3_guntas"].mul(0.025, fill_value=0)
    )

    df["h3_crop4land"] = (
        df["h3_crop4_acre"].fillna(0)
        + df["h3_crop4_cents"].mul(0.01, fill_value=0)
        + df["h3_crop4_guntas"].mul(0.025, fill_value=0)
    )

    df["h3_crop5land"] = (
        df["h3_crop5_acre"].fillna(0)
        + df["h3_crop5_cents"].mul(0.01, fill_value=0)
        + df["h3_crop5_guntas"].mul(0.025, fill_value=0)
    )

    return df
