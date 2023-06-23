import numpy as np
import pandas as pd


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
    raise NotImplementedError()


def calculate_h3_fullyorganic(df):
    # Note that these can't be created until you've calculated organic status for each plot individually!
    # Generate binary variable: fully organic if all plots of land reported by farmer are organic, not organic otherwise

    # Implement all dependent cleanup here

    raise NotImplementedError()


def calculate_h3_fullyorganic2(df):
    # Generate categorical variable: fully organic if all plots of land reported by farmer are organic, partially organic if at least one plot is organic, not organic otherwise
    raise NotImplementedError()


def calculate_h3_plot1land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot1land"] = (
        df["h3_plot1acre"].fillna(0)
        + df["h3_plot1cent"].mul(0.01, fill_value=0)
        + df["h3_plot1guntas"].mul(0.025, fill_value=0)
    )
    return df


def calculate_h3_plot1organic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot1fert and h3_plot1pest are not using synthetic
    # fertilizers or pesticides (option 1), NPM if not using synthetic pesticides, and all other households
    # as 'Conventional'
    raise NotImplementedError()


def calculate_h3_plot2land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot2land"] = (
        df["h3_plot2acre"].fillna(0)
        + df["h3_plot2cent"].mul(0.01, fill_value=0)
        + df["h3_plot2guntas"].mul(0.025, fill_value=0)
    )
    return df


def calculate_h3_plot1organic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot2fert and h3_plot2pest are not using synthetic fertilizers
    # or pesticides (option 1), NPM if not using synthetic pesticides, and all other households as 'Conventional'
    raise NotImplementedError()


def calculate_h3_plot3land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    df["h3_plot3land"] = (
        df["h3_plot3acre"].fillna(0)
        + df["h3_plot3cent"].mul(0.01, fill_value=0)
        + df["h3_plot3guntas"].mul(0.025, fill_value=0)
    )
    return df


def calculate_h3_plot3organic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot3fert and h3_plot3pest are not using synthetic fertilizers or pesticides (option 1), NPM if not using synthetic pesticides, and all other households as 'Conventional'
    raise NotImplementedError()


def calcualte_h3_costofcult(df):
    # sum of all cost variables below, including h3_otherorganic and h3_madecost, and seed cost for all five crops
    raise NotImplementedError()


def calculate_h3_seedcosttotal(df):
    # sum of all seed costs (five columns, one for each crop): h3_cropxseedcost
    raise NotImplementedError()


def calculate_h3_seedtreattotal(df):
    # sum of all seed treatment costs (five columns, one for each crop): h3_cropxseedtreatcost
    raise NotImplementedError()
