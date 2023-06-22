import pandas


def cleanup_s4_hhcategory(df):
    # 7 responses are 'Used to be organic, but no longer'. There are also some blank responses. Both should be categorized as 'conventional'
    condition = (
        (df["s4_hhcategory"] == "")
        | (df["s4_hhcategory"] == "Used to be organic, but no longer")
        | (pandas.isna(df["s4_hhcategory"]))
    )

    # Print the count
    count = len(df[condition])
    print(f"Cleanup affecting {count} rows")

    # Do the replacement
    df.loc[condition, "s4_hhcategory"] = "S2S (organic in at least one plot)"
    return df


def merge_s4_cluster(df):
    # Collapse cluster names from below three rows into a single column
    df["s4_cluster"] = df["s4_cluster_ka"].fillna(
        df["s4_cluster_an"].fillna(df["s4_cluster_ku"])
    )

    # Drop old columns
    new_df = df.drop(["s4_cluster_ka", "s4_cluster_an", "s4_cluster_ku"], axis=1)
    return new_df


def merge_s4_gp(df):
    # Collapse gp names into a single column
    df["s4_gp"] = df["s4_gp_ka1"].fillna(
        df["s4_gp_ka2"].fillna(
            df["s4_gp_ka3"].fillna(df["s4_gp_an1"].fillna(df["s4_gp_ku1"]))
        )
    )

    # Drop old columns
    new_df = df.drop(
        ["s4_gp_ka1", "s4_gp_ka2", "s4_gp_ka3", "s4_gp_an1", "s4_gp_ku1"], axis=1
    )
    return new_df


def calculate_h1_income_total(df):
    raise NotImplementedError()
    return df


def update_h1_exp_food(df):
    # Divide by 7 and multiply by 30 to get monthly exp
    raise NotImplementedError()


def update_h1_exp_water_amt(df):
    # divide by 6
    raise NotImplementedError()


def update_h1_exp_healthcare_amt(df):
    # divide by 6
    raise NotImplementedError()


def update_h1_exp_phone_amt(df):
    # Phone amount needs to be monthly amount. Divide value by 2 if reported for
    # 2 months and 3 if reported for 3 months
    raise NotImplementedError()


def update_h1_exp_educ_amt(df):
    # divide by 6
    raise NotImplementedError()


def update_h1_exp_rituals_amt(df):
    # divide by 6
    raise NotImplementedError()


def add_exp_other_specify_to_h1_exp_substance_amt(df):
    # There is one 'other' reported as 'Beedi'. Please move this to h1_exp_substance_amt
    raise NotImplementedError()


def calculate_total_agwork(df):
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks
    raise NotImplementedError()


def calculate_total_otherwork(df):
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks
    raise NotImplementedError()


def calculate_total_work(df):
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks
    raise NotImplementedError()


def calculate_total_nonwork(df):
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks
    raise NotImplementedError()


def calculate_h3_ownland(df):
    # Generate total land in number of acres. 1 acre = 100 cents
    raise NotImplementedError()


def calculate_h3_farmsize(df):
    # Bin each farmer into categories: Marginal (<2.41 acres); Small (>2.41 but <4.94 acres); Medium (>4.94 but <9.88); Large (>9.88)
    raise NotImplementedError()


def calculate_h3_cultivateland(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents, 1 gunta = 2.5 cents
    raise NotImplementedError()


def calculate_h3_fullyorganic(df):
    # Note that these can't be created until you've calculated organic status for each plot individually!
    # Generate binary variable: fully organic if all plots of land reported by farmer are organic, not organic otherwise
    raise NotImplementedError()


def calculate_h3_fullyorganic2(df):
    # Generate categorical variable: fully organic if all plots of land reported by farmer are organic, partially organic if at least one plot is organic, not organic otherwise
    raise NotImplementedError()


def calculate_h3_plot1land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    raise NotImplementedError()


def calculate_h3_plot1organic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot1fert and h3_plot1pest are not using synthetic
    # fertilizers or pesticides (option 1), NPM if not using synthetic pesticides, and all other households
    # as 'Conventional'
    raise NotImplementedError()


def calculate_h3_plot2land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    raise NotImplementedError()


def calculate_h3_plot1organic_calc(df):
    # Generate categorical variable.'Organic' if h3_plot2fert and h3_plot2pest are not using synthetic fertilizers
    # or pesticides (option 1), NPM if not using synthetic pesticides, and all other households as 'Conventional'
    raise NotImplementedError()


def calculate_h3_plot2land(df):
    # Generate total cultivated land in number of acres. 1 acre = 100 cents
    raise NotImplementedError()


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


