

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
