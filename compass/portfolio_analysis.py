import pandas as pd
import pycountry
import pycountry_convert as pc
from functools import reduce
from utils.utils import remove_stop_words

def merge_string_with_nulls(x, y):
    resp = ""
    if x and not y:
        resp = x
    elif y and not x:
        resp = y
    elif x and y:
        resp = ",".join([x, y]) if y not in x else x
    return resp
    

def merge_string(series):
    if series.empty:
        return ""
    else:
        return reduce(merge_string_with_nulls, series)


def apply_country_code(row):
    country = None
    if row:
        continents = {
            "AS": "Asia",
            "EU": "Europe",
            "NA": "North America",
            "SA": "South America",
            "AF": "Africa",
            "OC": "Oceania",
        }
        a2= row[:2]
        country = pycountry.countries.get(alpha_2=a2)
    name = country.name if country else ""
    code_2 = country.alpha_2 if country else "" 
    code_3 = country.alpha_3 if country else "" 
    continent_code = pc.country_alpha2_to_continent_code(code_2) if code_2 else ""
    continent = continents[continent_code] if continent_code else ""
    return pd.Series([name, continent, code_2, code_3])
    

def get_consolidated_securities(raw_holdings, only_imp):
    raw_df = pd.DataFrame.from_dict(raw_holdings)
    # adding a step to skip the mf securities with 0 units
    raw_df = raw_df[~((raw_df['units'] == 0) & (raw_df['isin'].str.startswith('INF')))]
    raw_df["instrument_type"] = raw_df["instrument_type"].replace("", "Unknown")
    raw_df["asset_class"] = raw_df["asset_class"].replace("", "Unknown")
    raw_df["sector"] = raw_df["sector"].replace("", "Unknown")
    raw_df["issuer"] = raw_df["issuer"].replace("", "Unknown")

    # cleaning data for the issuer information
    raw_df["issuer"] = raw_df["issuer"].str.upper()
    raw_df["issuer"] = raw_df["issuer"].str.replace('[^a-zA-Z0-9\s]', '')
    stop_words  = ['LTD', 'PVT']
    raw_df["issuer"] = raw_df.apply(lambda x: remove_stop_words(x["issuer"], stop_words), axis=1)
    raw_df["issuer"] = raw_df["issuer"].str.strip()

    # for unlisted the sub-sector is empty and hence the data wrangling is required
    raw_df["sub_sector"] = raw_df["sub_sector"].replace("", "Unknown")
    # for unlisted the account alias is empty and hence the data wrangling is required
    raw_df["account_alias"] = raw_df["account_alias"].fillna('').replace("", "Not Applicable")

    exposure = raw_df.groupby(['name'], dropna=False, as_index=False)\
                     .agg(
                            isin = ("isin", "first"),
                            name = ("name", "first"), 
                            total_price = ("total_price", "sum"),
                            units = ("units", "sum"),
                            instrument_type = ("instrument_type", "first"),     # TODO: Update this to Holding Security Type
                            asset_class = ("asset_class", "first"), 
                            issuer = ("issuer", "first"), 
                            sector = ("sector", "first"), 
                            sub_sector = ("sub_sector", "first"), 
                            market_cap = ("market_cap", "first"), 
                            equity_style = ("equity_style", "first"), 
                            risk_category = ("risk_category", "first"), 
                            account_alias = ("account_alias", merge_string), 
                            unit_price = ("unit_price" , "first"),
                            coupon_rate = ("coupon_rate", "first"),
                            maturity = ("maturity", "first"),
                        )


    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)
    if only_imp:
        exposure= exposure.drop(columns=["issuer", "sector", "total_price", "units", "instrument_type","sub_sector", "market_cap",
                                         "equity_style", "asset_class", "risk_category", "account_alias", "unit_price", "coupon_rate","maturity"])
    return exposure


def get_account_level_security_consolidation(raw_holdings):
    raw_df = pd.DataFrame.from_dict(raw_holdings)
    accounts = list(raw_df["account_alias"].unique())
    final = pd.DataFrame()

    for account_nr in accounts:
        partial = raw_df[raw_df.account_alias==account_nr]
        exposure = partial.groupby(['name'], dropna=False, as_index=False).agg(
            isin = ("isin", "first"),
            name = ("name", "first"), 
            total_price = ("total_price", "sum"),
            units = ("units", "sum"),
            instrument_type = ("instrument_type", "first"), 
            asset_class = ("asset_class", "first"), 
            issuer = ("issuer", "first"), 
            sector = ("sector", "first"), 
            sub_sector = ("sub_sector", "first"), 
            market_cap = ("market_cap", "first"), 
            equity_style = ("equity_style", "first"), 
            risk_category = ("risk_category", "first"), 
            account_alias = ("account_alias", "first"), 
            unit_price = ("unit_price" , "first"),
            coupon_rate = ("coupon_rate", "first"),
            maturity = ("maturity", "first"), 
        )

        if final.empty:
            final = exposure
        else:
            final = pd.concat([final, exposure])

    weight_col = (final["total_price"] / final["total_price"].sum())*100
    final.insert(3, column="weight", value=weight_col)
    final.sort_values(by="weight", ascending=False, inplace=True)

    return final

def get_issuer_exposure(consolidated_securities, only_imp):
    exposure = consolidated_securities.groupby(["issuer"], dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        # issuer = ("issuer", merge_string), 
        sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string), 
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "total_price", 
            "isin",
            "name",
            "instrument_type",
            "asset_class",
            # "issuer",
            "sector", 
            "sub_sector",
            "account_alias",
        ])
    return exposure


def get_market_cap_exposure(consolidated_securities, only_imp):
    equities = consolidated_securities.loc[
        (consolidated_securities.instrument_type == "Equity") | (consolidated_securities.asset_class == "Equity")
        ]

    exposure = equities.groupby(["market_cap"], dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        # issuer = ("issuer", merge_string), 
        sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string), 
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "total_price", 
            "isin",
            "name",
            "instrument_type",
            "asset_class",
            # "issuer",
            "sector", 
            "sub_sector",
            "account_alias",
        ])
    return exposure


def get_equity_style_exposure(consolidated_securities, only_imp):
    equities = consolidated_securities.loc[
        (consolidated_securities.instrument_type == "Equity") | (consolidated_securities.asset_class == "Equity")
        ]

    exposure = equities.groupby(["equity_style"], dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        # issuer = ("issuer", merge_string), 
        sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string), 
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "total_price", 
            "isin",
            "name",
            "instrument_type",
            "asset_class",
            # "issuer",
            "sector", 
            "sub_sector",
            "account_alias",
        ])
    return exposure


def get_investment_style_exposure(consolidated_securities, only_imp):
    equities = consolidated_securities.loc[
        (consolidated_securities.instrument_type == "Equity") | (consolidated_securities.asset_class == "Equity")
        ]

    exposure = equities.groupby(["equity_style", "market_cap"], dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        equity_style = ("equity_style", merge_string),
        market_cap = ("market_cap", merge_string)
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "weight", 
            "equity_style",
            "market_cap"
        ])
    return exposure


# TODO: Instrument type and Asset Class will be subcategory
def get_instrument_type_exposure(consolidated_securities, only_imp):
    exposure = consolidated_securities.groupby(['instrument_type'], dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        # instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        issuer = ("issuer", merge_string), 
        sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string), 
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure["instrument_type"]
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "total_price", 
            "isin",
            "name",
            # "instrument_type",
            "asset_class",
            "issuer",
            "sector", 
            "sub_sector",
            "account_alias",
        ])
        
    return exposure


# TODO: There is a problem in sector and sub sector as we are using VR and CMOTS data. Need to fix it.
def get_sector_exposure(consolidated_securities, only_imp, instrument_type_break_down):
    
    grp_by_cols = ['instrument_type', 'sector'] if instrument_type_break_down else ['sector']
    exposure = consolidated_securities.groupby(grp_by_cols, dropna=False, as_index=False).agg(
        security_count = pd.NamedAgg(column="isin", aggfunc="count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        issuer = ("issuer", merge_string), 
        # sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string), 
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)
    
    if only_imp:
        exposure= exposure.drop(columns=[
            "security_count",
            "total_price", 
            "isin",
            "name",
            "instrument_type",
            "asset_class",
            "issuer",
            # "sector", 
            "sub_sector",
            "account_alias",
        ])

    if instrument_type_break_down:
        exposure.set_index(grp_by_cols, drop=True, inplace=True)

    return exposure


def get_location_exposure(consolidated_securities, only_imp):
    consolidated_securities[["country", "continent", "iso_a2", "iso_a3"]] = consolidated_securities["isin"].apply(apply_country_code)
    exposure = consolidated_securities.groupby(['continent'], dropna=False, as_index=False).agg(
        country = ("country", merge_string),
        iso_a2 = ("iso_a2", merge_string),
        iso_a3 = ("iso_a3", merge_string), 
        security_count = ("isin", "count"),
        total_price = ("total_price", "sum"),
        isin = ("isin", merge_string),
        name = ("name", merge_string), 
        instrument_type = ("instrument_type", merge_string), 
        asset_class = ("asset_class", merge_string), 
        issuer = ("issuer", merge_string), 
        sector = ("sector", merge_string), 
        sub_sector = ("sub_sector", merge_string), 
        account_alias = ("account_alias", merge_string),         
    )
    weight_col = (exposure["total_price"] / exposure["total_price"].sum())*100
    exposure.insert(3, column="weight", value=weight_col)
    exposure = exposure.sort_values(by="weight", ascending=False)

    if only_imp:
        exposure= exposure.drop(columns=[
            "country",
            "iso_a2",
            "iso_a3",
            "security_count",
            "total_price", 
            "isin",
            "name",
            "instrument_type",
            "asset_class",
            "issuer",
            "sector", 
            "sub_sector",
            "account_alias",
        ])

    return exposure


def analyze_portfolio_movement(old_holdings_df, new_holdings_df):
    cols = ["issuer", "sector", "sub_sector", "market_cap", "equity_style", "risk_category", "coupon_rate", "maturity",
            "units", "unit_price", "instrument_type", "asset_class"]
    old_df = old_holdings_df.drop(cols, axis=1)
    new_df = new_holdings_df.drop(cols, axis=1)
    
    final = pd.merge(new_df, old_df, how="outer", on=["isin", "name", "account_alias"], suffixes=('', '_P'),)
    final["weight_diff"] = final["weight"] - final["weight_P"]
    
    increase_df = final.loc[(final["weight"] > 0) & (final["weight_P"] > 0) & (final["weight_diff"] > 0)]
    increase_df.insert(loc=2, column='Holding_Type', value="Increase_Exposure")

    decrease_df = final.loc[(final["weight"] > 0) & (final["weight_P"] > 0) & (final["weight_diff"] < 0)]
    decrease_df.insert(loc=2, column='Holding_Type', value="Decrease_Exposure")

    entry_df = final.loc[(final["weight"] > 0 ) & (final["weight_P"] == 0) & (final["weight_diff"] > 0)]
    entry_df.insert(loc=2, column='Holding_Type', value="New_Entrants")

    exit_df = final.loc[(final["weight"] == 0) & (final["weight_P"] > 0) & (final["weight_diff"] < 0)]
    exit_df.insert(loc=2, column='Holding_Type', value="Complete_Exit")

    if not final.empty:    
        portfolio_movement_df = pd.concat([increase_df, decrease_df, entry_df, exit_df])
    return portfolio_movement_df   