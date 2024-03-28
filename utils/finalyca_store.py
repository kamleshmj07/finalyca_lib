import sqlalchemy
from fin_models.masters_models import Base
from fin_models.controller_master_models import ControllerBase, DOFacilitator, UserType
from fin_models.masters_models import *
from fin_models.transaction_models import *
from fin_models.servicemanager_models import ServiceManagerBase
from fin_resource import DataStore, FieldChoices, ResourceType, FieldType, add_sqlalchemy_model
from utils import *

def get_finalyca_scoped_session(is_production, scope_func=None):
    pms_base_engine = sqlalchemy.engine.create_engine( mssql_prod_uri(is_production, "PMS_Base") )
    pms_controller_engine = sqlalchemy.engine.create_engine( mssql_prod_uri(is_production, "PMS_Controller") )
    sm_engine = sqlalchemy.engine.create_engine( mssql_prod_uri(is_production, "ServiceManager") )

    session_factory = sqlalchemy.orm.sessionmaker( binds={ 
        Base: pms_base_engine, ControllerBase: pms_controller_engine, ServiceManagerBase: sm_engine
        }, autocommit=False, autoflush=False)

    return sqlalchemy.orm.scoped_session(session_factory, scopefunc=scope_func)


def is_production_config(config):
    if "deploy" in config and config["deploy"] == "prod":
        is_production = True
    else:
        is_production = False

    return is_production

def get_data_store(config, context_func= None):
    is_production = is_production_config(config)

    return get_finalyca_store(is_production, context_func)

def get_finalyca_store(is_production, context_func):
    db_session = get_finalyca_scoped_session(is_production, context_func)

    store = DataStore(db_session)
    configure_data_store(store)

    return store

def configure_data_store(datastore: DataStore):
    add_sqlalchemy_model(datastore, UserType, {})
    add_sqlalchemy_model(datastore, ProductCategory, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    add_sqlalchemy_model(datastore, ProductType, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    add_sqlalchemy_model(datastore, Product, {
        "ProductCategory_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
        "ProductType_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
        "AssetClass_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AssetClass") },
        "Issuer_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Issuer") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    
    add_sqlalchemy_model(datastore, RiskFreeIndexRate, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS},
    } )

    add_sqlalchemy_model(datastore, RiskRating, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS},
    } )

    add_sqlalchemy_model(datastore, Issuer, {
        "IssuerGroup_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("IssuerGroup") },
        "IssuerCategory_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("IssuerCategory") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    add_sqlalchemy_model(datastore, IssuerCategory, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    add_sqlalchemy_model(datastore, IssuerGroup, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, FundManager, 
    {
        "Fund_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Fund") },
        "FundManager_Image" : {"type": FieldType.FILE},
        "AMC_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AMC") },
        "Product_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
    })

    add_sqlalchemy_model(datastore, Classification, {
        "AssetClass_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AssetClass") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, Fund, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, HoldingSecurity, {
        "Sector_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Sector") },
        "Issuer_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Issuer") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, Sector, {} )
    # add_sqlalchemy_model(datastore, ContentUploadType, {} )

    add_sqlalchemy_model(datastore, ContentUploadType, {} )
    add_sqlalchemy_model(datastore, ContentUpload, {
        "AMC_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AMC") },
    } )
    add_sqlalchemy_model(datastore, ContentCategory, {} )
    add_sqlalchemy_model(datastore, ContentType, {} )
    add_sqlalchemy_model(datastore, Content, {
        "Content_Category_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Content_Category") },
        "Content_Type_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Content_Type") },
        "AMC_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AMC") },
        "Product_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
        "Fund_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Fund") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, SecurityCategory, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, MarketCap, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, Country, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )

    add_sqlalchemy_model(datastore, Currency, {
        "Country_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Country") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )   

    add_sqlalchemy_model(datastore, AssetClass, {
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    
    add_sqlalchemy_model(datastore, PlanType, { } )
    add_sqlalchemy_model(datastore, Options, { } )
    add_sqlalchemy_model(datastore, SwitchAllowed, { } )

    add_sqlalchemy_model(datastore, Plans, { 
        "PlanType_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("PlanType") },
        "MF_Security_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("MF_Security") },
        "Option_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Options") },
        "SwitchAllowed_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("SwitchAllowed") },
    } )
    add_sqlalchemy_model(datastore, PlanProductMapping, { 
        "Plan_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Plans") },
        "Product_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    } )
    add_sqlalchemy_model(datastore, FactSheet, { } )
    add_sqlalchemy_model(datastore, Status, { } )
    add_sqlalchemy_model(datastore, BenchmarkIndices, { } )
    add_sqlalchemy_model(datastore, FundType, { } )

    
    add_sqlalchemy_model(datastore, AMC, 
    {
        "Product_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product"), "is_required": True},
        "AMC_Logo" : {"type": FieldType.FILE},
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    })

    add_sqlalchemy_model(datastore, FundStocks, 
    {
        "Product_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Product") },
        "Plan_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Plans") },
        "Fund_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Fund") },        
        "HoldingSecurity_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("HoldingSecurity") },        
        "Classification_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Classification") },        
    })

    add_sqlalchemy_model(datastore, MFSecurity, 
    {
        "Fund_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Fund") },
        "AssetClass_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AssetClass") },
        "Status_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Status") },
        "FundType_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("FundType") },
        "BenchmarkIndices_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("BenchmarkIndices") },
        "AMC_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("AMC") },
        "Classification_Id" : {"type": FieldType.REF, "options" : FieldChoices.from_model("Classification") },
        "Created_By" : {"type": FieldType.CURRENT_USER },
        "Created_Date" : {"type": FieldType.CURRENT_TS, "is_mutable": False},
        "Updated_By" : {"type": FieldType.CURRENT_USER },
        "Updated_Date" : {"type": FieldType.CURRENT_TS },
    })

    add_sqlalchemy_model(datastore, DOFacilitator, 
    {
        "created_by" : {"type": FieldType.CURRENT_USER },
        "created_at" : {"type": FieldType.CURRENT_TS },
        "edited_by" : {"type": FieldType.CURRENT_USER },
        "edited_at" : {"type": FieldType.CURRENT_TS },
    })

    add_sqlalchemy_model(datastore, FundScreener, 
    {
        "amc" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("amc"), 'label': "AMC" },        
        "product" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("product") },
        "classification_name" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("classification_name") },        
        "asset_class" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("asset_class") },        
        # "plan_name" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("plan_name") },
        "fund" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("fund") },
        "aum" : {"label": "AUM in Cr" },
        "large_cap" : {"label": "Large Cap (%)" },
        "mid_cap" : {"label": "Mid Cap (%)" },
        "small_cap" : {"label": "Small Cap (%)" },
        "fund_age_in_months" : {"label": "Fund Age In Months" },
        "pb_ratio" : {"type": FieldType.DECIMAL, "label" : "PB Ratio"},
        "pe_ratio" : {"type": FieldType.DECIMAL, "label" : "PE Ratio" },
        "transaction_date" : {"type": FieldType.DATE, "label" : "Factsheet Date" },
    },
    resource_type= ResourceType.view
    )

    add_sqlalchemy_model(datastore, DebtScreener,
    {
        "security_name" : {"label": "Security Name" },
        "isin" : {"label": "ISIN" },
        "security_type" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("security_type"), 'label': "Security Type"},
        "bond_type" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("bond_type"), 'label': "Instrument Type"},
        "country" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("country"), 'label': "Country"},
        "issuer" : {"type": FieldType.TEXT, "options" : FieldChoices.from_view_column("issuer"), 'label': "Issuer Name"},
        "is_perpetual" : {"label": "Is Perpetual"},
        "on_tap_indicator" : {"label": "On Tap Indicator"},
        "coupon_type" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("coupon_type"), 'label': "Coupon Type"},
        "interest_payment_frequency" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("interest_payment_frequency"), 'label': "Interest Payment Frequency"},
        "is_cumulative" : {"label": "Is Cumulative"},
        "issuer_type" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("issuer_type"), 'label': "Issuer Type"},
        "maturity_structure" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("maturity_structure"), 'label': "Maturity Structure"},
        "is_senior" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("is_senior"), 'label': "Senority"},
        "sector_name" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("sector_name"), 'label': "Sector"},
        "currency" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("currency"), 'label': "Currency"},
        "crisil" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("crisil"), "label": "Crisil Rating"},
        "care" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("care"), "label": "Care Rating"},
        "fitch" : {"type": FieldType.REF, "options" : FieldChoices.from_view_column("fitch"), "label": "Fitch Rating"},
        "icra" :  {"type": FieldType.REF, "options" : FieldChoices.from_view_column("icra"), "label": "ICRA Rating"},
        "brickwork" :  {"type": FieldType.REF, "options" : FieldChoices.from_view_column("brickwork"), "label": "Brickwork Rating"},
        "sovereign" :  {"type": FieldType.REF, "options" : FieldChoices.from_view_column("sovereign"), "label": "Sovereign Rating"},
        "acuite" :  {"type": FieldType.REF, "options" : FieldChoices.from_view_column("acuite"), "label": "Acuite Research Rating"},
        "markup" : {"type": FieldType.DECIMAL, "label" : "Markup"},
        "is_guaranteed" : {"label": "Is Guaranteed"},
        "is_secured" : {"label": "Is Secured"},
        "issue_size" : {"type": FieldType.DECIMAL, "label" : "Issue Size (In Cr.)"},
        "yield_at_issue" : {"type": FieldType.DECIMAL, "label" : "Yield At Issue"},
        "min_investment_amount" : {"type": FieldType.DECIMAL, "label" : "Min. Investment Amount"},
        "maturity_price" : {"type": FieldType.DECIMAL, "label" : "Maturity Price"},
        "interest_commencement_date" : {"type": FieldType.DATE, "label" : "Interest Commencement Date" },
        "security_collateral" : {"label": "Security Collateral"},
        "tier" : {"type": FieldType.DECIMAL, "label" : "Tier"},
        "is_upper" : {"label": "Is Upper"},
        "is_sub_ordinate" : {"label": "Is Subordinate"},
        "is_callable" : {"label": "Is Callable"},
        "is_puttable" : {"label": "Is Puttable"},
        "is_taxable" : {"label": "Is Taxable"},
        "maturity_date" : {"type": FieldType.DATE, "label" : "Maturity Date" },
        "interest_rate" : {"type": FieldType.DECIMAL, "label" : "Int. Rate (%)"},
        "face_value" : {"type": FieldType.DECIMAL, "label" : "Face Value"},
        "paid_up_value" : {"type": FieldType.DECIMAL, "label" : "Paidup Value"}
    },
    resource_type= ResourceType.view
    )