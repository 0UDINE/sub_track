import pandas as pd
import numpy as np
import json
import re

json_file_path = r'C:\Users\MaxHeaP\PycharmProjects\subscription_pipeline\raw_data_output.json'
with open(json_file_path, 'r') as file :
    database = json.load(file)

# assure that the variable tables is a dict
tables = {}
for table_name,record in database.items():
    tables[table_name] = pd.DataFrame(record)


# cleaning "raw_User" table
raw_User = tables['raw_User']
print(f"before cleaning : \n {raw_User}")
raw_User['role']= raw_User['role'].replace("",'utilisateur') # handel empty values in the "rôle" column
raw_User['email'] = np.where(raw_User['email'].str.contains('user'),raw_User['email'] + '@gmail.com',raw_User['email']) #this result in some cases ".com.com"
raw_User['email']=np.where(raw_User['email'].str.contains('.com.com'),raw_User['email'].str.replace(r'(\.com)+','.com',regex=True),raw_User['email']) # so i replace the multiple ".com"s to only one ".com"
raw_User.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_User}")

# cleaning "raw_Utilisateur_personnel" table
raw_Utilisateur_personnel = tables['raw_Utilisateur_personnel']
print(f"before cleaning : \n {raw_Utilisateur_personnel}")
# handling empty values in the "prenom" column by filling it with "inconnu(e)"
raw_Utilisateur_personnel['prenom'] = np.where(raw_Utilisateur_personnel['prenom'] == '',raw_Utilisateur_personnel['prenom'].replace('','inconnu(e)'),raw_Utilisateur_personnel['prenom'])
# striping(removing) everything but digits (123-456-789) -> (123456789)
raw_Utilisateur_personnel['telephone'] = raw_Utilisateur_personnel['telephone'].str.replace(r'\D', '', regex=True)
# standarizing the format of the phone number (e.g.: (12) 3456789)
raw_Utilisateur_personnel['telephone'] = raw_Utilisateur_personnel['telephone'].astype(str).apply(lambda x : f"({x[:2]}){x[2:]}" if x.strip().isdigit() else "0000000000")
raw_Utilisateur_personnel.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_Utilisateur_personnel}")

# cleaning "raw_Fournisseur" table
raw_Fournisseur = tables['raw_Fournisseur']
print(f"before cleaning : \n {raw_Fournisseur}")
# striping(removing) everything but digits (123-456-789) -> (123456789)
raw_Fournisseur['numTele'] = raw_Fournisseur['numTele'].str.replace(r'\D','',regex=True)
# standarizing the format of the phone number (e.g.: (12) 3456789)
raw_Fournisseur['numTele'] = raw_Fournisseur['numTele'].astype(str).apply(lambda x : f"({x[:2]}) {x[2:]}")
# handling empty emails but creating new ones usign this formula : <nomFournisseur>@<nomFournisseur>.com
raw_Fournisseur.loc[raw_Fournisseur['emailFournisseur'] == '', 'emailFournisseur'] = \
    raw_Fournisseur.loc[raw_Fournisseur['emailFournisseur'] == ''].apply(
        lambda x: f"{x['nomFournisseur'].replace(' ', '').lower()}@{x['nomFournisseur'].replace(' ', '').lower()}.com",
        axis=1
    )
raw_Fournisseur.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_Fournisseur}")


# cleaning "raw_Notifications" table
raw_Notifications = tables["raw_Notifications"]
print(f"before cleaning : \n {raw_Notifications}")
# making empty strings into NaN so we can fill it later
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].replace('',np.nan)
# standarizing text , instead of ('EMAIL','email'), it will be always ('email')
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].str.lower()
raw_Notifications["typeNotification"] = raw_Notifications["typeNotification"].str.lower()
# filling null values with a default value
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].fillna('email')

# stadarizing the date format
def standardize_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return np.nan

    # Try parsing with different formats
    for fmt in [
        '%d/%m/%Y %H:%M',  # 23/03/2025 05:00
        '%Y-%m-%dT%H:%M:%S.%fZ',  # 2023-12-31T23:59:59.999Z
        '%Y-%m-%dT%H:%M:%S',  # 2025-05-23T16:29:10
        '%Y-%m-%d',  # 2025-02-03
        '%d/%m/%Y'  # 23/03/2025 (without time)
    ]:
        try:
            dt = pd.to_datetime(date_str, format=fmt, errors='raise')
            return dt.strftime('%Y-%m-%d')
        except:
            continue

    return np.nan


# Apply the function to your column
raw_Notifications['dateEnvoi'] = raw_Notifications['dateEnvoi'].apply(standardize_date)
print(f"after cleaning : \n {raw_Notifications}")


# cleaning "raw_Journal" table
raw_Journal = tables["raw_Journal"]
print(f"before cleaning : \n {raw_Journal}")
# identifying existing NaN values in 'nouvelleValeur' column
nan_value=raw_Journal['nouvelleValeur'].isna()
# keeping existing NaN values as NaN
raw_Journal.loc[nan_value,'nouvelleValeur']=np.nan
# making empty strings into NaN so we can handle them consistently
raw_Journal['nouvelleValeur'] = raw_Journal['nouvelleValeur'].replace('',np.nan)
# identifying NaN values in 'valeurPrécédente' column
nan_value=raw_Journal['valeurPrÃ©cedente'].isna()
# filling null values with random integers between 0 and 100
raw_Journal.loc[nan_value,'valeurPrÃ©cedente']=np.random.randint(0,101,size=nan_value.sum())
# getting unique values from non-null 'idAbonnement' entries
value=raw_Journal[raw_Journal['idAbonnement'].notna()] ['idAbonnement'].unique()
# identifying NaN values in 'idAbonnement' column
nan_value=raw_Journal['idAbonnement'].isna()
# filling null values with random choices from existing valid IDs
raw_Journal.loc[nan_value,'idAbonnement']=np.random.choice(value,size=nan_value.sum())
# standardizing text to lowercase for 'StatusActivite' column
raw_Journal['StatusActivite']=raw_Journal['StatusActivite'].str.lower()
raw_Journal.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_Journal}")


# cleaning "raw_Abonnement" table
raw_Abonnement = tables["raw_Abonnement"]
print(f"before cleaning : \n {raw_Abonnement}")
# identifying NaN values in 'idFournisseur' column
nan_value=raw_Abonnement['idFournisseur'].isna()
# getting unique values from non-null 'idFournisseur' entries
value=raw_Abonnement[raw_Abonnement['idFournisseur'].notna()] ['idFournisseur'].unique()
# filling null values with random choices from existing valid IDs
raw_Abonnement.loc[nan_value,'idFournisseur']=np.random.choice(value,size=nan_value.sum())
# converting price column to string for text processing
raw_Abonnement['prixAbonnement']=raw_Abonnement['prixAbonnement'].astype(str)
# extracting numeric values (including decimals with comma or dot)
raw_Abonnement['prixAbonnement']=raw_Abonnement['prixAbonnement'].str.extract(r'(\d+[\.,]?\d*)',expand=False)
# standardizing decimal separator by replacing comma with dot
raw_Abonnement['prixAbonnement']=raw_Abonnement['prixAbonnement'].str.replace(',','.')
# converting cleaned price values to float type
raw_Abonnement['prixAbonnement']=raw_Abonnement['prixAbonnement'].astype(float)
raw_Abonnement.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_Abonnement}")


# cleaning "raw_Paiement" table
raw_Paiement = tables["raw_Paiement"]
print(f"before cleaning : \n {raw_Paiement}")
# standardizing text to lowercase for 'statusPaiement' column
raw_Paiement['statusPaiement']=raw_Paiement['statusPaiement'].str.lower()
# standardizing text to lowercase for 'methodePaiement' column
raw_Paiement['methodePaiement']=raw_Paiement['methodePaiement'].str.lower()
# standardizing text to lowercase for 'source_system' column
raw_Paiement['source_system']=raw_Paiement['source_system'].str.lower()

# FIXED: converting date strings to datetime format with day-first format (was using wrong DataFrame)
raw_Paiement['datePaiement'] = pd.to_datetime(
    raw_Paiement['datePaiement'].replace('', np.nan),
    errors='coerce',
    dayfirst=True
)
# getting unique valid dates for filling missing values
valid_dates = raw_Paiement[raw_Paiement['datePaiement'].notna()]['datePaiement'].unique()
# identifying NaN values in 'datePaiement' column
nan_mask = raw_Paiement['datePaiement'].isna()
# filling null dates with random choices from existing valid dates if available
if len(valid_dates) > 0:
    raw_Paiement.loc[nan_mask, 'datePaiement'] = np.random.choice(valid_dates, size=nan_mask.sum())
# otherwise filling with default date
else:
    raw_Paiement.loc[nan_mask, 'datePaiement'] = pd.to_datetime('2024-01-01')
# standardizing date format to YYYY-MM-DD
raw_Paiement['datePaiement'] = raw_Paiement['datePaiement'].dt.strftime('%Y-%m-%d')

# converting payment amount column to string for text processing
raw_Paiement['montantPaiement']=raw_Paiement['montantPaiement'].astype(str)
# extracting numeric values (including decimals with comma or dot)
raw_Paiement['montantPaiement']=raw_Paiement['montantPaiement'].str.extract(r'(\d+[\.,]?\d*)',expand=False)
# standardizing decimal separator by replacing comma with dot
raw_Paiement['montantPaiement']=raw_Paiement['montantPaiement'].str.replace(',','.')
# converting cleaned payment amount values to float type
raw_Paiement['montantPaiement']=raw_Paiement['montantPaiement'].astype(float)
# getting unique values from non-null 'motifPaiement' entries
value=raw_Paiement[raw_Paiement['motifPaiement'].notna()]  ['motifPaiement'].unique()
# identifying NaN values in 'motifPaiement' column
nan_value=raw_Paiement['motifPaiement'].isna()
# filling null values with random choices from existing valid payment reasons
raw_Paiement.loc[nan_value,'motifPaiement']=np.random.choice(value,size=nan_value.sum())

# FIXED: replacing empty strings with default message (moved after text standardization)
raw_Paiement['statusPaiement'] = raw_Paiement['statusPaiement'].replace(
    {
        '': 'il n\'a pas de motif',
        'success': 'reussi',
        'failed': 'echoue'
    })
raw_Paiement.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_Paiement}")


# cleaning "raw_MethodePaiement" table
def mask_card_info(val):
    digits = re.findall(r'\d', str(val))
    if len(digits) >= 4:
        return "**** **** **** " + ''.join(digits[-4:])
    return "**** **** **** ****"


def clean_expiry_date(date_str):
    date_str = str(date_str).strip()

    # 1. format MM/YY ou MM/YYYY
    match1 = re.match(r"^(\d{2})/(\d{2,4})$", date_str)
    if match1:
        month, year = match1.groups()
        if len(year) == 2:
            year = '20' + year
        return f"{year}-{month.zfill(2)}"

    # 2. format YYYY-MM or YYYY/MM
    match2 = re.match(r"^(\d{4})[-/](\d{2})$", date_str)
    if match2:
        year, month = match2.groups()
        return f"{year}-{month}"

    # 3. format JJ/MM/YYYY ou JJ-MM-YYYY
    match3 = re.match(r"^\d{2}[-/](\d{2})[-/](\d{4})$", date_str)
    if match3:
        month, year = match3.groups()
        return f"{year}-{month.zfill(2)}"

    return None  # valeur non reconnue

raw_MethodePaiement = tables["raw_MethodePaiement"]
print(f"before cleaning : \n {raw_MethodePaiement}")
# applying card masking function for security
raw_MethodePaiement['informationCarte'] = raw_MethodePaiement['informationCarte'].apply(mask_card_info)
# standardizing expiry date format
raw_MethodePaiement['dateExpirementCarte'] = raw_MethodePaiement['dateExpirementCarte'].apply(clean_expiry_date)
# standardizing text to lowercase for 'typeMethode' column
raw_MethodePaiement['typeMethode']=raw_MethodePaiement['typeMethode'].str.lower()
# standardizing text to lowercase for 'source_system' column
raw_MethodePaiement['source_system']=raw_MethodePaiement['source_system'].str.lower()
raw_MethodePaiement.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_MethodePaiement}")


# cleaning "raw_UtilisateurNotif" table
raw_UtilisateurNotif = tables["raw_UtilisateurNotif"]
print(f"before cleaning : \n {raw_UtilisateurNotif}")
# identifying null values in 'estUrgent' column
nan_value=raw_UtilisateurNotif['estUrgent'].isnull()
# filling null values with random binary choices (1 or 0)
raw_UtilisateurNotif.loc[nan_value,'estUrgent']=np.random.choice([1,0],size=nan_value.sum())
# standardizing boolean text values to binary integers
raw_UtilisateurNotif['estLu'] = raw_UtilisateurNotif['estLu'].replace({
    'true': True,
    'Oui': True,
    'false': False,
    'Non': False,
    1 : True,
    '1' : True,
    0 : False,
    "0" : False
})
raw_UtilisateurNotif['estUrgent'] = raw_UtilisateurNotif['estUrgent'].replace({
    1: True,
    0: False,
    None : False
})
raw_UtilisateurNotif.drop(columns=['raw_id'], inplace=True, errors='ignore')
print(f"after cleaning : \n {raw_UtilisateurNotif}")


# FIXED: cleaning "raw_utilisateur_abonement" table
raw_utilisateur_abonement = tables["raw_utilisateur_abonement"]
print(f"before cleaning : \n {raw_utilisateur_abonement}")

# Helper function to safely handle date conversions
def safe_date_convert(date_series, format_str='%d/%m/%Y'):
    """Safely convert date series with error handling"""
    return pd.to_datetime(date_series.replace('', np.nan), format=format_str, errors='coerce')

def safe_date_fill(df, column_name, valid_dates):
    """Safely fill missing dates"""
    nan_mask = df[column_name].isna()
    if len(valid_dates) > 0 and nan_mask.sum() > 0:
        df.loc[nan_mask, column_name] = np.random.choice(valid_dates, size=nan_mask.sum())
    elif nan_mask.sum() > 0:
        df.loc[nan_mask, column_name] = pd.to_datetime('2024-01-01')
    return df

# Process dateRenouvelement
raw_utilisateur_abonement['dateRenouvelement'] = safe_date_convert(raw_utilisateur_abonement['dateRenouvelement'])
valid_renewal_dates = raw_utilisateur_abonement[raw_utilisateur_abonement['dateRenouvelement'].notna()]['dateRenouvelement'].unique()
raw_utilisateur_abonement = safe_date_fill(raw_utilisateur_abonement, 'dateRenouvelement', valid_renewal_dates)
raw_utilisateur_abonement['dateRenouvelement'] = raw_utilisateur_abonement['dateRenouvelement'].dt.strftime('%Y-%m-%d')

# Process dateDebutFacturation
raw_utilisateur_abonement['dateDebutFacturation'] = safe_date_convert(raw_utilisateur_abonement['dateDebutFacturation'])
valid_billing_dates = raw_utilisateur_abonement[raw_utilisateur_abonement['dateDebutFacturation'].notna()]['dateDebutFacturation'].unique()
raw_utilisateur_abonement = safe_date_fill(raw_utilisateur_abonement, 'dateDebutFacturation', valid_billing_dates)
raw_utilisateur_abonement['dateDebutFacturation'] = raw_utilisateur_abonement['dateDebutFacturation'].dt.strftime('%Y-%m-%d')

# Process prochaineDeFacturation
raw_utilisateur_abonement['prochaineDeFacturation'] = safe_date_convert(raw_utilisateur_abonement['prochaineDeFacturation'])
valid_next_billing_dates = raw_utilisateur_abonement[raw_utilisateur_abonement['prochaineDeFacturation'].notna()]['prochaineDeFacturation'].unique()
raw_utilisateur_abonement = safe_date_fill(raw_utilisateur_abonement, 'prochaineDeFacturation', valid_next_billing_dates)
raw_utilisateur_abonement['prochaineDeFacturation'] = raw_utilisateur_abonement['prochaineDeFacturation'].dt.strftime('%Y-%m-%d')

# standardizing text to lowercase for 'typeFacturation' column
raw_utilisateur_abonement['typeFacturation'] = raw_utilisateur_abonement['typeFacturation'].str.lower()
# standardizing billing type terms to French equivalents
raw_utilisateur_abonement['typeFacturation'] = raw_utilisateur_abonement['typeFacturation'].replace({
    'annual': 'annuel',
    'mensual': 'mensuel',
})
# standardizing text to lowercase for 'statutAbonnement' column
raw_utilisateur_abonement['statutAbonnement'] = raw_utilisateur_abonement['statutAbonnement'].str.lower()
# standardizing text to lowercase for 'source_system' column
raw_utilisateur_abonement['source_system']=raw_utilisateur_abonement['source_system'].str.lower()
print(f"after cleaning : \n {raw_utilisateur_abonement}")


# Collect all cleaned DataFrames back into the tables dictionary
tables['raw_User'] = raw_User
tables['raw_Utilisateur_personnel'] = raw_Utilisateur_personnel
tables['raw_Fournisseur'] = raw_Fournisseur
tables['raw_Notifications'] = raw_Notifications
tables['raw_Journal'] = raw_Journal
tables['raw_Abonnement'] = raw_Abonnement
tables['raw_Paiement'] = raw_Paiement
tables['raw_MethodePaiement'] = raw_MethodePaiement
tables['raw_UtilisateurNotif'] = raw_UtilisateurNotif
tables['raw_utilisateur_abonement'] = raw_utilisateur_abonement

# Convert DataFrames back to dictionary format (same structure as original JSON)
cleaned_database = {}
for table_name, df in tables.items():
    # Convert DataFrame to list of dictionaries (records)
    # Handle NaN/NaT values by converting them to None for JSON serialization
    df_clean = df.copy()

    # Replace NaN and NaT values with None for JSON compatibility
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    # Convert to records (list of dictionaries)
    cleaned_database[table_name] = df_clean.to_dict('records')

# Save cleaned data to JSON file
output_file_path = 'cleaned_data_output.json'
with open(output_file_path, 'w', encoding='utf-8') as file:
    json.dump(cleaned_database, file, ensure_ascii=False, indent=4, default=str)

print(f"\nCleaned data successfully saved to {output_file_path}")

# Optional: Print summary of cleaned data
print("\nSummary of cleaned data:")
for table_name, records in cleaned_database.items():
    print(f"- {table_name}: {len(records)} records")