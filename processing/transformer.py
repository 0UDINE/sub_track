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
raw_User['rÃ´le']= raw_User['rÃ´le'].replace("",'utilisateur') # handel empty values in the "rôle" column
raw_User['email'] = np.where(raw_User['email'].str.contains('user'),raw_User['email'] + '@gmail.com',raw_User['email']) #this result in some cases ".com.com"
raw_User['email']=np.where(raw_User['email'].str.contains('.com.com'),raw_User['email'].str.replace(r'(\.com)+','.com',regex=True),raw_User['email']) # so i replace the multiple ".com"s to only one ".com"
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
print(f"after cleaning : \n {raw_Fournisseur}")


# cleaning "raw_Notifications" table
raw_Notifications = tables["raw_Notifications"]
print(f"before cleaning : \n {raw_Notifications}")
# making empty strings into NaN so we can fill it later
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].replace('',np.nan)
# standarizing text , instead of ('EMAIL','email'), it will be always ('email')
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].replace({
    'EMAIL': 'email',
    'SMS': 'sms',
    'In-App': 'in-app'
    })
# filling null values with a default value
raw_Notifications["typeEnvoi"] = raw_Notifications["typeEnvoi"].fillna('email')
# stadarizing the date format
raw_Notifications['dateEnvoi'] = raw_Notifications['dateEnvoi'].apply(
    lambda x: pd.to_datetime(x, errors='coerce', dayfirst=True)
)
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
# converting date strings to datetime format with day-first format
raw_Paiement['datePaiement'] = pd.to_datetime(raw_Paiement['datePaiement'], format='%d/%m/%Y', errors='coerce')
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
# replacing empty strings with default message
raw_Paiement['statusPaiement'] = raw_Paiement['statusPaiement'].replace(
    {
        '': 'il n\'a pas de motif',
        'success': 'reussi',
        'failed': 'echoue'
    })
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
print(f"after cleaning : \n {raw_MethodePaiement}")


# cleaning "raw_UtilisateurNotif" table
raw_UtilisateurNotif = tables["raw_UtilisateurNotif"]
print(f"before cleaning : \n {raw_UtilisateurNotif}")
# identifying null values in 'estUrgent' column
nan_value=raw_UtilisateurNotif['estUrgent'].isnull()
# filling null values with random binary choices (1 or 0)
raw_UtilisateurNotif.loc[nan_value,'estUrgent']=np.random.choice([1,0],size=nan_value.sum())
# converting 'estUrgent' column to integer type
raw_UtilisateurNotif['estUrgent']=raw_UtilisateurNotif['estUrgent'].astype(int)
# standardizing boolean text values to binary integers
raw_UtilisateurNotif['estLu'] = raw_UtilisateurNotif['estLu'].replace({
    'true': 1,
    'Oui': 1,
    'false': 0,
    'Non': 0
})
# converting 'estLu' column to integer type
raw_UtilisateurNotif['estLu']=raw_UtilisateurNotif['estLu'].astype(int)
print(f"after cleaning : \n {raw_UtilisateurNotif}")


# cleaning "raw_utilisateur_abonement" table
raw_utilisateur_abonement = tables["raw_utilisateur_abonement"]
print(f"before cleaning : \n {raw_utilisateur_abonement}")
# converting dateRenouvelement strings to datetime format with day-first format
raw_utilisateur_abonement['dateRenouvelement'] = pd.to_datetime(raw_utilisateur_abonement['dateRenouvelement'], format='%d/%m/%Y', errors='coerce')
# getting unique valid renewal dates for filling missing values
value=raw_utilisateur_abonement[raw_utilisateur_abonement['dateRenouvelement'].notna()] ['dateRenouvelement'].unique()
# identifying NaN values in 'dateRenouvelement' column
nan_value=raw_utilisateur_abonement['dateRenouvelement'].isna()
# filling null dates with random choices from existing valid dates
raw_utilisateur_abonement.loc[nan_value,'dateRenouvelement']=np.random.choice(value,size=nan_value.sum())
# standardizing date format to YYYY-MM-DD
raw_utilisateur_abonement['dateRenouvelement'] = raw_utilisateur_abonement['dateRenouvelement'].dt.strftime('%Y-%m-%d')
# re-converting to datetime for consistency checking
raw_utilisateur_abonement['dateRenouvelement'] = pd.to_datetime(raw_utilisateur_abonement['dateRenouvelement'],format='%d/%m/%Y',errors='coerce')
# getting updated valid dates after formatting
valid_dates = raw_utilisateur_abonement[
    raw_utilisateur_abonement['dateRenouvelement'].notna()
]['dateRenouvelement'].unique()
# identifying remaining NaN values after re-conversion
nan_mask = raw_utilisateur_abonement['dateRenouvelement'].isna()
# filling remaining null dates with random choices from valid dates if available
if len(valid_dates) > 0:
    raw_utilisateur_abonement.loc[nan_mask, 'dateRenouvelement'] = np.random.choice(valid_dates, size=nan_mask.sum())
# otherwise filling with default date
else:
    raw_utilisateur_abonement.loc[nan_mask, 'dateRenouvelement'] = pd.to_datetime('2024-01-01')
# final standardization of renewal date format to YYYY-MM-DD
raw_utilisateur_abonement['dateRenouvelement'] = raw_utilisateur_abonement['dateRenouvelement'].dt.strftime('%Y-%m-%d')
# converting dateDebutFacturation strings to datetime format with day-first format
raw_utilisateur_abonement['dateDebutFacturation'] = pd.to_datetime(raw_utilisateur_abonement['dateDebutFacturation'], format='%d/%m/%Y', errors='coerce')
# getting unique valid billing start dates for filling missing values
value=raw_utilisateur_abonement[raw_utilisateur_abonement['dateDebutFacturation'].notna()] ['dateDebutFacturation'].unique()
# identifying NaN values in 'dateDebutFacturation' column
nan_value=raw_utilisateur_abonement['dateDebutFacturation'].isna()
# filling null billing start dates with random choices from valid dates if available
if len(valid_dates) > 0:
    raw_utilisateur_abonement.loc[nan_mask, 'dateDebutFacturation'] = np.random.choice(
        valid_dates,
        size=nan_mask.sum()
    )
# otherwise filling with default date
else:
    raw_utilisateur_abonement.loc[nan_mask, 'dateDebutFacturation'] = pd.to_datetime('2024-01-01')
# converting prochaineDeFacturation strings to datetime format with day-first format
raw_utilisateur_abonement['prochaineDeFacturation'] = pd.to_datetime(raw_utilisateur_abonement['prochaineDeFacturation'], format='%d/%m/%Y', errors='coerce')
# getting unique valid next billing dates for filling missing values
value=raw_utilisateur_abonement[raw_utilisateur_abonement['prochaineDeFacturation'].notna()] ['prochaineDeFacturation'].unique()
# identifying NaN values in 'prochaineDeFacturation' column
nan_value=raw_utilisateur_abonement['prochaineDeFacturation'].isna()
# filling null next billing dates with random choices from existing valid dates
raw_utilisateur_abonement.loc[nan_value,'prochaineDeFacturation']=np.random.choice(value,size=nan_value.sum())
# standardizing next billing date format to YYYY-MM-DD
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



