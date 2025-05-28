"""
Kafka Consumer for Subscription Data
Reads messages from Kafka topic and transforms/inserts them into SQL Server processed database
"""
import json
import time
import argparse
import pyodbc
import hashlib
import re
import logging
from confluent_kafka import Consumer, KafkaError
from typing import Dict, Any, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define processed table columns mapping
PROCESSED_TABLE_COLUMNS = {
    'processed_User': ['userId', 'email', 'motsDePasse', 'role'],
    'processed_Utilisateur_personnel': ['userId', 'prenom', 'nom', 'telephone', 'adresse'],
    'processed_Admin': ['userId'],
    'processed_Utilisateur_fournisseur': ['userId'],
    'processed_Fournisseur': ['idFournisseur', 'nomFournisseur', 'emailFournisseur', 'numTele', 'addressFournisseur',
                              'secteurFournisseur'],
    'processed_Notifications': ['idNotification', 'typeNotification', 'dateEnvoi', 'typeEnvoi', 'contenu'],
    'processed_Journal': ['idJournal', 'userId', 'idAbonnement', 'descriptionJournal', 'typeActivite',
                          'dateCreationJournal', 'valeurPrécedente', 'nouvelleValeur', 'StatusActivite', 'Timestamps'],
    'processed_Abonnement': ['idAbonnement', 'idFournisseur', 'titreAbonnement', 'descriptionAbonnement', 'prixAbonnement',
                             'categorieAbonnement'],
    'processed_Paiement': ['idPaiement', 'idAbonnement', 'userId', 'methodePaiement', 'datePaiement', 'montantPaiement',
                           'motifPaiement', 'statusPaiement'],
    'processed_MethodePaiement': ['idPaiement', 'userId', 'informationCarte', 'dateExpirementCarte', 'typeMethode'],
    'UtilisateurNotif': ['idNotification', 'userId'],
    'utilisateur_abonement': ['userId', 'idAbonnement','dateDebutFacturation','statusAbonnement', 'dateRenouvelement', 'dateDebutAbonnement', 'prochaineDeFacturation']
}


class DataTransformer:
    """Utility methods for data transformation and validation."""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using SHA256 (simplified: no check for empty password)"""
        # Original: if not password: return ""; return hashlib.sha256(password.encode()).hexdigest()
        if password:
            return hashlib.sha256(str(password).encode()).hexdigest()
        return "" # Return empty string if password is None or empty

    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format (always returns True to bypass validation)"""
        # Original: return bool(re.match(r"[^@]+@[^@]+\.[^@]+", email))
        return True

    @staticmethod
    def clean_phone_number(phone_num: str) -> Optional[str]:
        """Clean phone number (simplified: returns as string or None)"""
        # Original: cleaned = re.sub(r'\D', '', str(phone_num)); return cleaned if len(cleaned) == 10 else None
        if phone_num is None:
            return None
        return str(phone_num)

    @staticmethod
    def parse_date(date_str: Any) -> Optional[datetime]:
        """Parse date from various formats (will return None if fails to parse)"""
        if not date_str or str(date_str).lower() in ['null', 'none', '']:
            return None

        formats = [
            '%d/%m/%Y %H:%M',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S.%f'
        ]

        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except ValueError:
                continue
        # If no format matches, return None (this is still a form of validation/conversion)
        return None

    @staticmethod
    def extract_id(id_value: Any, prefix: str) -> Optional[str]:
        """Extract ID (simplified: just convert to string if not None)"""
        # Original: if not isinstance(id_str, str) or not id_str.startswith(prefix) or len(id_str) != 8:
        #           logger.error(f"Invalid ID format for {prefix}: {id_str}. Expected {prefix}XXXXX.")
        #           return None
        #           return id_str
        if id_value is None:
            return None
        return str(id_value)

    @staticmethod
    def extract_user_id(user_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(user_id_str, 'USR')

    @staticmethod
    def extract_journal_id(journal_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(journal_id_str, 'JRN')

    @staticmethod
    def extract_abonnement_id(abonnement_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(abonnement_id_str, 'ABN')

    @staticmethod
    def extract_notification_id(notification_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(notification_id_str, 'NTF')

    @staticmethod
    def extract_fournisseur_id(fournisseur_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(fournisseur_id_str, 'FRN')

    @staticmethod
    def extract_paiement_id(paiement_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(paiement_id_str, 'PMT')

    @staticmethod
    def extract_methode_paiement_id(methode_paiement_id_str: Any) -> Optional[str]:
        return DataTransformer.extract_id(methode_paiement_id_str, 'PMT')


def create_db_connection(server, database, username=None, password=None, trusted_connection=False):
    """Establishes a connection to the SQL Server database."""
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};"
    if trusted_connection:
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str += f"UID={username};PWD={password};"

    try:
        conn = pyodbc.connect(conn_str)
        logger.info(f"Connected to database {database} on server {server}")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.critical(f"Database connection error: {sqlstate} - {ex.args[1]}")
        raise # Re-raise the exception to stop execution


def check_table_exists(conn: pyodbc.Connection, table_name: str) -> bool:
    """Checks if a given table exists in the database."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", table_name)
        return cursor.fetchone() is not None
    except pyodbc.Error as ex:
        logger.error(f"Error checking table existence for {table_name}: {ex}")
        return False


def record_exists(conn: pyodbc.Connection, table_name: str, id_column: str, record_id: str) -> bool:
    """Checks if a record with the given ID exists (no longer strictly used for skipping due to constraint removal)."""
    # This function is kept but its usage for "skipping" is mostly removed in processing functions.
    # It might still be used for composite keys, but the 'continue' logic is gone.
    cursor = conn.cursor()
    try:
        query = f"SELECT 1 FROM {table_name} WHERE {id_column} = ?"
        cursor.execute(query, record_id)
        return cursor.fetchone() is not None
    except pyodbc.Error as ex:
        logger.error(f"Error checking record existence in {table_name}: {ex}", exc_info=True)
        return False


def prepare_insert_statement(table_name: str, columns: List[str], values: List[Any]):
    """Prepares an SQL INSERT statement."""
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    return sql, values


def safe_execute(cursor: pyodbc.Cursor, sql: str, values: List[Any], record_identifier: str = "unknown") -> bool:
    """Executes an SQL statement safely, logging errors and returning failure status."""
    try:
        cursor.execute(sql, values)
        return True
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.error(f"Database error for record {record_identifier}: {sqlstate} - {ex.args[1]}", exc_info=True)
        return False # Indicate failure


def process_users(conn: pyodbc.Connection, user_data: List[Dict]) -> int:
    """Processes and inserts user data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_User'):
        logger.error("Table 'processed_User' does not exist. Cannot process users.")
        return 0 # Do not raise, just log and skip for this table

    for user in user_data:
        user_id = DataTransformer.extract_user_id(user.get('userId'))
        email = user.get('email')
        mots_de_passe = user.get('motsDePasse')
        role = user.get('role') or 'utilisateur'

        hashed_password = DataTransformer.hash_password(mots_de_passe)

        sql, values = prepare_insert_statement(
            'processed_User',
            PROCESSED_TABLE_COLUMNS['processed_User'],
            [user_id, email, hashed_password, role]
        )

        if safe_execute(cursor, sql, values, f"user_{user_id}"):
            processed_count += 1

            # Insert into specific role tables if applicable (no parent existence check)
            if role.lower() == 'admin':
                sql_admin, values_admin = prepare_insert_statement(
                    'processed_Admin',
                    PROCESSED_TABLE_COLUMNS['processed_Admin'],
                    [user_id]
                )
                safe_execute(cursor, sql_admin, values_admin, f"admin_{user_id}")
            elif role.lower() == 'fournisseur':
                sql_fournisseur, values_fournisseur = prepare_insert_statement(
                    'processed_Utilisateur_fournisseur',
                    PROCESSED_TABLE_COLUMNS['processed_Utilisateur_fournisseur'],
                    [user_id]
                )
                safe_execute(cursor, sql_fournisseur, values_fournisseur, f"fournisseur_user_{user_id}")
    return processed_count


def process_personal_users(conn: pyodbc.Connection, personal_user_data: List[Dict]) -> int:
    """Processes and inserts personal user data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Utilisateur_personnel'):
        logger.error("Table 'processed_Utilisateur_personnel' does not exist. Cannot process personal users.")
        return 0

    for p_user in personal_user_data:
        user_id = DataTransformer.extract_user_id(p_user.get('userId'))
        prenom = p_user.get('prenom') or None
        nom = p_user.get('nom') or None
        telephone = DataTransformer.clean_phone_number(p_user.get('telephone'))
        adresse = p_user.get('adresse') or None

        sql, values = prepare_insert_statement(
            'processed_Utilisateur_personnel',
            PROCESSED_TABLE_COLUMNS['processed_Utilisateur_personnel'],
            [user_id, prenom, nom, telephone, adresse]
        )

        if safe_execute(cursor, sql, values, f"personal_user_{user_id}"):
            processed_count += 1
    return processed_count


def process_fournisseurs(conn: pyodbc.Connection, fournisseur_data: List[Dict]) -> int:
    """Processes and inserts fournisseur data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Fournisseur'):
        logger.error("Table 'processed_Fournisseur' does not exist. Cannot process fournisseurs.")
        return 0

    for fournisseur in fournisseur_data:
        fournisseur_id = DataTransformer.extract_fournisseur_id(fournisseur.get('idFournisseur'))
        nom_fournisseur = fournisseur.get('nomFournisseur') or None
        email_fournisseur = fournisseur.get('emailFournisseur') or None
        num_tele = DataTransformer.clean_phone_number(fournisseur.get('numTele'))
        address_fournisseur = fournisseur.get('addressFournisseur') or None
        secteur_fournisseur = fournisseur.get('secteurFournisseur') or None

        sql, values = prepare_insert_statement(
            'processed_Fournisseur',
            PROCESSED_TABLE_COLUMNS['processed_Fournisseur'],
            [fournisseur_id, nom_fournisseur, email_fournisseur, num_tele, address_fournisseur, secteur_fournisseur]
        )

        if safe_execute(cursor, sql, values, f"fournisseur_{fournisseur_id}"):
            processed_count += 1
    return processed_count


def process_notifications(conn: pyodbc.Connection, notification_data: List[Dict]) -> int:
    """Process and insert notification data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Notifications'):
        logger.error("Table 'processed_Notifications' does not exist. Cannot process notifications.")
        return 0

    for notification in notification_data:
        notification_id = DataTransformer.extract_notification_id(notification.get('idNotification'))
        type_notification = notification.get('typeNotification') or None
        date_envoi = DataTransformer.parse_date(notification.get('dateEnvoi'))
        type_envoi = notification.get('typeEnvoi') or None
        contenu = notification.get('contenu') or None

        sql, values = prepare_insert_statement(
            'processed_Notifications',
            PROCESSED_TABLE_COLUMNS['processed_Notifications'],
            [notification_id, type_notification, date_envoi, type_envoi, contenu]
        )

        if safe_execute(cursor, sql, values, f"notification_{notification_id}"):
            processed_count += 1
    return processed_count


def process_journal_entries(conn: pyodbc.Connection, journal_data: List[Dict]) -> int:
    """Process and insert journal entry data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Journal'):
        logger.error("Table 'processed_Journal' does not exist. Cannot process journal entries.")
        return 0

    for journal in journal_data:
        try:
            journal_id = DataTransformer.extract_journal_id(journal.get('idJournal'))
            user_id = DataTransformer.extract_user_id(journal.get('userId'))
            abonnement_id = DataTransformer.extract_abonnement_id(journal.get('idAbonnement'))
            description_journal = journal.get('descriptionJournal') or None
            type_activite = journal.get('typeActivite') or None
            date_creation_journal = DataTransformer.parse_date(journal.get('dateCreationJournal'))
            valeur_precedente = journal.get('valeurPrécedente') or None
            nouvelle_valeur = journal.get('nouvelleValeur') or None
            status_activite = journal.get('StatusActivite') or None
            timestamps = DataTransformer.parse_date(journal.get('Timestamps'))

            sql, values = prepare_insert_statement(
                'processed_Journal',
                PROCESSED_TABLE_COLUMNS['processed_Journal'],
                [journal_id, user_id, abonnement_id, description_journal, type_activite,
                 date_creation_journal, valeur_precedente, nouvelle_valeur, status_activite, timestamps]
            )

            if safe_execute(cursor, sql, values, f"journal_{journal_id}"):
                processed_count += 1
        except Exception as e:
            logger.error(f"Error processing journal record {journal.get('idJournal', 'unknown')}: {e}", exc_info=True)
            # Do not re-raise, allow other records in the batch to be attempted
    return processed_count


def process_abonnements(conn: pyodbc.Connection, abonnement_data: List[Dict]) -> int:
    """Process and insert abonnement data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Abonnement'):
        logger.error("Table 'processed_Abonnement' does not exist. Cannot process abonnements.")
        return 0

    for abonnement in abonnement_data:
        abonnement_id = DataTransformer.extract_abonnement_id(abonnement.get('idAbonnement'))
        fournisseur_id = DataTransformer.extract_fournisseur_id(abonnement.get('idFournisseur'))
        titre_abonnement = abonnement.get('titreAbonnement') or None
        description_abonnement = abonnement.get('descriptionAbonnement') or None
        prix_abonnement = abonnement.get('prixAbonnement')
        categorie_abonnement = abonnement.get('categorieAbonnement') or None


        sql, values = prepare_insert_statement(
            'processed_Abonnement',
            PROCESSED_TABLE_COLUMNS['processed_Abonnement'],
            [abonnement_id, fournisseur_id, titre_abonnement, description_abonnement, prix_abonnement,
             categorie_abonnement]
        )

        if safe_execute(cursor, sql, values, f"abonnement_{abonnement_id}"):
            processed_count += 1
    return processed_count


def process_paiements(conn: pyodbc.Connection, paiement_data: List[Dict]) -> int:
    """Process and insert paiement data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_Paiement'):
        logger.error("Table 'processed_Paiement' does not exist. Cannot process paiements.")
        return 0

    for paiement in paiement_data:
        paiement_id = DataTransformer.extract_paiement_id(paiement.get('idPaiement'))
        abonnement_id = DataTransformer.extract_abonnement_id(paiement.get('idAbonnement'))
        user_id = DataTransformer.extract_user_id(paiement.get('userId'))
        methode_paiement = paiement.get('methodePaiement') or None
        date_paiement = DataTransformer.parse_date(paiement.get('datePaiement'))
        montant_paiement = paiement.get('montantPaiement')
        motif_paiement = paiement.get('motifPaiement') or None
        status_paiement = paiement.get('statusPaiement') or None

        sql, values = prepare_insert_statement(
            'processed_Paiement',
            PROCESSED_TABLE_COLUMNS['processed_Paiement'],
            [paiement_id, abonnement_id, user_id, methode_paiement, date_paiement, montant_paiement, motif_paiement, status_paiement]
        )

        if safe_execute(cursor, sql, values, f"paiement_{paiement_id}"):
            processed_count += 1
    return processed_count


def process_methode_paiement(conn: pyodbc.Connection, methode_paiement_data: List[Dict]) -> int:
    """Process and insert methodePaiement data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'processed_MethodePaiement'):
        logger.error("Table 'processed_MethodePaiement' does not exist. Cannot process payment methods.")
        return 0

    for methode in methode_paiement_data:
        paiement_id = DataTransformer.extract_paiement_id(methode.get('idPaiement'))
        user_id = DataTransformer.extract_user_id(methode.get('userId'))
        information_carte = methode.get('informationCarte') or None
        date_expiration_carte = DataTransformer.parse_date(methode.get('dateExpirementCarte'))
        type_methode = methode.get('typeMethode') or None

        # Check for composite primary key existence (still present, but will just log if exists)
        try:
            cursor.execute(f"SELECT 1 FROM processed_MethodePaiement WHERE idPaiement = ? AND userId = ?", paiement_id, user_id)
            if cursor.fetchone():
                logger.info(f"Payment method for Paiement ID {paiement_id} and User ID {user_id} already exists. Skipping insertion.")
                continue # Skip insertion for existing composite key
        except pyodbc.Error as ex:
            logger.error(f"Error checking existing composite key for processed_MethodePaiement: {ex}", exc_info=True)
            # Do not re-raise, allow other records to be attempted

        sql, values = prepare_insert_statement(
            'processed_MethodePaiement',
            PROCESSED_TABLE_COLUMNS['processed_MethodePaiement'],
            [paiement_id, user_id, information_carte, date_expiration_carte, type_methode]
        )

        if safe_execute(cursor, sql, values, f"methode_paiement_{paiement_id}_{user_id}"):
            processed_count += 1
    return processed_count


def process_utilisateur_notif(conn: pyodbc.Connection, utilisateur_notif_data: List[Dict]) -> int:
    """Process and insert utilisateur_notif data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'UtilisateurNotif'):
        logger.error("Table 'UtilisateurNotif' does not exist. Cannot process user notifications.")
        return 0

    for record in utilisateur_notif_data:
        notification_id = DataTransformer.extract_notification_id(record.get('idNotification'))
        user_id = DataTransformer.extract_user_id(record.get('userId'))

        # Check for composite primary key existence (still present, but will just log if exists)
        try:
            cursor.execute(f"SELECT 1 FROM UtilisateurNotif WHERE idNotification = ? AND userId = ?", notification_id, user_id)
            if cursor.fetchone():
                logger.info(f"User Notification for Notification ID {notification_id} and User ID {user_id} already exists. Skipping insertion.")
                continue # Skip insertion for existing composite key
        except pyodbc.Error as ex:
            logger.error(f"Error checking existing composite key for UtilisateurNotif: {ex}", exc_info=True)
            # Do not re-raise, allow other records to be attempted

        sql, values = prepare_insert_statement(
            'UtilisateurNotif',
            PROCESSED_TABLE_COLUMNS['UtilisateurNotif'],
            [notification_id, user_id]
        )

        if safe_execute(cursor, sql, values, f"user_notif_{notification_id}_{user_id}"):
            processed_count += 1
    return processed_count


def process_utilisateur_abonement(conn: pyodbc.Connection, utilisateur_abonement_data: List[Dict]) -> int:
    """Process and insert utilisateur_abonement data (validations removed)."""
    processed_count = 0
    cursor = conn.cursor()

    if not check_table_exists(conn, 'utilisateur_abonement'):
        logger.error("Table 'utilisateur_abonement' does not exist. Cannot process user subscriptions.")
        return 0

    for record in utilisateur_abonement_data:
        user_id = DataTransformer.extract_user_id(record.get('userId'))
        abonnement_id = DataTransformer.extract_abonnement_id(record.get('idAbonnement'))
        date_debut_abonnement = DataTransformer.parse_date(record.get('dateDebutAbonnement'))
        date_fin_abonnement = DataTransformer.parse_date(record.get('dateFinAbonnement'))

        # Check for composite primary key existence (still present, but will just log if exists)
        try:
            cursor.execute(f"SELECT 1 FROM utilisateur_abonement WHERE userId = ? AND idAbonnement = ?", user_id, abonnement_id)
            if cursor.fetchone():
                logger.info(f"User subscription for User ID {user_id} and Abonnement ID {abonnement_id} already exists. Skipping insertion.")
                continue # Skip insertion for existing composite key
        except pyodbc.Error as ex:
            logger.error(f"Error checking existing composite key for utilisateur_abonement: {ex}", exc_info=True)
            # Do not re-raise, allow other records to be attempted

        sql, values = prepare_insert_statement(
            'utilisateur_abonement',
            PROCESSED_TABLE_COLUMNS['utilisateur_abonement'],
            [user_id, abonnement_id, date_debut_abonnement, date_fin_abonnement]
        )

        if safe_execute(cursor, sql, values, f"user_abonnement_{user_id}_{abonnement_id}"):
            processed_count += 1
    return processed_count


def create_kafka_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    """Creates and configures a Kafka consumer."""
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer


def process_kafka_message(conn: pyodbc.Connection, msg) -> bool:
    """
    Processes a single Kafka message and inserts its data into the database.
    This version of the function has reduced internal validation.
    """
    try:
        full_message = json.loads(msg.value().decode('utf-8'))

        if 'data' in full_message and isinstance(full_message['data'], dict):
            message_data = full_message['data']
        else:
            logger.warning(
                f"Kafka message at offset {msg.offset()} does not contain a 'data' key or it's not a dictionary. Skipping message.")
            return False

        # Define a desired processing order for tables to handle potential dependencies.
        ordered_table_names = [
            'raw_User',
            'raw_Fournisseur',
            'raw_Utilisateur_personnel',
            'raw_Notifications',
            'raw_Abonnement',
            'raw_Paiement',
            'raw_MethodePaiement',
            'raw_Journal',
            'raw_UtilisateurNotif',
            'raw_utilisateur_abonement'
        ]

        # Process tables according to the defined order
        for table_name in ordered_table_names:
            if table_name in message_data:
                records = message_data[table_name]
                if not records:
                    continue

                logger.info(f"Processing {len(records)} records for table: {table_name}")

                # Call the appropriate processing function.
                # These functions no longer raise ValueErrors for data issues,
                # but might still return 0 if the table doesn't exist.
                if table_name == 'raw_User':
                    process_users(conn, records)
                elif table_name == 'raw_Utilisateur_personnel':
                    process_personal_users(conn, records)
                elif table_name == 'raw_Fournisseur':
                    process_fournisseurs(conn, records)
                elif table_name == 'raw_Notifications':
                    process_notifications(conn, records)
                elif table_name == 'raw_Journal':
                    process_journal_entries(conn, records)
                elif table_name == 'raw_Abonnement':
                    process_abonnements(conn, records)
                elif table_name == 'raw_Paiement':
                    process_paiements(conn, records)
                elif table_name == 'raw_MethodePaiement':
                    process_methode_paiement(conn, records)
                elif table_name == 'raw_UtilisateurNotif':
                    process_utilisateur_notif(conn, records)
                elif table_name == 'raw_utilisateur_abonement':
                    process_utilisateur_abonement(conn, records)
                elif table_name in ['raw_Admin', 'raw_Utilisateur_fournisseur']:
                    logger.debug(f"Table '{table_name}' handled by 'process_users' function. Skipping explicit processing.")
                else:
                    logger.warning(f"No specific processing function mapped for table: {table_name}. Skipping records.")

        # If we reach here, all records for this message were attempted without unhandled exceptions.
        # Commit the entire transaction for this message.
        conn.commit()
        logger.info(f"Successfully processed and committed batch from offset {msg.offset()}.")
        return True

    except pyodbc.Error as e:
        # Catch pyodbc.Error for direct DB issues
        conn.rollback() # Rollback the entire transaction for this message
        logger.error(f"Database error processing Kafka message at offset {msg.offset()}. Transaction rolled back. Error: {e}", exc_info=True)
        return False # Indicate failure to process this message
    except Exception as e:
        # Catch any other unexpected errors
        conn.rollback() # Ensure rollback for any unhandled exception
        logger.critical(f"An unhandled critical error occurred while processing Kafka message at offset {msg.offset()}. Transaction rolled back. Error: {e}", exc_info=True)
        return False


def consume_messages(consumer: Consumer, conn: pyodbc.Connection, timeout: float = 1.0,
                     max_messages: int = None,
                     batch_size: int = 1) -> int:
    """Consume messages from Kafka and insert them into the database"""
    message_count = 0

    try:
        running = True
        while running:
            msg = consumer.poll(timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition: {msg.topic()}, {msg.partition()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            if process_kafka_message(conn, msg):
                message_count += 1
                consumer.commit(message=msg, asynchronous=False)
                logger.debug(f"Committed offset for message at offset {msg.offset()}")
            else:
                logger.warning(f"Failed to fully process message at offset {msg.offset()}. Not committing this offset.")

            if max_messages and message_count >= max_messages:
                logger.info(f"Reached maximum messages ({max_messages}), stopping.")
                running = False

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in consume_messages: {e}", exc_info=True)
    finally:
        logger.info(f"Consumer stopped. Total messages successfully processed and committed: {message_count}")

    return message_count


def main():
    parser = argparse.ArgumentParser(description="Kafka Consumer for Subscription Data")
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, required=True,
                        help='Kafka topic to consume from')
    parser.add_argument('--group-id', type=str, default='subscription_consumer_group',
                        help='Consumer group ID')
    parser.add_argument('--max-messages', type=int, default=None,
                        help='Maximum number of messages to consume before exiting')
    parser.add_argument('--batch-size', type=int, default=1,
                        help='Number of messages to process in a batch (currently processes one Kafka message at a time fully)')
    parser.add_argument('--sql-server', type=str, required=True,
                        help='SQL Server instance name or IP address')
    parser.add_argument('--database', type=str, required=True,
                        help='SQL Server database name')
    parser.add_argument('--db-username', type=str, default=None,
                        help='SQL Server username (if not using trusted connection)')
    parser.add_argument('--db-password', type=str, default=None,
                        help='SQL Server password (if not using trusted connection)')
    parser.add_argument('--trusted-connection', action='store_true',
                        help='Use Windows Trusted_Connection for SQL Server')

    args = parser.parse_args()

    consumer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([args.topic])
    print(f"Subscribed to topic: {args.topic}")

    try:
        conn = create_db_connection(
            args.sql_server,
            args.database,
            username=args.db_username,
            password=args.db_password,
            trusted_connection=args.trusted_connection
        )

        message_count = consume_messages(consumer, conn, batch_size=args.batch_size, max_messages=args.max_messages)
        print(f"Successfully processed {message_count} messages")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()