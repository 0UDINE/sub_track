"""
Enhanced Raw Data Generator for Subscription Management System
Generates raw data matching the raw database schema with realistic inconsistencies
"""
from faker import Faker
import random
import json
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

# Initialize Faker with multiple locales
fake = Faker(['fr_FR', 'en_US'])  # Primarily French with some English mixed in

# Configure random seed for reproducibility
random.seed(42)
np.random.seed(42)
fake.seed_instance(42)

class RawDataGenerator:
    """Class for generating raw data matching the raw database schema"""

    def __init__(self, num_users=50, num_suppliers=20, num_subscriptions=100,
                 num_payments=200, num_notifications=300, num_journals=400):
        """Initialize with desired data volumes"""
        self.num_users = num_users
        self.num_suppliers = num_suppliers
        self.num_subscriptions = num_subscriptions
        self.num_payments = num_payments
        self.num_notifications = num_notifications
        self.num_journals = num_journals

        # Data containers for raw tables
        self.raw_users: List[Dict[str, Any]] = []
        self.raw_utilisateur_personnel: List[Dict[str, Any]] = []
        self.raw_admins: List[Dict[str, Any]] = []
        self.raw_utilisateur_fournisseur: List[Dict[str, Any]] = []
        self.raw_fournisseurs: List[Dict[str, Any]] = []
        self.raw_notifications: List[Dict[str, Any]] = []
        self.raw_journals: List[Dict[str, Any]] = []
        self.raw_abonnements: List[Dict[str, Any]] = []
        self.raw_paiements: List[Dict[str, Any]] = []
        self.raw_methodes_paiement: List[Dict[str, Any]] = []
        self.raw_utilisateur_notif: List[Dict[str, Any]] = []
        self.raw_utilisateur_abonement: List[Dict[str, Any]] = []

        # Track IDs as strings to match raw schema
        self.next_user_id = 1
        self.next_fournisseur_id = 1
        self.next_notification_id = 1
        self.next_journal_id = 1
        self.next_abonnement_id = 1
        self.next_paiement_id = 1

    def _generate_source_system(self) -> str:
        """Generate plausible source system names"""
        return random.choice([
            "CRM_System",
            "Web_Form",
            "Mobile_App_v2",
            "Legacy_DB",
            "API_Import",
            "Manual_Entry"
        ])

    def generate_raw_user(self) -> Dict[str, Any]:
        """Generate raw user record with inconsistencies"""
        # Generate various email formats
        email_formats = [
            fake.email(),
            f"{fake.user_name()}+{random.randint(1,100)}@example.com",
            f"{fake.first_name().lower()}.{fake.last_name().lower()}@company.fr",
            f"{fake.word()}@localhost",  # Invalid format
            f"user{random.randint(100,999)}"  # Missing domain
        ]

        # Generate various password formats
        password_formats = [
            fake.password(length=12),
            f" {fake.password(length=10)} ",  # With whitespace
            fake.password(length=8) + str(random.randint(1, 100)),  # With numbers
            fake.password(length=16, special_chars=False),
            "password123"  # Common weak password
        ]

        user = {
            "raw_id": len(self.raw_users) + 1,
            "userId": f"USR{self.next_user_id:05}",  # String ID format
            "email": random.choice(email_formats),
            "motsDePasse": random.choice(password_formats),
            "role": random.choice(["utilisateur", "admin", "fournisseur", "manager", ""]),  # Inconsistent roles
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system(),
            "data_quality_flag": random.choice([0, 1, None])
        }

        self.next_user_id += 1
        self.raw_users.append(user)
        return user

    def generate_raw_utilisateur_personnel(self) -> Dict[str, Any]:
        """Generate raw personnel user data"""
        user = self.generate_raw_user()

        # Generate various phone number formats
        phone_formats = [
            fake.phone_number(),
            f"0{random.randint(100000000, 999999999)}",  # French format
            f"+33 {random.randint(1,9)}{random.randint(10000000, 99999999)}",
            f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(1000, 9999)}",  # US format
            f"({random.randint(10, 99)}) {random.randint(1000, 9999)}-{random.randint(1000, 9999)}",  # Brazilian format
            "N/A"  # Missing value indicator
        ]

        personnel = {
            "raw_id": len(self.raw_utilisateur_personnel) + 1,
            "userId": user["userId"],  # Inherited from user
            "prenom": random.choice([fake.first_name(), fake.first_name().upper(), ""]),  # Sometimes empty
            "nom": random.choice([fake.last_name(), fake.last_name().lower()]),
            "telephone": random.choice(phone_formats),
            "adresse": fake.address().replace('\n', '; '),  # Different delimiter
            "date_extraction": user["date_extraction"],
            "source_system": user["source_system"],
            "data_quality_notes": random.choice([None, "Missing first name", "Phone format issue", "Address incomplete"])
        }

        self.raw_utilisateur_personnel.append(personnel)
        return personnel

    def generate_raw_admin(self) -> Dict[str, Any]:
        """Generate raw admin user data"""
        user = self.generate_raw_user()

        admin = {
            "raw_id": len(self.raw_admins) + 1,
            "userId": user["userId"],
            "date_extraction": user["date_extraction"],
            "source_system": user["source_system"]
        }

        self.raw_admins.append(admin)
        return admin

    def generate_raw_utilisateur_fournisseur(self) -> Dict[str, Any]:
        """Generate raw supplier user data"""
        user = self.generate_raw_user()
        fournisseur = self.generate_raw_fournisseur()

        fournisseur_user = {
            "raw_id": len(self.raw_utilisateur_fournisseur) + 1,
            "userId": user["userId"],
            "idFournisseur" : fournisseur["idFournisseur"],
            "date_extraction": user["date_extraction"],
            "source_system": user["source_system"]
        }

        self.raw_utilisateur_fournisseur.append(fournisseur_user)
        return fournisseur_user

    def generate_raw_fournisseur(self) -> Dict[str, Any]:
        """Generate raw supplier data"""
        # Generate various sector formats
        sectors = [
            "Industrie",
            "Agriculture",
            "Technologie",
            "Santé",
            "Construction",
            "Transport",
            "Commerce",
            "INDUSTRIE",  # Uppercase
            "tech",  # Abbreviated
            "Autre"  # Other category
        ]

        fournisseur = {
            "raw_id": len(self.raw_fournisseurs) + 1,
            "idFournisseur": f"FRN{self.next_fournisseur_id:04}",
            "nomFournisseur": random.choice([fake.company(), fake.company().upper()]),
            "emailFournisseur": random.choice([fake.company_email(), f"contact@{fake.domain_word()}.com", ""]),
            "numTele": fake.phone_number(),
            "addressFournisseur": fake.address().replace('\n', ' - '),  # Different delimiter
            "secteurFournisseur": random.choice(sectors),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.next_fournisseur_id += 1
        self.raw_fournisseurs.append(fournisseur)
        return fournisseur

    def generate_raw_notifications(self) -> Dict[str, Any]:
        """Generate raw notification data"""
        # Generate various date formats
        date_formats = [
            fake.date_time_this_year().isoformat(),
            fake.date_time_this_year().strftime('%d/%m/%Y %H:%M'),
            fake.date_time_this_year().strftime('%Y-%m-%d'),
            str(fake.date_time_this_year()),
            "2023-12-31T23:59:59.999Z"  # ISO with timezone
        ]

        notification_types = [
            "PAYMENT_REMINDER",
            "SUBSCRIPTION_EXPIRY",
            "payment_reminder",  # Lowercase
            "Subscription Expiry",  # Spaced
            "SYSTEM_ALERT",
            "INFO",
            "URGENT"
        ]

        notification = {
            "raw_id": len(self.raw_notifications) + 1,
            "idNotification": f"NOT{self.next_notification_id:06}",
            "typeNotification": random.choice(notification_types),
            "dateEnvoi": random.choice(date_formats),
            "typeEnvoi": random.choice(["email", "SMS", "push", "In-App", "EMAIL", ""]),
            "contenu": fake.text(max_nb_chars=random.randint(50, 500)),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.next_notification_id += 1
        self.raw_notifications.append(notification)
        return notification

    def generate_raw_journal(self) -> Dict[str, Any]:
        """Generate raw journal/log data"""
        # Generate various activity types
        activities = [
            "CREATION_ABONEMMENT",
            "creation abonnement",  # Spaced
            "ANNULATION_ABONEMMENT",
            "annulation",  # Shortened
            "ECHOUE_PAIEMENT",
            "PAIMENT_SUCCCES",
            "PRIX_ABONEMENT_CHANGE",
            "MODIFICATION_PROFIL",
            "login",
            "LOGOUT"
        ]

        journal = {
            "raw_id": len(self.raw_journals) + 1,
            "idJournal": f"JRN{self.next_journal_id:05}",
            "userId": random.choice([user["userId"] for user in self.raw_users] + [None, "UNKNOWN"]),
            "idAbonnement": random.choice([ab["idFournisseur"] for ab in self.raw_fournisseurs] + [None, "N/A"]),
            "descriptionJournal": fake.sentence(),
            "typeActivite": random.choice(activities),
            "dateCreationJournal": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "valeurPrécedente": random.choice([fake.word(), str(random.randint(1, 100)), None, ""]),
            "nouvelleValeur": random.choice([fake.word(), str(random.randint(1, 100)), None, ""]),
            "StatusActivite": random.choice(["COMPLETED", "failed", "In Progress", "PENDING"]),
            "Timestamps": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.next_journal_id += 1
        self.raw_journals.append(journal)
        return journal

    def generate_raw_abonnement(self) -> Dict[str, Any]:
        """Generate raw subscription data"""
        # Generate various price formats
        price_formats = [
            round(random.uniform(5, 100), 2),
            f"{round(random.uniform(5, 100), 2)} €",
            f"EUR {round(random.uniform(5, 100), 2)}",
            str(round(random.uniform(5, 100), 2)).replace('.', ','),  # European decimal
            round(random.uniform(5, 100))  # No decimal
        ]
        
        categories = [
            "STREAMING",
            "MUSIC",
            "CLOUD",
            "FORFAIT APPEL CONX",
            "streaming",  # Lowercase
            "Cloud Storage",  # Spaced
            "TELECOM"
        ]

        abonnement = {
            "raw_id": len(self.raw_abonnements) + 1,
            "idAbonnement": f"ABN{self.next_abonnement_id:05}",
            "idFournisseur": random.choice([fourn["idFournisseur"] for fourn in self.raw_fournisseurs] + [None]),
            "titreAbonnement": fake.catch_phrase(),
            "descriptionAbonnement": fake.text(max_nb_chars=200),
            "prixAbonnement": random.choice(price_formats),
            "categorieAbonnement": random.choice(categories),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.next_abonnement_id += 1
        self.raw_abonnements.append(abonnement)
        return abonnement

    def generate_raw_utilisateur_abonement(self) -> Dict[str, Any]:
        """Generate raw user-subscription relationship data"""
        if not self.raw_users or not self.raw_abonnements:
            return {}

        statuses = [
            "ACTIF",
            "actif",  # Lowercase
            "EXPIRE",
            "ANNULE",
            "PENDING",
            "SUSPENDU",
            "Inactif"
        ]

        billing_types = [
            "MENSUAL",
            "ANNUEL",
            "mensuel",  # Lowercase
            "Annual",  # English
            "QUOTIDIEN",
            "HEBDOMADAIRE",
            "Bi-Mensuel"  # Variant
        ]

        # Generate dates with various formats
        start_date = fake.date_between(start_date='-1y', end_date='today')
        date_formats = [
            start_date.strftime('%Y-%m-%d'),
            start_date.strftime('%d/%m/%Y'),
            start_date.strftime('%m/%d/%Y'),
            str(start_date),
            start_date.isoformat()
        ]

        abonement = {
            "raw_id": len(self.raw_utilisateur_abonement) + 1,
            "userId": random.choice([u["userId"] for u in self.raw_users]),
            "idAbonnement": random.choice([a["idAbonnement"] for a in self.raw_abonnements]),
            "typeFacturation": random.choice(billing_types),
            "statutAbonnement": random.choice(statuses),
            "dateRenouvelement": random.choice(date_formats),
            "dateDebutFacturation": random.choice(date_formats),
            "prochaineDeFacturation": random.choice(date_formats),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.raw_utilisateur_abonement.append(abonement)
        return abonement

    def generate_raw_paiement(self) -> Dict[str, Any]:
        """Generate raw payment data"""
        if not self.raw_users or not self.raw_abonnements:
            return {}

        # Generate various payment methods
        payment_methods = [
            "CARTE_CREDIT",
            "VIREMENT",
            "carte crédit",  # Spaced and lowercase
            "Virement Bancaire",
            "PayPal",
            "Apple Pay",
            "Prélèvement"
        ]

        # Generate various payment statuses
        payment_statuses = [
            "REUSSI",
            "ECHOUE",
            "success",  # English
            "Failed",
            "En attente",
            "Annulé",
            "Remboursé"
        ]

        # Generate various date formats
        payment_date = fake.date_between(start_date='-1y', end_date='today')
        date_formats = [
            payment_date.strftime('%Y-%m-%d'),
            payment_date.strftime('%d/%m/%Y'),
            payment_date.strftime('%m-%d-%Y'),
            str(payment_date),
            payment_date.isoformat()
        ]

        # Generate various amount formats
        amount_formats = [
            round(random.uniform(5, 500), 2),
            f"{round(random.uniform(5, 500), 2)} €",
            f"EUR {round(random.uniform(5, 500), 2)}",
            str(round(random.uniform(5, 500), 2)).replace('.', ','),
            round(random.uniform(5, 500))  # No decimal
        ]
        
        paiement = {
            "raw_id": len(self.raw_paiements) + 1,
            "idPaiement": f"PAY{self.next_paiement_id:06}",
            "idAbonnement": random.choice([a["idAbonnement"] for a in self.raw_abonnements] + [None]),
            "userId": random.choice([u["userId"] for u in self.raw_users]),
            "methodePaiement": random.choice(payment_methods),
            "datePaiement": random.choice(date_formats),
            "montantPaiement": random.choice(amount_formats),
            "motifPaiement": random.choice([fake.sentence(), "Abonnement mensuel", "Renouvellement", ""]),
            "statusPaiement": random.choice(payment_statuses),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.next_paiement_id += 1
        self.raw_paiements.append(paiement)
        return paiement

    def generate_raw_methode_paiement(self) -> Dict[str, Any]:
        """Generate raw payment method data"""
        if not self.raw_users or not self.raw_paiements:
            return {}

        # Generate various card types
        card_types = [
            "CARTE_CREDIT",
            "VIREMENT",
            "Carte Bancaire",
            "VISA",
            "MasterCard",
            "AMEX",
            "PayPal"
        ]

        # Generate various expiration date formats
        exp_date = fake.date_between(start_date='today', end_date='+3y')
        date_formats = [
            exp_date.strftime('%m/%y'),
            exp_date.strftime('%m/%Y'),
            exp_date.strftime('%Y-%m'),
            exp_date.strftime('%d/%m/%Y'),
            str(exp_date)
        ]

        # Generate various card numbers
        card_number_formats = [
            fake.credit_card_number(),
            fake.credit_card_number().replace('-', ''),
            f"**** **** **** {fake.credit_card_number()[-4:]}",
            fake.credit_card_number()[:15] + 'X',  # Invalid
            f"{random.randint(1000, 9999)} XXXX XXXX {random.randint(1000, 9999)}"
        ]

        methode = {
            "raw_id": len(self.raw_methodes_paiement) + 1,
            "idPaiement": random.choice([p["idPaiement"] for p in self.raw_paiements]),
            "userId": random.choice([u["userId"] for u in self.raw_users]),
            "informationCarte": random.choice(card_number_formats),
            "dateExpirementCarte": random.choice(date_formats),
            "typeMethode": random.choice(card_types),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.raw_methodes_paiement.append(methode)
        return methode

    def generate_raw_utilisateur_notif(self) -> Dict[str, Any]:
        """Generate raw user-notification relationship data"""
        if not self.raw_users or not self.raw_notifications:
            return {}

        notif = {
            "raw_id": len(self.raw_utilisateur_notif) + 1,
            "userId": random.choice([u["userId"] for u in self.raw_users]),
            "idNotification": random.choice([n["idNotification"] for n in self.raw_notifications]),
            "estLu": random.choice([1, 0, "1", "0", "true", "false", "Oui", "Non"]),
            "estUrgent": random.choice([1, 0, None]),
            "date_extraction": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            "source_system": self._generate_source_system()
        }

        self.raw_utilisateur_notif.append(notif)
        return notif

    def generate_all_raw_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate all raw data according to specified volumes"""
        print("Generating raw users...")
        for _ in range(self.num_users):
            self.generate_raw_user()
            # Randomly create user types
            if random.random() < 0.7:  # 70% personal users
                self.generate_raw_utilisateur_personnel()
            elif random.random() < 0.2:  # 20% of remaining = 6% total admins
                self.generate_raw_admin()
            else:  # 24% supplier users
                self.generate_raw_utilisateur_fournisseur()

        print("Generating raw suppliers...")
        for _ in range(self.num_suppliers):
            self.generate_raw_fournisseur()

        print("Generating raw subscriptions...")
        for _ in range(self.num_subscriptions):
            self.generate_raw_abonnement()

        print("Generating raw user-subscription relationships...")
        for _ in range(int(self.num_subscriptions * 1.5)):  # More relationships than subscriptions
            self.generate_raw_utilisateur_abonement()

        print("Generating raw payments...")
        for _ in range(self.num_payments):
            self.generate_raw_paiement()

        print("Generating raw payment methods...")
        for _ in range(int(self.num_users * 0.8)):  # 80% of users have payment methods
            self.generate_raw_methode_paiement()

        print("Generating raw notifications...")
        for _ in range(self.num_notifications):
            self.generate_raw_notifications()

        print("Generating raw user-notification relationships...")
        for _ in range(int(self.num_notifications * 1.2)):  # More relationships than notifications
            self.generate_raw_utilisateur_notif()

        print("Generating raw journal entries...")
        for _ in range(self.num_journals):
            self.generate_raw_journal()

        return {
            "raw_User": self.raw_users,
            "raw_Utilisateur_personnel": self.raw_utilisateur_personnel,
            "raw_Admin": self.raw_admins,
            "raw_Utilisateur_fournisseur": self.raw_utilisateur_fournisseur,
            "raw_Fournisseur": self.raw_fournisseurs,
            "raw_Notifications": self.raw_notifications,
            "raw_Journal": self.raw_journals,
            "raw_Abonnement": self.raw_abonnements,
            "raw_Paiement": self.raw_paiements,
            "raw_MethodePaiement": self.raw_methodes_paiement,
            "raw_UtilisateurNotif": self.raw_utilisateur_notif,
            "raw_utilisateur_abonement": self.raw_utilisateur_abonement
        }

    def save_raw_to_json(self, output_path: str = "raw_data_output.json") -> str:
        """Save all generated raw data to JSON file"""
        data = self.generate_all_raw_data()

        # Ensure the directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Raw data successfully saved to {output_path}")
        return output_path

    def save_raw_to_csv(self, output_dir: str = "raw_data_csv") -> str:
        """Save each raw entity to a separate CSV file"""
        data = self.generate_all_raw_data()

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save each entity to its own CSV file
        for entity_name, entity_data in data.items():
            if entity_data:  # Only save non-empty entities
                df = pd.DataFrame(entity_data)
                output_path = os.path.join(output_dir, f"{entity_name}.csv")
                df.to_csv(output_path, index=False, encoding='utf-8')
                print(f"Saved {len(entity_data)} {entity_name} records to {output_path}")

        return output_dir

# Main execution
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Generate raw data for subscription management system')
    parser.add_argument('--users', type=int, default=50, help='Number of users to generate')
    parser.add_argument('--suppliers', type=int, default=20, help='Number of suppliers to generate')
    parser.add_argument('--subscriptions', type=int, default=100, help='Number of subscriptions to generate')
    parser.add_argument('--payments', type=int, default=200, help='Number of payments to generate')
    parser.add_argument('--notifications', type=int, default=300, help='Number of notifications to generate')
    parser.add_argument('--journals', type=int, default=400, help='Number of journal entries to generate')
    parser.add_argument('--output-json', type=str, default="raw_data_output.json", help='Output JSON file path')
    parser.add_argument('--output-csv-dir', type=str, default="raw_data_csv", help='Output CSV directory')
    parser.add_argument('--format', choices=['json', 'csv', 'both'], default='both', help='Output format')

    args = parser.parse_args()

    # Initialize the generator with command line arguments
    generator = RawDataGenerator(
        num_users=args.users,
        num_suppliers=args.suppliers,
        num_subscriptions=args.subscriptions,
        num_payments=args.payments,
        num_notifications=args.notifications,
        num_journals=args.journals
    )

    # Generate and save data in the specified format(s)
    if args.format in ['json', 'both']:
        json_path = generator.save_raw_to_json(args.output_json)
    if args.format in ['csv', 'both']:
        csv_dir = generator.save_raw_to_csv(args.output_csv_dir)

    print("\nRaw data generation complete!")
    if args.format in ['json', 'both']:
        print(f"JSON data: {json_path}")
    if args.format in ['csv', 'both']:
        print(f"CSV data: {csv_dir}")