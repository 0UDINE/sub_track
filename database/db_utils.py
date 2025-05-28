import pyodbc
from typing import List, Optional
from .config import CONNECTION_STRING
from .models import *


class DatabaseManager:
    def __init__(self, database: str = "Processed_Subscription_Data_DB"):
        self.conn = pyodbc.connect(
            CONNECTION_STRING.replace("Database=SubscriptionDB", f"Database={database}"),
            autocommit=False
        )
        self.cursor = self.conn.cursor()

    def create_raw_tables(self):
        """Create all tables for raw data database"""
        try:
            # Create raw user table
            self.cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='raw_User')
            CREATE TABLE raw_User (
                raw_id INT IDENTITY(1,1) PRIMARY KEY,
                userId NVARCHAR(100),
                email NVARCHAR(255),
                motsDePasse NVARCHAR(255),
                role NVARCHAR(100),
                date_extraction DATETIME DEFAULT GETDATE(),
                source_system NVARCHAR(100),
                data_quality_flag BIT DEFAULT 0
            )
            """)

            # Create other raw tables similarly...
            # (Include all your raw table creation scripts here)

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e

    def create_processed_tables(self):
        """Create all tables for processed data database with constraints"""
        try:
            # Processed User with constraints
            self.cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='processed_User')
            CREATE TABLE processed_User (
                userId VARCHAR(15) PRIMARY KEY,
                email NVARCHAR(255) NOT NULL,
                motsDePasse NVARCHAR(255) NOT NULL,
                role NVARCHAR(50) NOT NULL
            )
            """)

            self.cursor.execute("""
            ALTER TABLE processed_User
            ADD CONSTRAINT CHK_valid_email CHECK (email LIKE '%_@__%.__%')
            """)

            # Create other processed tables with constraints...
            # (Include all your processed table creation scripts with constraints)

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e

    def insert_processed_user(self, user: ProcessedUser):
        """Insert a processed user record with validation"""
        try:
            self.cursor.execute("""
            INSERT INTO processed_User (userId, email, motsDePasse, role)
            VALUES (?, ?, ?, ?)
            """, (user.user_id, user.email, user.password, user.role.value))
            self.conn.commit()
            return True
        except pyodbc.IntegrityError as e:
            self.conn.rollback()
            raise ValueError(f"Data validation failed: {str(e)}")
        except Exception as e:
            self.conn.rollback()
            raise e

    def get_user_subscriptions(self, user_id: str) -> List[UserSubscription]:
        """Retrieve all subscriptions for a user"""
        self.cursor.execute("""
        SELECT * FROM utilisateur_abonement
        WHERE userId = ?
        """, user_id)

        subscriptions = []
        for row in self.cursor.fetchall():
            subs = UserSubscription(
                user_id=row.userId,
                subscription_id=row.idAbonnement,
                billing_type=BillingType(row.typeFacturation),
                status=SubscriptionStatus(row.statutAbonnement),
                renewal_date=row.dateRenouvelement,
                billing_start_date=row.dateDebutFacturation,
                next_billing_date=row.prochaineDeFacturation
            )
            subscriptions.append(subs)
        return subscriptions

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()