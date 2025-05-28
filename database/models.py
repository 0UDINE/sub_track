from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, List
from enum import Enum

# Enums for constrained values
class RoleType(str, Enum):
    USER = "USER"
    ADMIN = "ADMIN"
    SUPPLIER = "SUPPLIER"

class SectorType(str, Enum):
    INDUSTRY = "Industrie"
    AGRICULTURE = "Agriculture"
    TECHNOLOGY = "Technologie"
    HEALTH = "Santé"
    CONSTRUCTION = "Construction"
    TRANSPORT = "Transport"
    COMMERCE = "Commerce"

class SubscriptionCategory(str, Enum):
    STREAMING = "STREAMING"
    MUSIC = "MUSIC"
    CLOUD = "CLOUD"
    CALL_PLAN = "FORFAIT APPEL CONX"

class NotificationType(str, Enum):
    PAYMENT_REMINDER = "PAYMENT_REMINDER"
    SUBSCRIPTION_EXPIRY = "SUBSCRIPTION_EXPIRY"
    PAYMENT_CONFIRMATION = "PAYMENT_CONFIRMATION"
    SUBSCRIPTION_RENEWAL = "SUBSCRIPTION_RENEWAL"
    PRICE_CHANGE = "PRICE_CHANGE"
    SYSTEM_ALERT = "SYSTEM_ALERT"

class ActivityStatus(str, Enum):
    CREATION = "CREATION_ABONEMMENT"
    CANCELLATION = "ANNULATION_ABONEMMENT"
    PAYMENT_FAILED = "ECHOUE_PAIEMENT"
    PAYMENT_SUCCESS = "PAIMENT_SUCCCES"
    PRICE_CHANGE = "PRIX_ABONEMENT_CHANGE"

class SubscriptionStatus(str, Enum):
    ACTIVE = "ACTIF"
    EXPIRED = "EXPIRE"
    CANCELLED = "ANNULE"

class BillingType(str, Enum):
    MONTHLY = "MENSUAL"
    YEARLY = "ANNUEL"
    DAILY = "QUOTIDIEN"
    WEEKLY = "HEBDOMADAIRE"

class PaymentStatus(str, Enum):
    SUCCESS = "REUSSI"
    FAILED = "ECHOUE"

class PaymentMethodType(str, Enum):
    CREDIT_CARD = "CARTE_CREDIT"
    TRANSFER = "VIREMENT"

# Raw Database Models
@dataclass
class RawUser:
    raw_id: Optional[int] = None
    user_id: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    extraction_date: Optional[datetime] = None
    source_system: Optional[str] = None
    data_quality_flag: Optional[bool] = None

@dataclass
class RawSubscription:
    raw_id: Optional[int] = None
    subscription_id: Optional[str] = None
    supplier_id: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[str] = None
    category: Optional[str] = None
    extraction_date: Optional[datetime] = None
    source_system: Optional[str] = None

# Processed Database Models
@dataclass
class ProcessedUser:
    user_id: str
    email: str
    password: str
    role: RoleType

@dataclass
class ProcessedPersonalUser(ProcessedUser):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

@dataclass
class ProcessedSupplier:
    supplier_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    sector: Optional[SectorType] = None

@dataclass
class ProcessedSubscription:
    subscription_id: str
    supplier_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    category: Optional[SubscriptionCategory] = None

@dataclass
class UserSubscription:
    user_id: str
    subscription_id: str
    billing_type: BillingType
    status: SubscriptionStatus
    renewal_date: Optional[date] = None
    billing_start_date: Optional[date] = None
    next_billing_date: Optional[date] = None

@dataclass
class ProcessedPayment:
    payment_id: str
    subscription_id: str
    user_id: str
    method: Optional[str] = None
    date: Optional[date] = None
    amount: Optional[float] = None
    reason: Optional[str] = None
    status: Optional[PaymentStatus] = None