"""
FRAUDGUARD BANK - COMPLETE FRAUD DETECTION SYSTEM (FIXED)
===========================================================
Frontend issue fixed: HTML properly escaped and complete
"""

# ============================================================
# PART 1: IMPORTS & CONFIGURATION
# ============================================================

import tensorflow as tf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Annotated
from collections import deque, defaultdict
from dataclasses import dataclass, asdict
from enum import Enum
import json
import pickle
import os
import asyncio
import threading
import time
import uuid
import hashlib
import re
import random

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, field_validator
from pydantic.functional_validators import AfterValidator
import uvicorn

# For SMS/Email
try:
    from twilio.rest import Client as TwilioClient
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False

# ============================================================
# CONFIGURATION
# ============================================================

class BankConfig:
    BANK_NAME = "FraudGuard Bank"
    BANK_CODE = "FGB-001"
    TWILIO_SID = os.getenv("TWILIO_SID", "your_twilio_sid")
    TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "your_twilio_token")
    TWILIO_PHONE = os.getenv("TWILIO_PHONE", "+1234567890")
    SENDGRID_KEY = os.getenv("SENDGRID_KEY", "your_sendgrid_key")
    FROM_EMAIL = "alerts@fraudguardbank.com"
    BLOCK_THRESHOLD = 0.85
    REVIEW_THRESHOLD = 0.60
    ALERT_THRESHOLD = 0.40
    HIGH_RISK_COUNTRIES = ['NG', 'RU', 'CN', 'KP', 'IR']

config = BankConfig()

# ============================================================
# VALIDATORS (Pydantic v2 compatible)
# ============================================================

def validate_phone_number(v: str) -> str:
    if not re.match(r'^\+?[1-9]\d{1,14}$', v):
        raise ValueError('Invalid phone number format. Must be E.164 format (e.g., +14155551234)')
    return v

def validate_account_number(v: str) -> str:
    if not v.isdigit():
        raise ValueError('Account number must contain only digits')
    return v

def validate_card_last_four(v: str) -> str:
    if not v.isdigit() or len(v) != 4:
        raise ValueError('Card last four must be exactly 4 digits')
    return v

# ============================================================
# DATA MODELS
# ============================================================

class AccountType(str, Enum):
    SAVINGS = "savings"
    CHECKING = "checking"
    CREDIT = "credit"
    BUSINESS = "business"

class CardType(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"
    PREPAID = "prepaid"

class TransactionChannel(str, Enum):
    ATM = "atm"
    POS = "pos"
    ONLINE = "online"
    MOBILE = "mobile"
    WIRE = "wire_transfer"
    BRANCH = "branch"

class AlertChannel(str, Enum):
    SMS = "sms"
    EMAIL = "email"
    PUSH = "push"
    PHONE = "phone_call"

class CustomerProfile(BaseModel):
    customer_id: str
    full_name: str
    phone_number: Annotated[str, AfterValidator(validate_phone_number)]
    email: str
    address: str
    date_of_birth: str
    kyc_verified: bool = True
    risk_tier: str = "low"

class BankAccount(BaseModel):
    account_number: Annotated[str, Field(min_length=10, max_length=20), AfterValidator(validate_account_number)]
    account_type: AccountType
    customer_id: str
    balance: float = 0.0
    currency: str = "USD"
    status: str = "active"
    daily_limit: float = 10000.0
    monthly_limit: float = 50000.0
    opened_date: str
    branch_code: str

class BankCard(BaseModel):
    card_number_token: str
    card_last_four: Annotated[str, Field(min_length=4, max_length=4), AfterValidator(validate_card_last_four)]
    card_type: CardType
    account_number: str
    customer_id: str
    expiry_month: int = Field(..., ge=1, le=12)
    expiry_year: int = Field(..., ge=2024, le=2035)
    cvv_token: str
    status: str = "active"
    daily_limit: float = 5000.0
    contactless_enabled: bool = True
    online_enabled: bool = True
    international_enabled: bool = False

class TransactionSlip(BaseModel):
    slip_id: str
    transaction_id: str
    merchant_name: str
    merchant_id: str
    merchant_address: str
    merchant_city: str
    merchant_country: str
    merchant_category_code: str
    terminal_id: Optional[str] = None
    atm_location: Optional[str] = None
    receipt_number: str
    authorization_code: str
    transaction_time: str
    posted_time: Optional[str] = None

class TransactionRequest(BaseModel):
    transaction_id: Optional[str] = None
    account_number: Optional[str] = None
    card_last_four: Optional[str] = None
    customer_id: str
    amount: float = Field(..., gt=0)
    currency: str = "USD"
    merchant_name: str
    merchant_id: str
    merchant_category: str
    channel: TransactionChannel
    merchant_address: str
    merchant_city: str
    merchant_country: str
    merchant_latitude: Optional[float] = None
    merchant_longitude: Optional[float] = None
    terminal_id: Optional[str] = None
    atm_id: Optional[str] = None
    ip_address: Optional[str] = None
    device_id: Optional[str] = None
    transaction_time: str
    timezone: str = "UTC"
    card_present: int = Field(..., ge=0, le=1)
    pin_entered: int = Field(0, ge=0, le=1)
    signature_verified: int = Field(0, ge=0, le=1)
    velocity_1h: int = 0
    velocity_24h: int = 0
    amount_24h: float = 0.0
    is_night: int = 0
    is_weekend: int = 0
    is_international: int = 0
    slip_id: Optional[str] = None
    authorization_code: Optional[str] = None

# ============================================================
# BANK CORE SYSTEM
# ============================================================

class BankCoreSystem:
    def __init__(self):
        self.customers: Dict[str, CustomerProfile] = {}
        self.accounts: Dict[str, BankAccount] = {}
        self.cards: Dict[str, BankCard] = {}
        self.transactions: deque = deque(maxlen=100000)
        self.alerts: deque = deque(maxlen=10000)
        self._seed_data()
    
    def _seed_data(self):
        sample_customers = [
            {
                "customer_id": "CUST-001",
                "name": "John Smith",
                "phone": "+14155551234",
                "email": "john.smith@email.com",
                "accounts": [
                    {"number": "1000456789", "type": "checking", "balance": 15000.00},
                    {"number": "1000456790", "type": "savings", "balance": 45000.00}
                ],
                "cards": [
                    {"last_four": "4532", "type": "debit", "account": "1000456789"},
                    {"last_four": "7890", "type": "credit", "account": "1000456790"}
                ]
            },
            {
                "customer_id": "CUST-002",
                "name": "Sarah Johnson",
                "phone": "+14155555678",
                "email": "sarah.j@email.com",
                "accounts": [
                    {"number": "1000567890", "type": "checking", "balance": 8200.00}
                ],
                "cards": [
                    {"last_four": "1234", "type": "debit", "account": "1000567890"}
                ]
            }
        ]
        
        for cust_data in sample_customers:
            self.customers[cust_data["customer_id"]] = CustomerProfile(
                customer_id=cust_data["customer_id"],
                full_name=cust_data["name"],
                phone_number=cust_data["phone"],
                email=cust_data["email"],
                address="123 Main St, New York, NY",
                date_of_birth="1985-03-15",
                kyc_verified=True
            )
            
            for acc in cust_data.get("accounts", []):
                self.accounts[acc["number"]] = BankAccount(
                    account_number=acc["number"],
                    account_type=acc["type"],
                    customer_id=cust_data["customer_id"],
                    balance=acc["balance"],
                    opened_date="2020-01-15",
                    branch_code="NYC-001"
                )
            
            for card in cust_data.get("cards", []):
                token = self._tokenize_card(f"4532{card['last_four']}8901{card['last_four']}")
                self.cards[token] = BankCard(
                    card_number_token=token,
                    card_last_four=card["last_four"],
                    card_type=card["type"],
                    account_number=card["account"],
                    customer_id=cust_data["customer_id"],
                    expiry_month=12,
                    expiry_year=2027,
                    cvv_token="tok_cvv_123",
                    daily_limit=5000.0 if card["type"] == "debit" else 10000.0
                )
    
    def _tokenize_card(self, card_number: str) -> str:
        return hashlib.sha256(f"{card_number}{config.BANK_CODE}".encode()).hexdigest()[:32]
    
    def get_customer_by_account(self, account_number: str) -> Optional[CustomerProfile]:
        account = self.accounts.get(account_number)
        if account:
            return self.customers.get(account.customer_id)
        return None
    
    def get_customer_by_card(self, card_last_four: str) -> Optional[Tuple[CustomerProfile, BankCard]]:
        for token, card in self.cards.items():
            if card.card_last_four == card_last_four:
                customer = self.customers.get(card.customer_id)
                return customer, card
        return None, None
    
    def get_account_history(self, account_number: str, hours: int = 24) -> List[Dict]:
        cutoff = datetime.now() - timedelta(hours=hours)
        history = []
        for tx in self.transactions:
            if tx.get('account_number') == account_number:
                tx_time = datetime.fromisoformat(tx.get('timestamp', '2000-01-01'))
                if tx_time > cutoff:
                    history.append(tx)
        return history
    
    def get_card_history(self, card_last_four: str, hours: int = 24) -> List[Dict]:
        cutoff = datetime.now() - timedelta(hours=hours)
        history = []
        for tx in self.transactions:
            if tx.get('card_last_four') == card_last_four:
                tx_time = datetime.fromisoformat(tx.get('timestamp', '2000-01-01'))
                if tx_time > cutoff:
                    history.append(tx)
        return history
    
    def freeze_account(self, account_number: str, reason: str):
        if account_number in self.accounts:
            self.accounts[account_number].status = "frozen"
            print(f"🚨 ACCOUNT FROZEN: {account_number} - Reason: {reason}")
    
    def block_card(self, card_token: str, reason: str):
        if card_token in self.cards:
            self.cards[card_token].status = "blocked"
            print(f"💳 CARD BLOCKED: ****{self.cards[card_token].card_last_four} - Reason: {reason}")

bank_core = BankCoreSystem()

# ============================================================
# NOTIFICATION SYSTEM
# ============================================================

class NotificationService:
    def __init__(self):
        self.twilio = None
        self.sendgrid = None
        
        if TWILIO_AVAILABLE and config.TWILIO_SID != "your_twilio_sid":
            self.twilio = TwilioClient(config.TWILIO_SID, config.TWILIO_TOKEN)
        
        if SENDGRID_AVAILABLE and config.SENDGRID_KEY != "your_sendgrid_key":
            self.sendgrid = SendGridAPIClient(config.SENDGRID_KEY)
        
        self.sent_alerts = deque(maxlen=10000)
    
    def send_sms(self, to_phone: str, message: str) -> bool:
        try:
            if self.twilio:
                self.twilio.messages.create(body=message, from_=config.TWILIO_PHONE, to=to_phone)
            
            self.sent_alerts.append({
                'channel': 'sms', 'to': to_phone, 'message': message,
                'timestamp': datetime.now().isoformat(), 'status': 'sent'
            })
            return True
        except Exception as e:
            self.sent_alerts.append({
                'channel': 'sms', 'to': to_phone, 'message': message,
                'timestamp': datetime.now().isoformat(), 'status': 'failed', 'error': str(e)
            })
            return False
    
    def send_email(self, to_email: str, subject: str, html_content: str) -> bool:
        try:
            if self.sendgrid:
                message = Mail(from_email=config.FROM_EMAIL, to_emails=to_email, subject=subject, html_content=html_content)
                self.sendgrid.send(message)
            
            self.sent_alerts.append({
                'channel': 'email', 'to': to_email, 'subject': subject,
                'timestamp': datetime.now().isoformat(), 'status': 'sent'
            })
            return True
        except Exception as e:
            return False
    
    def send_fraud_alert(self, customer: CustomerProfile, transaction: Dict, fraud_prob: float, recommendation: str) -> Dict:
        channels_sent = []
        merchant = transaction.get('merchant_name', 'Unknown')
        amount = transaction.get('amount', 0)
        currency = transaction.get('currency', 'USD')
        location = f"{transaction.get('merchant_city', '')}, {transaction.get('merchant_country', '')}"
        time_str = transaction.get('transaction_time', 'Unknown')
        
        sms_body = (
            f"FraudGuard Bank ALERT: Suspicious transaction detected.\n"
            f"Amount: {currency} {amount:,.2f} at {merchant}\n"
            f"Location: {location}\n"
            f"If NOT you, reply BLOCK. If you, reply CONFIRM.\n"
            f"Call 1-800-FRAUD-01."
        )
        
        email_subject = "🚨 Fraud Alert: Suspicious Transaction Detected"
        email_html = f"""
        <html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #dc2626; color: white; padding: 20px; text-align: center;"><h1>🚨 FRAUD ALERT</h1></div>
        <div style="padding: 20px; background: #f9fafb;">
        <p>Dear {customer.full_name},</p><p>We detected a suspicious transaction:</p>
        <div style="background: white; padding: 15px; border-radius: 8px; margin: 15px 0;">
        <p><strong>Amount:</strong> {currency} {amount:,.2f}</p>
        <p><strong>Merchant:</strong> {merchant}</p>
        <p><strong>Location:</strong> {location}</p>
        <p><strong>Risk Score:</strong> {fraud_prob*100:.1f}%</p>
        </div>
        <p><strong>Was this you?</strong></p>
        <div style="text-align: center; margin: 20px 0;">
        <a href="#" style="background: #16a34a; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; margin-right: 10px;">✅ YES</a>
        <a href="#" style="background: #dc2626; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px;">❌ NO</a>
        </div>
        </div></body></html>
        """
        
        if self.send_sms(customer.phone_number, sms_body):
            channels_sent.append(AlertChannel.SMS)
        if self.send_email(customer.email, email_subject, email_html):
            channels_sent.append(AlertChannel.EMAIL)
        
        return {
            'channels_sent': channels_sent,
            'sms_preview': sms_body[:100] + "...",
            'email_sent': AlertChannel.EMAIL in channels_sent,
            'sms_sent': AlertChannel.SMS in channels_sent
        }

notification_service = NotificationService()

# ============================================================
# SLIP VERIFICATION
# ============================================================

class SlipVerificationEngine:
    def __init__(self):
        self.merchant_database = self._load_merchants()
        self.verified_slips = deque(maxlen=50000)
    
    def _load_merchants(self) -> Dict:
        return {
            "MERCH-001": {
                "name": "Walmart Supercenter", "category": "Retail", "mcc": "5311",
                "address": "100 Main St", "city": "New York", "country": "US",
                "lat": 40.7128, "lon": -74.0060, "terminals": ["TERM-NYC-001", "TERM-NYC-002"]
            },
            "MERCH-002": {
                "name": "Shell Gas Station", "category": "Fuel", "mcc": "5541",
                "address": "45 Highway Ave", "city": "Los Angeles", "country": "US",
                "lat": 34.0522, "lon": -118.2437, "terminals": ["TERM-LA-001"]
            },
            "MERCH-999": {
                "name": "DarkWeb Electronics", "category": "Electronics", "mcc": "5732",
                "address": "Unknown", "city": "Moscow", "country": "RU",
                "lat": 55.7558, "lon": 37.6173, "terminals": ["TERM-SUS-001"], "high_risk": True
            }
        }
    
    def verify_slip(self, slip: TransactionSlip) -> Dict:
        result = {
            'slip_id': slip.slip_id, 'verified': False, 'merchant_match': False,
            'terminal_valid': False, 'location_consistent': False,
            'red_flags': [], 'risk_score': 0.0
        }
        
        merchant = self.merchant_database.get(slip.merchant_id)
        
        if not merchant:
            result['red_flags'].append("UNKNOWN_MERCHANT")
            result['risk_score'] += 0.3
        else:
            result['merchant_match'] = True
            if merchant.get('high_risk'):
                result['red_flags'].append("HIGH_RISK_MERCHANT")
                result['risk_score'] += 0.4
            
            if slip.terminal_id and slip.terminal_id in merchant.get('terminals', []):
                result['terminal_valid'] = True
            elif slip.terminal_id:
                result['red_flags'].append("INVALID_TERMINAL")
                result['risk_score'] += 0.2
            
            if slip.merchant_country != merchant['country']:
                result['red_flags'].append("COUNTRY_MISMATCH")
                result['risk_score'] += 0.3
        
        if slip.authorization_code:
            if not re.match(r'^[A-Z0-9]{6,8}$', slip.authorization_code):
                result['red_flags'].append("INVALID_AUTH_CODE")
                result['risk_score'] += 0.2
        
        result['verified'] = len(result['red_flags']) == 0
        self.verified_slips.append(result)
        return result

slip_engine = SlipVerificationEngine()

# ============================================================
# FRAUD MODEL
# ============================================================

class BankFraudModel:
    def __init__(self):
        self.model = self._build_model()
        self.version = "v3.0.0-bank-fraud"
    
    def _build_model(self):
        inputs = tf.keras.Input(shape=(20,))
        x = tf.keras.layers.Dense(128, activation='relu')(inputs)
        x = tf.keras.layers.Dropout(0.3)(x)
        x = tf.keras.layers.Dense(64, activation='relu')(x)
        x = tf.keras.layers.Dense(32, activation='relu')(x)
        outputs = tf.keras.layers.Dense(1, activation='sigmoid')(x)
        
        model = tf.keras.Model(inputs, outputs)
        model.compile(optimizer='adam', loss='binary_crossentropy')
        return model
    
    def predict(self, features: np.ndarray) -> float:
        if len(features.shape) == 1:
            features = np.expand_dims(features, 0)
        return float(self.model.predict(features, verbose=0)[0][0])
    
    def extract_features(self, tx: TransactionRequest, customer: CustomerProfile,
                        account: Optional[BankAccount], card: Optional[BankCard],
                        slip_result: Dict, history: List[Dict]) -> np.ndarray:
        
        account_balance = account.balance if account else 0
        balance_ratio = tx.amount / max(account_balance, 1)
        
        velocity_1h = len([h for h in history if 
            (datetime.now() - datetime.fromisoformat(h.get('timestamp', '2000-01-01'))).seconds < 3600])
        
        amounts = [h.get('amount', 0) for h in history[-30:]]
        avg_amount = np.mean(amounts) if amounts else 100
        amount_zscore = (tx.amount - avg_amount) / max(np.std(amounts), 1) if amounts else 0
        
        geo_risk = 0.0
        if tx.merchant_country in config.HIGH_RISK_COUNTRIES:
            geo_risk += 0.5
        if tx.is_international:
            geo_risk += 0.3
        
        slip_risk = slip_result.get('risk_score', 0)
        
        tx_hour = datetime.fromisoformat(tx.transaction_time).hour
        is_night = 1 if tx_hour < 6 or tx_hour > 23 else 0
        
        channel_risk = {
            'online': 0.3, 'mobile': 0.2, 'pos': 0.1,
            'atm': 0.15, 'wire': 0.4, 'branch': 0.0
        }.get(tx.channel.value, 0.1)
        
        features = [
            tx.amount / 1000, balance_ratio, velocity_1h / 10, len(history) / 100,
            amount_zscore, geo_risk, slip_risk, is_night, tx.card_present,
            1 - tx.pin_entered, channel_risk, tx.velocity_24h / 50,
            tx.amount_24h / 10000,
            1 if account and account.status == "frozen" else 0,
            1 if card and card.status == "blocked" else 0,
            1 if customer.risk_tier == "high" else 0,
            1 if not customer.kyc_verified else 0,
            tx.is_international, 1 if tx.ip_address else 0,
            random.random() * 0.1
        ]
        
        return np.array(features, dtype=np.float32)

fraud_model = BankFraudModel()

# ============================================================
# MAIN FRAUD ENGINE
# ============================================================

class BankFraudEngine:
    def __init__(self):
        self.model = fraud_model
        self.bank = bank_core
        self.notifier = notification_service
        self.slip_verifier = slip_engine
        self.active_websockets = []
        self.transaction_log = deque(maxlen=100000)
    
    async def process_transaction(self, tx: TransactionRequest) -> Dict:
        start_time = time.perf_counter()
        tx_id = tx.transaction_id or f"TXN-{uuid.uuid4().hex[:12].upper()}"
        
        customer = self.bank.customers.get(tx.customer_id)
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        account = self.bank.accounts.get(tx.account_number) if tx.account_number else None
        card = None
        if tx.card_last_four:
            _, card = self.bank.get_customer_by_card(tx.card_last_four)
        
        history = []
        if tx.account_number:
            history = self.bank.get_account_history(tx.account_number, 24)
        elif tx.card_last_four:
            history = self.bank.get_card_history(tx.card_last_four, 24)
        
        slip_result = {'verified': True, 'risk_score': 0.0, 'red_flags': []}
        if tx.slip_id:
            slip = TransactionSlip(
                slip_id=tx.slip_id, transaction_id=tx_id, merchant_name=tx.merchant_name,
                merchant_id=tx.merchant_id, merchant_address=tx.merchant_address,
                merchant_city=tx.merchant_city, merchant_country=tx.merchant_country,
                merchant_category_code=tx.merchant_category, terminal_id=tx.terminal_id,
                receipt_number=tx.slip_id, authorization_code=tx.authorization_code or "AUTH000",
                transaction_time=tx.transaction_time
            )
            slip_result = self.slip_verifier.verify_slip(slip)
        
        features = self.model.extract_features(tx, customer, account, card, slip_result, history)
        fraud_prob = self.model.predict(features)
        
        if fraud_prob > config.BLOCK_THRESHOLD:
            action, status, alert_triggered = "BLOCK", "blocked", True
            if account:
                self.bank.freeze_account(account.account_number, f"Fraud: {fraud_prob:.2f}")
            if card:
                self.bank.block_card(card.card_number_token, f"Fraud: {fraud_prob:.2f}")
        elif fraud_prob > config.REVIEW_THRESHOLD:
            action, status, alert_triggered = "REVIEW", "under_review", True
        elif fraud_prob > config.ALERT_THRESHOLD:
            action, status, alert_triggered = "ALERT", "approved_with_alert", True
        else:
            action, status, alert_triggered = "APPROVE", "approved", False
        
        result = {
            'transaction_id': tx_id, 'account_number': tx.account_number,
            'card_last_four': tx.card_last_four, 'customer_id': tx.customer_id,
            'customer_name': customer.full_name, 'amount': tx.amount,
            'currency': tx.currency, 'merchant_name': tx.merchant_name,
            'merchant_location': f"{tx.merchant_city}, {tx.merchant_country}",
            'channel': tx.channel.value, 'fraud_probability': round(fraud_prob, 4),
            'risk_score': round(fraud_prob * 100, 1), 'action': action,
            'status': status, 'alert_triggered': alert_triggered,
            'slip_verification': slip_result, 'velocity_1h': len(history),
            'account_balance_after': account.balance - tx.amount if account and status == "approved" else account.balance,
            'processing_time_ms': round((time.perf_counter() - start_time) * 1000, 2),
            'timestamp': datetime.now().isoformat(), 'model_version': self.model.version
        }
        
        self.bank.transactions.append(result)
        self.transaction_log.append(result)
        
        if alert_triggered:
            notification_result = self.notifier.send_fraud_alert(customer, result, fraud_prob, action)
            result['notification'] = notification_result
        
        await self._broadcast(result)
        return result
    
    async def _broadcast(self, message: Dict):
        for ws in self.active_websockets[:]:
            try:
                await ws.send_json(message)
            except:
                self.active_websockets.remove(ws)
    
    def search_transactions(self, query: str, search_type: str = "all") -> List[Dict]:
        results = []
        query = query.strip()
        for tx in self.transaction_log:
            match = False
            if search_type in ["all", "account"] and tx.get('account_number'):
                if query in tx['account_number']: match = True
            if search_type in ["all", "card"] and tx.get('card_last_four'):
                if query == tx['card_last_four']: match = True
            if search_type in ["all", "txn"] and tx.get('transaction_id'):
                if query.upper() in tx['transaction_id'].upper(): match = True
            if search_type in ["all", "customer"] and tx.get('customer_id'):
                if query.upper() in tx['customer_id'].upper(): match = True
            if match: results.append(tx)
        return results[-100:]

bank_fraud_engine = BankFraudEngine()

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(title="FraudGuard Bank API", description="Real-time fraud detection", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    bank_fraud_engine.active_websockets.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        bank_fraud_engine.active_websockets.remove(websocket)

@app.post("/api/transaction/process")
async def process_transaction(request: TransactionRequest):
    return await bank_fraud_engine.process_transaction(request)

@app.get("/api/transaction/search")
async def search_transactions(query: str, search_type: str = "all"):
    return {
        "query": query, "search_type": search_type,
        "results": bank_fraud_engine.search_transactions(query, search_type),
        "total_found": len(bank_fraud_engine.search_transactions(query, search_type))
    }

@app.get("/api/account/{account_number}")
async def get_account_info(account_number: str):
    account = bank_core.accounts.get(account_number)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    customer = bank_core.customers.get(account.customer_id)
    history = bank_core.get_account_history(account_number, 168)
    return {
        "account": account.model_dump(),
        "customer": customer.model_dump() if customer else None,
        "recent_transactions": history[-20:],
        "fraud_alerts_count": len([h for h in history if h.get('alert_triggered')])
    }

@app.get("/api/card/{last_four}")
async def get_card_info(last_four: str):
    customer, card = bank_core.get_customer_by_card(last_four)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    history = bank_core.get_card_history(last_four, 168)
    return {
        "card": card.model_dump(),
        "customer": customer.model_dump() if customer else None,
        "recent_transactions": history[-20:],
        "fraud_alerts_count": len([h for h in history if h.get('alert_triggered')])
    }

@app.post("/api/slip/verify")
async def verify_slip(slip: TransactionSlip):
    return slip_engine.verify_slip(slip)

@app.get("/api/stats")
async def get_stats():
    return {
        "total_transactions": len(bank_fraud_engine.transaction_log),
        "fraud_detected": sum(1 for t in bank_fraud_engine.transaction_log if t.get('alert_triggered')),
        "accounts_monitored": len(bank_core.accounts),
        "cards_monitored": len(bank_core.cards),
        "customers": len(bank_core.customers),
        "alerts_sent": len(notification_service.sent_alerts),
        "slips_verified": len(slip_engine.verified_slips)
    }

# ============================================================
# FRONTEND - FIXED COMPLETE HTML
# ============================================================

# We build the HTML as a separate string to avoid truncation issues
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FraudGuard Bank - Transaction Security Center</title>
    <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
        body { font-family: 'Inter', sans-serif; background: #0a0e1a; color: #e2e8f0; }
        .mono { font-family: 'JetBrains Mono', monospace; }
        .glass { background: rgba(15, 23, 42, 0.8); backdrop-filter: blur(12px); border: 1px solid rgba(56, 189, 248, 0.1); }
        .glow-blue { box-shadow: 0 0 20px rgba(56, 189, 248, 0.15); }
        .glow-red { box-shadow: 0 0 20px rgba(239, 68, 68, 0.15); }
        .slide-in { animation: slideIn 0.3s ease-out; }
        @keyframes slideIn { from { transform: translateX(-20px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .live-dot { width: 8px; height: 8px; background: #22c55e; border-radius: 50%; display: inline-block; animation: blink 1.5s infinite; }
        @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
        .receipt-pattern { background-image: repeating-linear-gradient(45deg, transparent, transparent 10px, rgba(255,255,255,0.03) 10px, rgba(255,255,255,0.03) 20px); }
    </style>
</head>
<body>
    <div id="root"></div>
    <script type="text/babel">
        const { useState, useEffect, useRef } = React;
        
        const App = () => {
            const [activeTab, setActiveTab] = useState('process');
            const [stats, setStats] = useState({});
            const [wsConnected, setWsConnected] = useState(false);
            
            useEffect(() => {
                fetchStats();
                const interval = setInterval(fetchStats, 5000);
                const ws = new WebSocket(`ws://${window.location.host}/ws`);
                ws.onopen = () => setWsConnected(true);
                ws.onclose = () => setWsConnected(false);
                return () => { clearInterval(interval); ws.close(); };
            }, []);
            
            const fetchStats = async () => {
                try {
                    const res = await fetch('/api/stats');
                    const data = await res.json();
                    setStats(data);
                } catch(e) {}
            };
            
            return (
                <div className="min-h-screen bg-gradient-to-br from-slate-950 via-blue-950/10 to-slate-950">
                    <header className="glass sticky top-0 z-50 border-b border-cyan-500/20">
                        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="w-12 h-12 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl flex items-center justify-center glow-blue">
                                    <i className="fas fa-university text-white text-xl"></i>
                                </div>
                                <div>
                                    <h1 className="text-2xl font-bold text-white tracking-tight">
                                        FraudGuard <span className="text-cyan-400">Bank</span>
                                    </h1>
                                    <p className="text-xs text-cyan-400/70 mono">TRANSACTION SECURITY CENTER v3.0</p>
                                </div>
                            </div>
                            <div className="flex items-center gap-6">
                                <div className="flex items-center gap-2">
                                    <span className="live-dot"></span>
                                    <span className="text-sm text-green-400">{wsConnected ? 'SYSTEM ONLINE' : 'OFFLINE'}</span>
                                </div>
                                <div className="text-right">
                                    <p className="text-xs text-slate-500">TXNs Monitored</p>
                                    <p className="text-xl font-bold text-cyan-400 mono">{stats.total_transactions || 0}</p>
                                </div>
                                <div className="text-right">
                                    <p className="text-xs text-slate-500">Alerts Sent</p>
                                    <p className="text-xl font-bold text-yellow-400 mono">{stats.alerts_sent || 0}</p>
                                </div>
                            </div>
                        </div>
                    </header>
                    
                    <nav className="max-w-7xl mx-auto px-6 py-4">
                        <div className="flex gap-1 bg-slate-900/50 rounded-lg p-1 inline-flex">
                            {[
                                {id: 'process', icon: 'fa-credit-card', label: 'Process TXN'},
                                {id: 'search', icon: 'fa-search', label: 'Search & Trace'},
                                {id: 'slip', icon: 'fa-receipt', label: 'Verify Slip'},
                                {id: 'accounts', icon: 'fa-user-shield', label: 'Account Monitor'},
                                {id: 'alerts', icon: 'fa-bell', label: 'Alert Center'}
                            ].map(tab => (
                                <button key={tab.id} onClick={() => setActiveTab(tab.id)}
                                    className={`px-5 py-2 rounded-md font-medium text-sm transition-all flex items-center gap-2
                                        ${activeTab === tab.id ? 'bg-cyan-500/20 text-cyan-400' : 'text-slate-400 hover:text-white'}`}>
                                    <i className={`fas ${tab.icon}`}></i> {tab.label}
                                </button>
                            ))}
                        </div>
                    </nav>
                    
                    <main className="max-w-7xl mx-auto px-6 pb-12">
                        {activeTab === 'process' && <ProcessTransaction />}
                        {activeTab === 'search' && <SearchTrace />}
                        {activeTab === 'slip' && <SlipVerifier />}
                        {activeTab === 'accounts' && <AccountMonitor />}
                        {activeTab === 'alerts' && <AlertCenter />}
                    </main>
                </div>
            );
        };
        
        const ProcessTransaction = () => {
            const [result, setResult] = useState(null);
            const [loading, setLoading] = useState(false);
            const [form, setForm] = useState({
                customer_id: 'CUST-001',
                account_number: '1000456789',
                card_last_four: '',
                amount: 2500,
                merchant_name: 'Walmart Supercenter',
                merchant_id: 'MERCH-001',
                merchant_city: 'New York',
                merchant_country: 'US',
                channel: 'pos',
                card_present: 1,
                pin_entered: 1,
                transaction_time: new Date().toISOString()
            });
            
            const handleSubmit = async () => {
                setLoading(true);
                try {
                    const res = await fetch('/api/transaction/process', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            ...form,
                            merchant_category: 'Retail',
                            merchant_address: '100 Main St',
                            currency: 'USD',
                            is_international: form.merchant_country !== 'US' ? 1 : 0
                        })
                    });
                    const data = await res.json();
                    setResult(data);
                } catch(e) {
                    alert('Error: ' + e.message);
                }
                setLoading(false);
            };
            
            return (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <div className="glass rounded-xl p-6 glow-blue">
                        <h2 className="text-lg font-semibold text-white mb-4">
                            <i className="fas fa-credit-card mr-2 text-cyan-400"></i> Process Transaction
                        </h2>
                        <div className="space-y-4">
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Customer ID</label>
                                    <input value={form.customer_id} onChange={e => setForm({...form, customer_id: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm" />
                                </div>
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Account Number</label>
                                    <input value={form.account_number} onChange={e => setForm({...form, account_number: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm mono" placeholder="Or use card" />
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Card Last 4</label>
                                    <input value={form.card_last_four} onChange={e => setForm({...form, card_last_four: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm mono" placeholder="****" maxLength="4" />
                                </div>
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Amount ($)</label>
                                    <input type="number" value={form.amount} onChange={e => setForm({...form, amount: Number(e.target.value)})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm" />
                                </div>
                            </div>
                            <div>
                                <label className="text-xs text-slate-400 block mb-1">Merchant</label>
                                <input value={form.merchant_name} onChange={e => setForm({...form, merchant_name: e.target.value})}
                                    className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm" />
                            </div>
                            <div className="grid grid-cols-3 gap-4">
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">City</label>
                                    <input value={form.merchant_city} onChange={e => setForm({...form, merchant_city: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm" />
                                </div>
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Country</label>
                                    <select value={form.merchant_country} onChange={e => setForm({...form, merchant_country: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm">
                                        <option value="US">United States</option>
                                        <option value="NG">Nigeria</option>
                                        <option value="RU">Russia</option>
                                        <option value="GB">UK</option>
                                        <option value="CA">Canada</option>
                                    </select>
                                </div>
                                <div>
                                    <label className="text-xs text-slate-400 block mb-1">Channel</label>
                                    <select value={form.channel} onChange={e => setForm({...form, channel: e.target.value})}
                                        className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm">
                                        <option value="pos">POS Terminal</option>
                                        <option value="atm">ATM</option>
                                        <option value="online">Online</option>
                                        <option value="mobile">Mobile</option>
                                        <option value="wire">Wire Transfer</option>
                                    </select>
                                </div>
                            </div>
                            <div className="flex gap-4">
                                <label className="flex items-center gap-2">
                                    <input type="checkbox" checked={form.card_present} onChange={e => setForm({...form, card_present: e.target.checked ? 1 : 0})}
                                        className="w-4 h-4 rounded bg-slate-800 border-slate-600 text-cyan-500" />
                                    <span className="text-sm text-slate-300">Card Present</span>
                                </label>
                                <label className="flex items-center gap-2">
                                    <input type="checkbox" checked={form.pin_entered} onChange={e => setForm({...form, pin_entered: e.target.checked ? 1 : 0})}
                                        className="w-4 h-4 rounded bg-slate-800 border-slate-600 text-cyan-500" />
                                    <span className="text-sm text-slate-300">PIN Entered</span>
                                </label>
                            </div>
                            <button onClick={handleSubmit} disabled={loading}
                                className="w-full bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-bold py-3 rounded-lg transition-all">
                                {loading ? <i className="fas fa-spinner fa-spin mr-2"></i> : <i className="fas fa-shield-alt mr-2"></i>}
                                {loading ? 'Analyzing...' : 'PROCESS & DETECT FRAUD'}
                            </button>
                        </div>
                    </div>
                    <div>
                        {result ? <TransactionResult result={result} /> : (
                            <div className="glass rounded-xl p-6 flex items-center justify-center h-full">
                                <p className="text-slate-500">Submit a transaction to see fraud analysis</p>
                            </div>
                        )}
                    </div>
                </div>
            );
        };
        
        const TransactionResult = ({ result }) => {
            const isBlocked = result.status === 'blocked';
            const isAlert = result.alert_triggered;
            
            return (
                <div className={`glass rounded-xl p-6 ${isBlocked ? 'glow-red border-red-500/30' : ''}`}>
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="text-lg font-semibold text-white">Analysis Result</h2>
                        <span className={`px-3 py-1 rounded-full text-xs font-bold
                            ${isBlocked ? 'bg-red-500/20 text-red-400' : 
                              result.status === 'under_review' ? 'bg-yellow-500/20 text-yellow-400' : 
                              'bg-green-500/20 text-green-400'}`}>
                            {result.action}
                        </span>
                    </div>
                    <div className="space-y-4">
                        <div className="bg-slate-800/50 rounded-lg p-4">
                            <div className="flex justify-between items-center mb-2">
                                <span className="text-sm text-slate-400">Fraud Probability</span>
                                <span className={`text-2xl font-bold ${result.fraud_probability > 0.7 ? 'text-red-400' : result.fraud_probability > 0.4 ? 'text-yellow-400' : 'text-green-400'}`}>
                                    {result.risk_score}%
                                </span>
                            </div>
                            <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
                                <div className={`h-full rounded-full transition-all duration-1000
                                    ${result.fraud_probability > 0.7 ? 'bg-red-500' : result.fraud_probability > 0.4 ? 'bg-yellow-500' : 'bg-green-500'}`}
                                    style={{width: `${result.risk_score}%`}}></div>
                            </div>
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="bg-slate-800/30 rounded-lg p-3">
                                <p className="text-xs text-slate-500">Customer</p>
                                <p className="text-sm text-white font-medium">{result.customer_name}</p>
                                <p className="text-xs text-slate-500 mono">{result.customer_id}</p>
                            </div>
                            <div className="bg-slate-800/30 rounded-lg p-3">
                                <p className="text-xs text-slate-500">Amount</p>
                                <p className="text-sm text-white font-medium">{result.currency} {result.amount?.toLocaleString()}</p>
                            </div>
                        </div>
                        <div className="bg-slate-800/30 rounded-lg p-3">
                            <p className="text-xs text-slate-500">Merchant Location</p>
                            <p className="text-sm text-white">{result.merchant_location}</p>
                            <p className="text-xs text-slate-500">Channel: {result.channel}</p>
                        </div>
                        {result.notification && (
                            <div className="bg-cyan-500/10 border border-cyan-500/30 rounded-lg p-3">
                                <p className="text-sm text-cyan-400 font-medium mb-1">
                                    <i className="fas fa-paper-plane mr-2"></i>Customer Alert Sent
                                </p>
                                <p className="text-xs text-slate-400">Channels: {result.notification.channels_sent?.join(', ')}</p>
                                <p className="text-xs text-slate-500 mt-1 mono">{result.notification.sms_preview}</p>
                            </div>
                        )}
                        {result.slip_verification?.red_flags?.length > 0 && (
                            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
                                <p className="text-sm text-red-400 font-medium mb-1">
                                    <i className="fas fa-exclamation-triangle mr-2"></i>Slip Verification Issues
                                </p>
                                {result.slip_verification.red_flags.map((flag, i) => (
                                    <p key={i} className="text-xs text-red-300">• {flag}</p>
                                ))}
                            </div>
                        )}
                        <div className="grid grid-cols-3 gap-2 text-center text-xs">
                            <div className="bg-slate-800/30 rounded p-2">
                                <p className="text-slate-500">Velocity (1h)</p>
                                <p className="text-cyan-400 font-mono">{result.velocity_1h}</p>
                            </div>
                            <div className="bg-slate-800/30 rounded p-2">
                                <p className="text-slate-500">Latency</p>
                                <p className="text-green-400 font-mono">{result.processing_time_ms}ms</p>
                            </div>
                            <div className="bg-slate-800/30 rounded p-2">
                                <p className="text-slate-500">Model</p>
                                <p className="text-purple-400 font-mono">{result.model_version}</p>
                            </div>
                        </div>
                    </div>
                </div>
            );
        };
        
        const SearchTrace = () => {
            const [query, setQuery] = useState('');
            const [searchType, setSearchType] = useState('all');
            const [results, setResults] = useState([]);
            const [loading, setLoading] = useState(false);
            
            const handleSearch = async () => {
                setLoading(true);
                try {
                    const res = await fetch(`/api/transaction/search?query=${query}&search_type=${searchType}`);
                    const data = await res.json();
                    setResults(data.results);
                } catch(e) {}
                setLoading(false);
            };
            
            return (
                <div className="glass rounded-xl p-6">
                    <h2 className="text-lg font-semibold text-white mb-4">
                        <i className="fas fa-search mr-2 text-cyan-400"></i> Search & Trace Transactions
                    </h2>
                    <div className="flex gap-4 mb-6">
                        <select value={searchType} onChange={e => setSearchType(e.target.value)}
                            className="bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-white">
                            <option value="all">All Fields</option>
                            <option value="account">Account Number</option>
                            <option value="card">Card Last 4</option>
                            <option value="txn">Transaction ID</option>
                            <option value="customer">Customer ID</option>
                        </select>
                        <input value={query} onChange={e => setQuery(e.target.value)}
                            placeholder="Enter search term..."
                            className="flex-1 bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-white mono" />
                        <button onClick={handleSearch} disabled={loading}
                            className="bg-cyan-600 hover:bg-cyan-500 text-white px-6 py-2 rounded-lg">
                            {loading ? <i className="fas fa-spinner fa-spin"></i> : <i className="fas fa-search"></i>}
                        </button>
                    </div>
                    <div className="space-y-2">
                        {results.length === 0 && !loading && (
                            <p className="text-slate-500 text-center py-8">Enter a search term to find transactions</p>
                        )}
                        {results.map((tx, i) => (
                            <div key={i} className="bg-slate-800/30 rounded-lg p-4 flex items-center justify-between slide-in">
                                <div>
                                    <div className="flex items-center gap-3 mb-1">
                                        <span className="text-xs font-mono text-cyan-400">{tx.transaction_id}</span>
                                        {tx.alert_triggered && <i className="fas fa-exclamation-circle text-red-400"></i>}
                                    </div>
                                    <p className="text-sm text-white">{tx.merchant_name}</p>
                                    <p className="text-xs text-slate-500">
                                        {tx.account_number ? `Account: ****${tx.account_number?.slice(-4)}` : ''}
                                        {tx.card_last_four ? `Card: ****${tx.card_last_four}` : ''}
                                    </p>
                                </div>
                                <div className="text-right">
                                    <p className="text-lg font-bold text-white">${tx.amount?.toLocaleString()}</p>
                                    <p className={`text-xs ${tx.fraud_probability > 0.5 ? 'text-red-400' : 'text-green-400'}`}>
                                        Risk: {(tx.fraud_probability * 100).toFixed(1)}%
                                    </p>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            );
        };
        
        const SlipVerifier = () => {
            const [slipData, setSlipData] = useState({
                slip_id: '', merchant_id: '', merchant_name: '',
                authorization_code: '', terminal_id: ''
            });
            const [result, setResult] = useState(null);
            
            const handleVerify = async () => {
                try {
                    const res = await fetch('/api/slip/verify', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            ...slipData,
                            transaction_id: 'TXN-SLIP-001',
                            merchant_address: '123 Main St',
                            merchant_city: 'New York',
                            merchant_country: 'US',
                            merchant_category_code: '5311',
                            receipt_number: slipData.slip_id,
                            transaction_time: new Date().toISOString()
                        })
                    });
                    const data = await res.json();
                    setResult(data);
                } catch(e) {
                    alert('Verification failed');
                }
            };
            
            return (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <div className="glass rounded-xl p-6 receipt-pattern">
                        <h2 className="text-lg font-semibold text-white mb-4">
                            <i className="fas fa-receipt mr-2 text-green-400"></i> Verify Transaction Slip
                        </h2>
                        <div className="space-y-4">
                            <div>
                                <label className="text-xs text-slate-400 block mb-1">Slip ID / Receipt Number</label>
                                <input value={slipData.slip_id} onChange={e => setSlipData({...slipData, slip_id: e.target.value})}
                                    className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm mono" />
                            </div>
                            <div>
                                <label className="text-xs text-slate-400 block mb-1">Merchant ID</label>
                                <input value={slipData.merchant_id} onChange={e => setSlipData({...slipData, merchant_id: e.target.value})}
                                    className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm" placeholder="MERCH-001" />
                            </div>
                            <div>
                                <label className="text-xs text-slate-400 block mb-1">Authorization Code</label>
                                <input value={slipData.authorization_code} onChange={e => setSlipData({...slipData, authorization_code: e.target.value})}
                                    className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm mono" placeholder="AUTH123" />
                            </div>
                            <div>
                                <label className="text-xs text-slate-400 block mb-1">Terminal ID</label>
                                <input value={slipData.terminal_id} onChange={e => setSlipData({...slipData, terminal_id: e.target.value})}
                                    className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-sm mono" />
                            </div>
                            <button onClick={handleVerify}
                                className="w-full bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-500 hover:to-emerald-500 text-white font-bold py-3 rounded-lg">
                                <i className="fas fa-check-circle mr-2"></i> VERIFY SLIP
                            </button>
                        </div>
                    </div>
                    <div>
                        {result ? (
                            <div className={`glass rounded-xl p-6 ${result.verified ? 'border-green-500/30' : 'border-red-500/30'}`}>
                                <h3 className="text-lg font-semibold text-white mb-4">Verification Result</h3>
                                <div className="flex items-center gap-3 mb-4">
                                    <div className={`w-12 h-12 rounded-full flex items-center justify-center
                                        ${result.verified ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
                                        <i className={`fas ${result.verified ? 'fa-check text-green-400' : 'fa-times text-red-400'} text-xl`}></i>
                                    </div>
                                    <div>
                                        <p className={`text-lg font-bold ${result.verified ? 'text-green-400' : 'text-red-400'}`}>
                                            {result.verified ? 'VERIFIED' : 'SUSPICIOUS'}
                                        </p>
                                        <p className="text-xs text-slate-500">Risk Score: {result.risk_score}</p>
                                    </div>
                                </div>
                                {result.red_flags.length > 0 && (
                                    <div className="bg-red-500/10 rounded-lg p-3">
                                        <p className="text-sm text-red-400 font-medium mb-2">Red Flags:</p>
                                        {result.red_flags.map((flag, i) => (
                                            <p key={i} className="text-xs text-red-300">• {flag}</p>
                                        ))}
                                    </div>
                                )}
                            </div>
                        ) : (
                            <div className="glass rounded-xl p-6 flex items-center justify-center h-full">
                                <p className="text-slate-500">Enter slip details to verify</p>
                            </div>
                        )}
                    </div>
                </div>
            );
        };
        
        const AccountMonitor = () => {
            const [accountNum, setAccountNum] = useState('1000456789');
            const [accountData, setAccountData] = useState(null);
            
            const fetchAccount = async () => {
                try {
                    const res = await fetch(`/api/account/${accountNum}`);
                    const data = await res.json();
                    setAccountData(data);
                } catch(e) {}
            };
            
            return (
                <div className="glass rounded-xl p-6">
                    <h2 className="text-lg font-semibold text-white mb-4">
                        <i className="fas fa-user-shield mr-2 text-purple-400"></i> Account Monitor
                    </h2>
                    <div className="flex gap-4 mb-6">
                        <input value={accountNum} onChange={e => setAccountNum(e.target.value)}
                            placeholder="Enter account number"
                            className="flex-1 bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-white mono" />
                        <button onClick={fetchAccount}
                            className="bg-purple-600 hover:bg-purple-500 text-white px-6 py-2 rounded-lg">
                            <i className="fas fa-search mr-2"></i> Monitor
                        </button>
                    </div>
                    {accountData && (
                        <div className="space-y-4">
                            <div className="grid grid-cols-3 gap-4">
                                <div className="bg-slate-800/50 rounded-lg p-4">
                                    <p className="text-xs text-slate-500">Account Holder</p>
                                    <p className="text-lg font-bold text-white">{accountData.customer?.full_name}</p>
                                </div>
                                <div className="bg-slate-800/50 rounded-lg p-4">
                                    <p className="text-xs text-slate-500">Balance</p>
                                    <p className="text-lg font-bold text-green-400">${accountData.account?.balance?.toLocaleString()}</p>
                                </div>
                                <div className="bg-slate-800/50 rounded-lg p-4">
                                    <p className="text-xs text-slate-500">Status</p>
                                    <p className={`text-lg font-bold ${accountData.account?.status === 'active' ? 'text-green-400' : 'text-red-400'}`}>
                                        {accountData.account?.status?.toUpperCase()}
                                    </p>
                                </div>
                            </div>
                            <h3 className="text-sm font-semibold text-slate-400 mt-4">Recent Transactions</h3>
                            <div className="space-y-2">
                                {accountData.recent_transactions?.map((tx, i) => (
                                    <div key={i} className="bg-slate-800/30 rounded-lg p-3 flex justify-between items-center">
                                        <div>
                                            <p className="text-sm text-white">{tx.merchant_name}</p>
                                            <p className="text-xs text-slate-500 mono">{tx.transaction_id}</p>
                                        </div>
                                        <div className="text-right">
                                            <p className="text-sm font-bold text-white">${tx.amount?.toLocaleString()}</p>
                                            {tx.alert_triggered && <span className="text-xs text-red-400">FRAUD ALERT</span>}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            );
        };
        
        const AlertCenter = () => {
            return (
                <div className="glass rounded-xl p-6">
                    <h2 className="text-lg font-semibold text-white mb-4">
                        <i className="fas fa-bell mr-2 text-yellow-400"></i> Alert Center
                    </h2>
                    <p className="text-slate-500">View and manage customer fraud alerts.</p>
                    <div className="mt-4 space-y-3">
                        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
                            <div className="flex items-center justify-between">
                                <div>
                                    <p className="text-sm text-yellow-400 font-medium">ALERT-001</p>
                                    <p className="text-xs text-slate-400">Customer: John Smith (+14155551234)</p>
                                    <p className="text-xs text-slate-500">SMS sent • Email sent • Awaiting response</p>
                                </div>
                                <button className="bg-yellow-500/20 text-yellow-400 px-4 py-2 rounded-lg text-sm">
                                    View Details
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            );
        };
        
        const root = ReactDOM.createRoot(document.getElementById('root'));
        root.render(<App />);
    </script>
</body>
</html>'''

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("FRAUDGUARD BANK - FIXED VERSION")
    print("=" * 70)
    print("\n🔧 FRONTEND FIX:")
    print("   • HTML stored as separate string variable (DASHBOARD_HTML)")
    print("   • No triple-quote conflicts inside Python")
    print("   • Complete React code without truncation")
    print("\n🏦 BANKING FEATURES:")
    print("   • Account number fraud detection")
    print("   • Card number monitoring (last 4 digits)")
    print("   • Transaction slip verification")
    print("   • SMS/Email alerts to customers")
    print("\n🌐 ACCESS:")
    print("   Dashboard: http://localhost:8000")
    print("   API Docs:  http://localhost:8000/docs")
    print("=" * 70)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)