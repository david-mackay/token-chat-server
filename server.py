import uuid
import eventlet
eventlet.monkey_patch()
from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import json
import requests
from typing import Dict, Set, Any
from boto3.dynamodb.conditions import Key
import jwt
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import functools
from solders.pubkey import Pubkey
from solders.message import Message
from solders.signature import Signature

load_dotenv()

app = Flask(__name__)
CORS(app)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='eventlet',
    logger=True,
    engineio_logger=True,
    ping_timeout=60
)

# Initialize rate limiter
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# Initialize DynamoDB
dynamodb_resource = boto3.resource('dynamodb', 
    region_name=os.getenv('AWS_REGION', 'us-east-1'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

class AuthManager:
    def __init__(self):
        self.SECRET_KEY = os.getenv('JWT_SECRET_KEY')
        if not self.SECRET_KEY:
            raise ValueError("JWT_SECRET_KEY must be set in environment variables")
            
    def generate_token(self, wallet_address: str, token_address: str) -> str:
        payload = {
            'wallet_address': wallet_address,
            'token_address': token_address,
            'exp': datetime.now(timezone.utc) + timedelta(days=1),
            'iat': datetime.now(timezone.utc)
        }
        return jwt.encode(payload, self.SECRET_KEY, algorithm='HS256')
        
    def verify_token(self, token: str) -> dict:
        try:
            return jwt.decode(token, self.SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise ValueError("Token has expired")
        except jwt.InvalidTokenError:
            raise ValueError("Invalid token")
            
    def verify_signature(self, message: str, signature: str, wallet_address: str) -> bool:
        try:
            print(f"[Signature Verification] Starting for wallet: {wallet_address}")
            print(f"[Signature Verification] Raw signature (hex): {signature}")
            
            # Convert wallet address to Pubkey
            try:
                pubkey = Pubkey.from_string(wallet_address)
                print(f"[Signature Verification] Pubkey created: {pubkey}")
            except Exception as e:
                print(f"[Signature Verification] Failed to parse pubkey: {e}")
                return False

            # Convert signature from hex to Signature object
            try:
                signature_bytes = bytes.fromhex(signature)
                signature_obj = Signature.from_bytes(signature_bytes)
                print(f"[Signature Verification] Signature object created")
            except Exception as e:
                print(f"[Signature Verification] Failed to create signature object: {e}")
                return False

            # Create the message bytes
            try:
                message_bytes = message.encode('utf-8')
                print(f"[Signature Verification] Message: {message}")
                print(f"[Signature Verification] Message bytes length: {len(message_bytes)}")
            except Exception as e:
                print(f"[Signature Verification] Failed to encode message: {e}")
                return False

            # Verify the signature using solders Signature verify method
            try:
                verified = signature_obj.verify(pubkey, message_bytes)
                print(f"[Signature Verification] Result: {verified}")
                return verified
            except Exception as e:
                print(f"[Signature Verification] Verification failed: {e}")
                return False
                
        except Exception as e:
            print(f"[Signature Verification] Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return False

class ChatManager:
    def __init__(self, dynamodb_resource):
        self.MESSAGE_LIMIT = 500
        self.table_name = os.getenv('DYNAMODB_TABLE_NAME', 'token-chat-server')
        self.table = dynamodb_resource.Table(self.table_name)
        self.chat_rooms: Dict[str, Dict[str, Any]] = {}

    def get_recent_messages(self, token_address: str) -> list:
        try:
            response = self.table.query(
                KeyConditionExpression=Key('tokenAddress').eq(token_address),
                ScanIndexForward=True,
                Limit=self.MESSAGE_LIMIT
            )
            return response.get('Items', [])
        except ClientError as e:
            print(f"Error querying messages: {e}")
            return []

    def handle_message(self, message: dict) -> None:
        try:
            # Insert into DynamoDB
            self.table.put_item(Item=message)
            
            # Broadcast to room
            socketio.emit('chat_message', {
                'type': 'chat_message',
                'data': message
            }, room=message['tokenAddress'])

        except ClientError as e:
            print(f"Error handling message: {e}")
            raise e

class PriceTracker:
    def __init__(self):
        self.token_subscriptions = {}
        self.API_KEY = os.getenv('BIRDEYE_API_KEY', '')
        self.API_URL = 'https://public-api.birdeye.so/defi'
        self.price_interval = 120

    def start_tracking(self, token_address: str, sid: str) -> None:
        if token_address in self.token_subscriptions:
            self.token_subscriptions[token_address]['subscribers'].add(sid)
            if self.token_subscriptions[token_address]['last_price']:
                self.send_price_update(sid, token_address)
            return

        self.token_subscriptions[token_address] = {
            'last_price': 0,
            'subscribers': {sid},
            'token_name': None,
            'token_symbol': None
        }

        def update_loop():
            while token_address in self.token_subscriptions:
                self.update_price(token_address)
                eventlet.sleep(self.price_interval)

        eventlet.spawn(update_loop)

    def stop_tracking(self, token_address: str, sid: str) -> None:
        if token_address not in self.token_subscriptions:
            return

        self.token_subscriptions[token_address]['subscribers'].discard(sid)
        if not self.token_subscriptions[token_address]['subscribers']:
            del self.token_subscriptions[token_address]

    def update_price(self, token_address: str) -> None:
        if token_address not in self.token_subscriptions:
            return

        try:
            headers = {
                'accept': 'application/json',
                'x-chain': 'solana',
                'X-API-KEY': self.API_KEY
            }
            
            response = requests.get(
                f"{self.API_URL}/price?address={token_address}",
                headers=headers
            )
            
            if response.status_code != 200:
                print(f"HTTP error! status: {response.status_code}")
                return
                
            price_data = response.json()
            if not price_data['success']:
                print('Failed to fetch price data')
                return

            subscription = self.token_subscriptions[token_address]
            subscription['last_price'] = price_data['data']['value']
            subscription['token_symbol'] = price_data['data'].get('symbol')
            subscription['token_name'] = price_data['data'].get('name')

            self.broadcast_price(token_address)

        except Exception as e:
            print(f"Price update failed for {token_address}: {e}")

    def broadcast_price(self, token_address: str) -> None:
        if token_address not in self.token_subscriptions:
            return

        subscription = self.token_subscriptions[token_address]
        price_message = {
            'type': 'price_update',
            'data': {
                'tokenAddress': token_address,
                'price': subscription['last_price'],
                'tokenName': subscription['token_name'],
                'tokenSymbol': subscription['token_symbol'],
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        }

        for sid in subscription['subscribers']:
            socketio.emit('price_update', price_message, room=sid)

    def send_price_update(self, sid: str, token_address: str) -> None:
        if token_address not in self.token_subscriptions:
            return

        subscription = self.token_subscriptions[token_address]
        price_message = {
            'type': 'price_update',
            'data': {
                'tokenAddress': token_address,
                'price': subscription['last_price'],
                'tokenName': subscription['token_name'],
                'tokenSymbol': subscription['token_symbol'],
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        }

        socketio.emit('price_update', price_message, room=sid)

# Initialize managers
chat_manager = ChatManager(dynamodb_resource)
price_tracker = PriceTracker()
auth_manager = AuthManager()

# Define supported tokens
open_chats = {
    "So11111111111111111111111111111111111111112",
    "HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC",
    "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
}

# Track user message counts for rate limiting
user_message_counts = {}

def rate_limit_messages(wallet_address: str) -> bool:
    """Rate limit messages per user"""
    current_time = datetime.now()
    if wallet_address not in user_message_counts:
        user_message_counts[wallet_address] = []
    
    # Clean old messages (older than 1 minute)
    user_message_counts[wallet_address] = [
        timestamp for timestamp in user_message_counts[wallet_address]
        if (current_time - timestamp).total_seconds() < 60
    ]
    
    # Check if user has sent too many messages
    if len(user_message_counts[wallet_address]) >= 60:  # 60 messages per minute
        return False
    
    user_message_counts[wallet_address].append(current_time)
    return True

def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        print("Starting auth check...")
        print(args)
        # In Socket.IO, the data is the first argument after 'self'
            
        data = args[0]  # args[0] would be 'self' if this is a class method
        print(f"Got data: {data}")
            
        if not isinstance(data, dict):
            print("Data is not a dictionary")
            emit('auth_required')
            return
            
        session_token = data.get('session_token')
        if not session_token:
            print(f"No session token in data. Keys available: {data.keys()}")
            emit('auth_required')
            return
            
        try:
            print(f"Verifying token: {session_token[:20]}...")
            payload = auth_manager.verify_token(session_token)
            print(f"Token verified. Payload: {payload}")
            return f(data, payload)
        except ValueError as e:
            print(f"Token verification error: {e}")
            emit('auth_required')
            return
    return decorated

@socketio.on('connect')
def handle_connect():
    try:
        # Get authentication data from connection query parameters
        token_address = request.args.get('token_address')
        auth_message = request.args.get('auth_message')
        signature = request.args.get('signature')
        wallet_address = request.args.get('wallet_address')
        
        print(f"Connection attempt with:")
        print(f"token_address: {token_address}")
        print(f"wallet_address: {wallet_address}")
        print(f"auth_message: {auth_message}")
        print(f"signature: {signature}")
        
        if not all([token_address, auth_message, signature, wallet_address]):
            missing = [k for k, v in {
                'token_address': token_address,
                'auth_message': auth_message,
                'signature': signature,
                'wallet_address': wallet_address
            }.items() if not v]
            print(f"Missing authentication parameters: {missing}")
            return False
            
        if token_address not in open_chats:
            print(f"Invalid token address: {token_address}")
            return False
            
        # Verify signature
        if not auth_manager.verify_signature(auth_message, signature, wallet_address):
            print(f"Invalid signature for wallet {wallet_address}")
            return False
            
        # Start price tracking
        price_tracker.start_tracking(token_address, request.sid)
            
        # Generate JWT
        session_token = auth_manager.generate_token(wallet_address, token_address)
        
        # Join room and send token
        join_room(token_address)
        emit('auth_success', {'session_token': session_token})
        
        # Send message history
        recent_messages = chat_manager.get_recent_messages(token_address)
        if recent_messages:
            emit('message_history', {
                'type': 'message_history',
                'data': recent_messages
            })
            
        print(f"Successfully authenticated wallet {wallet_address}")
        return True
    except Exception as e:
        print(f"Error in connection setup: {e}")
        import traceback
        traceback.print_exc()
        return False

@socketio.on('disconnect')
def handle_disconnect():
    token_address = request.args.get('token_address')
    if token_address:
        price_tracker.stop_tracking(token_address, request.sid)
        leave_room(token_address)

@socketio.on('chat_message')
@require_auth
def handle_message(data, auth_payload):
    try:
        # Rate limit check
        if not rate_limit_messages(auth_payload['wallet_address']):
            emit('error', 'Rate limit exceeded. Please wait before sending more messages.')
            return
            
        print(f"Auth payload wallet: {auth_payload['wallet_address']}")
        print(f"Message wallet: {data.get('walletAddress')}")
            
        if auth_payload['wallet_address'].lower() != data.get('walletAddress', '').lower():
            print("Wallet address mismatch")
            emit('auth_required')
            return
            
        if 'tokenAddress' not in data:
            return
            
        # Always generate a new server-side ID
        data['id'] = str(uuid.uuid4())
        
        # Add server-side timestamp to ensure consistency
        data['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        print("Processing message...")
        chat_manager.handle_message(data)
        print("Message processed successfully")
    except Exception as e:
        print(f"Error handling message: {e}")
        import traceback
        traceback.print_exc()
        emit('error', 'Failed to process message')

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3001))
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=True,
        use_reloader=False
    )