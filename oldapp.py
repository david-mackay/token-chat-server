import uuid
import eventlet
eventlet.monkey_patch()
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import functools
from maintenance.cache_maintenance import setup_cache_maintenance

from auth.auth_manager import AuthManager
from chat.chat_manager import ChatManager
from price_tracker.price_tracker import PriceTracker

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

# Initialize managers
chat_manager = ChatManager(dynamodb_resource, socketio)
price_tracker = PriceTracker(socketio)
auth_manager = AuthManager()

setup_cache_maintenance(chat_manager, interval_minutes=30)
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
        data = args[0]
            
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
            payload = auth_manager.verify_token(session_token)
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
        
        if not all([token_address, auth_message, signature, wallet_address]):
            missing = [k for k, v in {
                'token_address': token_address,
                'auth_message': auth_message,
                'signature': signature,
                'wallet_address': wallet_address
            }.items() if not v]
            print(f"Missing authentication parameters: {missing}")
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
            
        if auth_payload['wallet_address'] != data.get('walletAddress', ''):
            print("Wallet address mismatch")
            emit('auth_required')
            return
            
        if 'tokenAddress' not in data:
            return
            
        # Always generate a new server-side ID
        data['id'] = str(uuid.uuid4())
        
        # Add server-side timestamp to ensure consistency
        data['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        chat_manager.handle_message(data)
    except Exception as e:
        print(f"Error handling message: {e}")
        import traceback
        traceback.print_exc()
        emit('error', 'Failed to process message')

@app.route('/active-rooms', methods=['GET'])
@limiter.limit("60 per minute")  # Add rate limiting
def get_active_rooms():
    try:
        active_rooms = []
        for token_address, data in price_tracker.token_subscriptions.items():
            room_data = {
                'tokenAddress': token_address,
                'userCount': len(data['subscribers']),
                'tokenName': data['token_name'],
                'tokenSymbol': data['token_symbol'],
                'lastPrice': data['last_price']
            }
            active_rooms.append(room_data)

        return jsonify({'rooms': active_rooms})
    except Exception as e:
        print(f"Error getting active rooms: {e}")
        return jsonify({'error': 'Failed to fetch room data'}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 3001))
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=True,
        use_reloader=False
    )