import uuid
import eventlet
eventlet.monkey_patch()
from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO
import boto3
import os
from dotenv import load_dotenv
import functools
from maintenance.cache_maintenance import setup_cache_maintenance

from auth.auth_manager import AuthManager
from chat.chat_manager import ChatManager
from price_tracker.price_tracker import PriceTracker
from routes import register_routes

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

# Set up maintenance job
setup_cache_maintenance(chat_manager, interval_minutes=30)

# Define supported tokens
open_chats = {
    "So11111111111111111111111111111111111111112",
    "HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC",
    "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
}

# Register all routes and socket events
register_routes(app, socketio, chat_manager, price_tracker, auth_manager)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3001))
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=True,
        use_reloader=False
    )