import eventlet
eventlet.monkey_patch()
from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import requests
from typing import Dict, Set, Any

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

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    host=os.getenv('DB_HOST', 'localhost'),
    port=int(os.getenv('DB_PORT', '5432')),
    database=os.getenv('DB_NAME', 'token_chat'),
    user=os.getenv('DB_USER', 'token_chat_user'),
    password=os.getenv('DB_PASSWORD')
)

class ChatManager:
    def __init__(self, db_pool):
        self.chat_rooms: Dict[str, Dict[str, Any]] = {}
        self.MESSAGE_LIMIT = 500
        self.db_pool = db_pool

    def get_db_connection(self):
        conn = self.db_pool.getconn()
        conn.autocommit = False
        return conn

    def release_connection(self, conn):
        self.db_pool.putconn(conn)

    def get_message_count(self, token_address: str) -> int:
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM messages WHERE token_address = %s",
                    (token_address,)
                )
                return cur.fetchone()[0]
        finally:
            self.release_connection(conn)

    def get_recent_messages(self, token_address: str) -> list:
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, content, wallet_address, created_at
                    FROM messages
                    WHERE token_address = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (token_address, self.MESSAGE_LIMIT))
                messages = cur.fetchall()
                return [{
                    'id': msg['id'],
                    'content': msg['content'],
                    'walletAddress': msg['wallet_address'],
                    'timestamp': msg['created_at'].isoformat(),
                    'tokenAddress': token_address
                } for msg in messages]
        finally:
            self.release_connection(conn)

    def handle_message(self, message: dict) -> None:
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("""
                    INSERT INTO messages (id, content, wallet_address, token_address, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    message['id'],
                    message['content'],
                    message['walletAddress'],
                    message['tokenAddress'],
                    datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
                ))

                room = self.chat_rooms.get(message['tokenAddress'])
                if room:
                    room['message_count'] += 1
                    if room['message_count'] > self.MESSAGE_LIMIT:
                        cur.execute("""
                            DELETE FROM messages
                            WHERE token_address = %s
                            AND id NOT IN (
                                SELECT id FROM messages
                                WHERE token_address = %s
                                ORDER BY created_at DESC
                                LIMIT %s
                            )
                        """, (message['tokenAddress'], message['tokenAddress'], self.MESSAGE_LIMIT))
                        room['message_count'] = self.MESSAGE_LIMIT

                conn.commit()
                socketio.emit('chat_message', {
                    'type': 'chat_message',
                    'data': message
                }, room=message['tokenAddress'])

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.release_connection(conn)

class PriceTracker:
    def __init__(self):
        self.token_subscriptions = {}
        self.API_KEY = os.getenv('BIRDEYE_API_KEY', '')
        self.API_URL = 'https://public-api.birdeye.so/defi'
        self.price_interval = 5

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
                'timestamp': datetime.utcnow().isoformat()
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
                'timestamp': datetime.utcnow().isoformat()
            }
        }

        socketio.emit('price_update', price_message, room=sid)

chat_manager = ChatManager(db_pool)
price_tracker = PriceTracker()

@app.route('/tc/<token_address>')
def token_chat(token_address):
    return {'status': 'WebSocket endpoint'}

@socketio.on('connect')
def handle_connect():
    try:
        token_address = request.args.get('token_address')
        if not token_address:
            return False
        
        join_room(token_address)
        price_tracker.start_tracking(token_address, request.sid)
        
        recent_messages = chat_manager.get_recent_messages(token_address)
        if recent_messages:
            emit('message_history', {
                'type': 'message_history',
                'data': recent_messages
            })
            
        return True
    except Exception as e:
        print(f"Error in connection setup: {e}")
        return False

@socketio.on('disconnect')
def handle_disconnect():
    token_address = request.args.get('token_address')
    if token_address:
        price_tracker.stop_tracking(token_address, request.sid)
        leave_room(token_address)

@socketio.on('chat_message')
def handle_message(data):
    try:
        chat_manager.handle_message(data)
    except Exception as e:
        print(f"Error handling message: {e}")

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3001))
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=True,
        use_reloader=False
    )