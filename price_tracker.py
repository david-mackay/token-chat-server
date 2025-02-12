import os
import requests
from datetime import datetime, timezone
import eventlet

class PriceTracker:
    def __init__(self, socketio):
        self.token_subscriptions = {}
        self.API_KEY = os.getenv('BIRDEYE_API_KEY', '')
        self.API_URL = 'https://public-api.birdeye.so/defi'
        self.price_interval = 600
        self.socketio = socketio

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
            self.socketio.emit('price_update', price_message, room=sid)

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

        self.socketio.emit('price_update', price_message, room=sid)