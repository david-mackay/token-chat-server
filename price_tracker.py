import os
import requests
from datetime import datetime, timezone
import eventlet

class PriceTracker:
    def __init__(self, socketio):
        self.token_subscriptions = {}
        self.API_URL = 'https://api-v3.raydium.io/mint'
        self.price_interval = 5  # 5 seconds between updates
        self.socketio = socketio
        self.last_update = 0

    def start_tracking(self, token_address: str, sid: str) -> None:
        if token_address in self.token_subscriptions:
            self.token_subscriptions[token_address]['subscribers'].add(sid)
            if self.token_subscriptions[token_address]['last_price']:
                self.send_price_update(sid, token_address)
            return

        # Initialize with empty metadata
        self.token_subscriptions[token_address] = {
            'last_price': 0,
            'subscribers': {sid},
            'token_name': None,
            'token_symbol': None
        }

        # Fetch metadata once when we start tracking
        self.fetch_token_metadata(token_address)

        # Only start a new update loop if we don't have one running
        if len(self.token_subscriptions) == 1:
            self.start_price_updates()

    def fetch_token_metadata(self, token_address: str) -> None:
        try:
            response = requests.get(f"{self.API_URL}/ids?mints={token_address}")
            if response.status_code != 200:
                print(f"Failed to fetch token metadata: HTTP {response.status_code}")
                return

            data = response.json()
            if not data.get('data'):
                print(f"No metadata found for token {token_address}")
                return

            token_info = data['data'][0] if isinstance(data['data'], list) else None
            if token_info:
                self.token_subscriptions[token_address].update({
                    'token_name': token_info.get('name'),
                    'token_symbol': token_info.get('symbol')
                })
        except Exception as e:
            print(f"Error fetching token metadata: {e}")

    def stop_tracking(self, token_address: str, sid: str) -> None:
        if token_address not in self.token_subscriptions:
            return

        self.token_subscriptions[token_address]['subscribers'].discard(sid)
        if not self.token_subscriptions[token_address]['subscribers']:
            del self.token_subscriptions[token_address]

    def start_price_updates(self) -> None:
        def update_loop():
            while self.token_subscriptions:
                self.update_prices()
                eventlet.sleep(self.price_interval)

        eventlet.spawn(update_loop)

    def update_prices(self) -> None:
        try:
            # Get all token addresses that need updating
            token_addresses = list(self.token_subscriptions.keys())
            if not token_addresses:
                return

            # Convert addresses to comma-separated string and encode
            addresses_string = ','.join(token_addresses)
            encoded_addresses = requests.utils.quote(addresses_string)

            # Fetch prices
            response = requests.get(f"{self.API_URL}/price?mints={encoded_addresses}")

            if response.status_code != 200:
                print(f"HTTP error! status: {response.status_code}")
                return

            price_data = response.json()
            
            # Update subscriptions with new prices
            for token_address in token_addresses:
                subscription = self.token_subscriptions[token_address]
                price = float(price_data.get('data', {}).get(token_address, 0))
                subscription['last_price'] = price
                self.broadcast_price(token_address)

        except Exception as e:
            print(f"Price update failed: {e}")

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