import os
import time
import requests
import json
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from typing import Dict, Any, List, Optional, TypedDict
from dataclasses import dataclass
from datetime import datetime, timedelta


class ProfileData(TypedDict, total=False):
    """Type definition for profile data from external API"""
    profile: Dict[str, Any]
    walletAddress: str
    username: Optional[str]
    bio: Optional[str]
    image: Optional[str]
    socialCounts: Optional[Dict[str, int]]
    namespace: Optional[Dict[str, str]]


class Message(TypedDict):
    """Type definition for chat messages"""
    id: str
    tokenAddress: str
    walletAddress: str
    content: str
    timestamp: str
    colorCode: Optional[str]
    # Will be populated with profile data if available
    profile: Optional[ProfileData]


@dataclass
class CachedProfile:
    """Class for caching profile data with expiration"""
    data: ProfileData
    timestamp: float  # Unix timestamp when cache was created
    
    def is_expired(self, cache_ttl: int) -> bool:
        """Check if the cached profile has expired"""
        return time.time() - self.timestamp > cache_ttl


class ChatManager:
    def __init__(self, dynamodb_resource, socketio):
        self.MESSAGE_LIMIT = 500
        self.PROFILE_CACHE_TTL = 3600  # Cache profiles for 1 hour (in seconds)
        self.table_name = os.getenv('DYNAMODB_TABLE_NAME', 'token-chat-server')
        self.table = dynamodb_resource.Table(self.table_name)
        self.chat_rooms: Dict[str, Dict[str, Any]] = {}
        self.socketio = socketio
        self.profile_cache: Dict[str, CachedProfile] = {}
        self.api_key = os.getenv('TAPESTRY_API_KEY', '')
        self.api_base_url = 'https://api.usetapestry.dev/api/v1/profiles'

    def get_recent_messages(self, token_address: str) -> List[Message]:
        """
        Get recent messages for a token and enrich them with profile data
        """
        try:
            # Get messages from DynamoDB
            response = self.table.query(
                KeyConditionExpression=Key('tokenAddress').eq(token_address),
                ScanIndexForward=True,
                Limit=self.MESSAGE_LIMIT
            )
            messages = response.get('Items', [])
            
            # Extract unique wallet addresses
            wallet_addresses = set(msg.get('walletAddress') for msg in messages if 'walletAddress' in msg)
            
            # Batch fetch any missing profiles
            self._prefetch_profiles(wallet_addresses)
            
            # Enrich messages with profile data
            for message in messages:
                wallet = message.get('walletAddress')
                if wallet and wallet in self.profile_cache:
                    message['profile'] = self.profile_cache[wallet].data
            
            return messages
            
        except ClientError as e:
            print(f"Error querying messages: {e}")
            return []

    def handle_message(self, message: Message) -> None:
        """
        Process and store an incoming message
        """
        try:
            wallet_address = message.get('walletAddress')
            
            # Fetch profile if not in cache
            if wallet_address and self.api_key:
                profile = self._get_profile(wallet_address)
                if profile:
                    message['profile'] = profile
            
            # Insert into DynamoDB
            self.table.put_item(Item=message)
            
            # Broadcast to room
            self.socketio.emit('chat_message', {
                'type': 'chat_message',
                'data': message
            }, room=message['tokenAddress'])

        except ClientError as e:
            print(f"Error handling message: {e}")
            raise e

    def _get_profile(self, wallet_address: str) -> Optional[ProfileData]:
        """
        Get profile data for a wallet address (from cache or API)
        """
        # Check cache first
        if wallet_address in self.profile_cache:
            cached = self.profile_cache[wallet_address]
            if not cached.is_expired(self.PROFILE_CACHE_TTL):
                return cached.data
        
        # Fetch from API if not in cache or expired
        return self._fetch_profile(wallet_address)

    def _fetch_profile(self, wallet_address: str) -> Optional[ProfileData]:
        """
        Fetch profile data from external API
        """
        if not self.api_key:
            print("No API key configured for profile service")
            return None
            
        try:
            url = f"{self.api_base_url}/{wallet_address}"
            params = {"apiKey": self.api_key}
            
            response = requests.get(url, params=params, timeout=5)
            print(response)
            if response.status_code == 200:
                profile_data = response.json()
                
                # Cache the result
                self.profile_cache[wallet_address] = CachedProfile(
                    data=profile_data,
                    timestamp=time.time()
                )
                
                return profile_data
            else:
                print(f"Failed to fetch profile for {wallet_address}. Status: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error fetching profile for {wallet_address}: {e}")
            return None

    def _prefetch_profiles(self, wallet_addresses: set) -> None:
        """
        Prefetch profiles for a set of wallet addresses
        Only fetches profiles not in cache or that have expired
        """
        addresses_to_fetch = []
        
        for address in wallet_addresses:
            if address not in self.profile_cache or self.profile_cache[address].is_expired(self.PROFILE_CACHE_TTL):
                addresses_to_fetch.append(address)
        
        # Fetch profiles in batches to avoid overwhelming the API
        # TODO: If the API supports batch fetching, use that instead
        batch_size = 10
        for i in range(0, len(addresses_to_fetch), batch_size):
            batch = addresses_to_fetch[i:i+batch_size]
            for address in batch:
                self._fetch_profile(address)
            
            # Small delay between batches to be nice to the API
            if i + batch_size < len(addresses_to_fetch):
                time.sleep(0.5)

    def clear_expired_cache(self) -> None:
        """
        Clear expired entries from the profile cache
        Can be called periodically to free up memory
        """
        expired_keys = []
        
        for key, cached in self.profile_cache.items():
            if cached.is_expired(self.PROFILE_CACHE_TTL):
                expired_keys.append(key)
                
        for key in expired_keys:
            del self.profile_cache[key]

    def get_cached_profile_count(self) -> int:
        """
        Get the number of profiles in the cache
        """
        return len(self.profile_cache)