import os
from datetime import datetime, timezone, timedelta
import jwt
from solders.pubkey import Pubkey
from solders.signature import Signature


class AuthManager:
    def __init__(self):
        self.SECRET_KEY = os.getenv('JWT_SECRET')
        if not self.SECRET_KEY:
            raise ValueError("JWT_SECRET must be set in environment variables")

    def generate_token(self, wallet_address: str) -> str:
        payload = {
            'wallet_address': wallet_address,
            'exp': datetime.now(timezone.utc) + timedelta(days=1),
            'iat': datetime.now(timezone.utc),
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
            pubkey = Pubkey.from_string(wallet_address)
            signature_bytes = bytes.fromhex(signature)
            signature_obj = Signature.from_bytes(signature_bytes)
            message_bytes = message.encode('utf-8')
            return signature_obj.verify(pubkey, message_bytes)
        except Exception as e:
            print(f"Signature verification failed for {wallet_address}: {e}")
            return False
