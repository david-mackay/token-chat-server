import os
from datetime import datetime, timezone, timedelta
import jwt
from solders.pubkey import Pubkey
from solders.signature import Signature

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
            
            try:
                pubkey = Pubkey.from_string(wallet_address)
                print(f"[Signature Verification] Pubkey created: {pubkey}")
            except Exception as e:
                print(f"[Signature Verification] Failed to parse pubkey: {e}")
                return False

            try:
                signature_bytes = bytes.fromhex(signature)
                signature_obj = Signature.from_bytes(signature_bytes)
                print(f"[Signature Verification] Signature object created")
            except Exception as e:
                print(f"[Signature Verification] Failed to create signature object: {e}")
                return False

            try:
                message_bytes = message.encode('utf-8')
                print(f"[Signature Verification] Message: {message}")
                print(f"[Signature Verification] Message bytes length: {len(message_bytes)}")
            except Exception as e:
                print(f"[Signature Verification] Failed to encode message: {e}")
                return False

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