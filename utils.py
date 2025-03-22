from datetime import datetime, timezone

def rate_limit_messages(user_message_counts, wallet_address):
    current_time = datetime.now(timezone.utc)
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
    from functools import wraps

    @wraps(f)
    def decorated(*args, **kwargs):
        data = args[0]

        if not isinstance(data, dict):
            print("Data is not a dictionary")
            return {'success': False, 'message': 'Authentication Required'}

        session_token = data.get('session_token')
        if not session_token:
            print(f"No session token in data. Keys available: {data.keys()}")
            return {'success': False, 'message': 'Authentication Required'}

        try:
            payload = auth_manager.verify_token(session_token)
            return f(data, payload)
        except ValueError as e:
            print(f"Token verification error: {e}")
            return {'success': False, 'message': 'Authentication Required'}

    return decorated