import os
import functools
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs

from dotenv import load_dotenv

# Load env from token-chat-server dir regardless of CWD
_app_dir = Path(__file__).resolve().parent
import socketio
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from supabase import acreate_client

from auth import AuthManager
from chat import ChatManager

load_dotenv(_app_dir / '.env.local')
load_dotenv(_app_dir / '.env')

auth_manager = AuthManager()

sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*',
    logger=True,
    engineio_logger=True,
    ping_timeout=60,
)

chat_manager: ChatManager | None = None

# ── Per-user in-memory rate limiting ─────────────────────────────────────────

user_message_counts: dict[str, list[datetime]] = {}


def rate_limit_messages(wallet_address: str) -> bool:
    now = datetime.now()
    timestamps = user_message_counts.get(wallet_address, [])
    timestamps = [t for t in timestamps if (now - t).total_seconds() < 60]

    if len(timestamps) >= 60:
        return False

    timestamps.append(now)
    user_message_counts[wallet_address] = timestamps
    return True

# ── Auth decorator for Socket.IO events ──────────────────────────────────────


def require_auth(f):
    @functools.wraps(f)
    async def decorated(sid, data):
        if not isinstance(data, dict):
            await sio.emit('auth_required', to=sid)
            return

        session_token = data.get('session_token')
        if not session_token:
            await sio.emit('auth_required', to=sid)
            return

        try:
            payload = auth_manager.verify_token(session_token)
            return await f(sid, data, payload)
        except ValueError:
            await sio.emit('auth_required', to=sid)
            return
    return decorated

# ── Socket.IO events ────────────────────────────────────────────────────────


@sio.event
async def connect(sid, environ, auth=None):
    try:
        # Engine.IO passes WSGI-style environ with QUERY_STRING (already decoded)
        query_string = environ.get('QUERY_STRING', '') or ''
        if isinstance(query_string, bytes):
            query_string = query_string.decode()
        params = parse_qs(query_string)

        token = params.get('token', [None])[0]

        if token:
            try:
                payload = auth_manager.verify_token(token)
                wallet_address = payload['wallet_address']
            except ValueError:
                return False
        else:
            auth_message = params.get('auth_message', [None])[0]
            signature = params.get('signature', [None])[0]
            wallet_address = params.get('wallet_address', [None])[0]

            if not all([auth_message, signature, wallet_address]):
                return False

            if not auth_manager.verify_signature(auth_message, signature, wallet_address):
                return False

        session_token = auth_manager.generate_token(wallet_address)
        await sio.emit('auth_success', {'session_token': session_token}, to=sid)
        return True
    except Exception as e:
        print(f"Connection error: {e}")
        return False


@sio.event
async def disconnect(sid):
    pass


@sio.on('join_conversation')
@require_auth
async def handle_join_conversation(sid, data, auth_payload):
    try:
        other_wallet = data.get('otherWallet')
        if not other_wallet:
            await sio.emit('error', {'message': 'otherWallet is required'}, to=sid)
            return

        my_wallet = auth_payload['wallet_address']
        conversation_id = ChatManager.make_conversation_id(my_wallet, other_wallet)

        await sio.enter_room(sid, conversation_id)

        messages = await chat_manager.get_recent_messages(my_wallet, other_wallet)
        await sio.emit('message_history', {
            'conversationId': conversation_id,
            'messages': messages,
        }, to=sid)
    except Exception as e:
        print(f"Error joining conversation: {e}")
        await sio.emit('error', {'message': 'Failed to join conversation'}, to=sid)


@sio.on('leave_conversation')
@require_auth
async def handle_leave_conversation(sid, data, auth_payload):
    try:
        other_wallet = data.get('otherWallet')
        if not other_wallet:
            return

        my_wallet = auth_payload['wallet_address']
        conversation_id = ChatManager.make_conversation_id(my_wallet, other_wallet)
        await sio.leave_room(sid, conversation_id)
    except Exception as e:
        print(f"Error leaving conversation: {e}")


@sio.on('send_message')
@require_auth
async def handle_send_message(sid, data, auth_payload):
    try:
        sender = auth_payload['wallet_address']

        if not rate_limit_messages(sender):
            await sio.emit('error', {'message': 'Rate limit exceeded. Please wait.'}, to=sid)
            return

        recipient = data.get('recipientWallet')
        content = data.get('content')

        if not recipient:
            await sio.emit('error', {'message': 'recipientWallet is required'}, to=sid)
            return
        if not content:
            await sio.emit('error', {'message': 'content is required'}, to=sid)
            return

        conversation_id = ChatManager.make_conversation_id(sender, recipient)
        message = await chat_manager.save_message(
            sender_wallet=sender,
            recipient_wallet=recipient,
            content=content,
            msg_type='text',
        )

        await sio.emit('new_message', {
            'conversationId': conversation_id,
            'message': message,
        }, room=conversation_id)

    except Exception as e:
        print(f"Error sending message: {e}")
        await sio.emit('error', {'message': 'Failed to send message'}, to=sid)


@sio.on('send_gift')
@require_auth
async def handle_send_gift(sid, data, auth_payload):
    try:
        sender = auth_payload['wallet_address']

        if not rate_limit_messages(sender):
            await sio.emit('error', {'message': 'Rate limit exceeded. Please wait.'}, to=sid)
            return

        recipient = data.get('recipientWallet')
        msg_type = data.get('type')

        if not recipient:
            await sio.emit('error', {'message': 'recipientWallet is required'}, to=sid)
            return
        if msg_type not in ('gift_token', 'gift_nft'):
            await sio.emit('error', {'message': 'type must be gift_token or gift_nft'}, to=sid)
            return

        gift_data = {
            'transactionHash': data.get('transactionHash'),
            'mintAddress': data.get('mintAddress'),
            'amount': data.get('amount'),
            'tokenSymbol': data.get('tokenSymbol'),
            'tokenName': data.get('tokenName'),
            'nftName': data.get('nftName'),
            'imageUrl': data.get('imageUrl'),
        }

        conversation_id = ChatManager.make_conversation_id(sender, recipient)
        message = await chat_manager.save_message(
            sender_wallet=sender,
            recipient_wallet=recipient,
            content=data.get('content'),
            msg_type=msg_type,
            gift_data=gift_data,
        )

        await sio.emit('new_message', {
            'conversationId': conversation_id,
            'message': message,
        }, room=conversation_id)

    except ValueError as e:
        await sio.emit('error', {'message': str(e)}, to=sid)
    except Exception as e:
        print(f"Error sending gift: {e}")
        await sio.emit('error', {'message': 'Failed to send gift'}, to=sid)


@sio.on('mark_read')
@require_auth
async def handle_mark_read(sid, data, auth_payload):
    try:
        other_wallet = data.get('otherWallet')
        if not other_wallet:
            await sio.emit('error', {'message': 'otherWallet is required'}, to=sid)
            return

        my_wallet = auth_payload['wallet_address']
        conversation_id = ChatManager.make_conversation_id(my_wallet, other_wallet)
        count = await chat_manager.mark_messages_read(my_wallet, other_wallet)

        if count > 0:
            await sio.emit('messages_read', {
                'conversationId': conversation_id,
                'readerWallet': my_wallet,
                'count': count,
            }, room=conversation_id)
    except Exception as e:
        print(f"Error marking read: {e}")
        await sio.emit('error', {'message': 'Failed to mark messages as read'}, to=sid)

# ── REST endpoints ───────────────────────────────────────────────────────────


async def get_conversations(request):
    wallet = request.query_params.get('wallet')
    auth_header = request.headers.get('Authorization', '')

    if not wallet or not auth_header.startswith('Bearer '):
        return JSONResponse(
            {'error': 'wallet query param and Bearer token required'},
            status_code=400,
        )

    try:
        token = auth_header.split(' ', 1)[1]
        payload = auth_manager.verify_token(token)
        if payload['wallet_address'] != wallet:
            return JSONResponse({'error': 'Token does not match wallet'}, status_code=403)
    except ValueError as e:
        return JSONResponse({'error': str(e)}, status_code=401)

    conversations = await chat_manager.get_user_conversations(wallet)
    return JSONResponse({'conversations': conversations})


async def health_check(request):
    return JSONResponse({'status': 'ok'})

# ── App setup ────────────────────────────────────────────────────────────────


async def startup():
    global chat_manager
    supabase_client = await acreate_client(
        os.getenv('NEXT_PUBLIC_SUPABASE_URL'),
        os.getenv('SUPABASE_SERVICE_ROLE_KEY'),
    )
    chat_manager = ChatManager(supabase_client, sio)


http_app = Starlette(
    routes=[
        Route('/conversations', get_conversations),
        Route('/health', health_check),
    ],
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=['*'],
            allow_methods=['*'],
            allow_headers=['*'],
        ),
    ],
    on_startup=[startup],
)

app = socketio.ASGIApp(sio, other_asgi_app=http_app)

# ── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import uvicorn
    port = int(os.getenv('PORT', 3001))
    uvicorn.run('app:app', host='0.0.0.0', port=port, reload=True)
