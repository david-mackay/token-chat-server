from typing import Optional


VALID_MESSAGE_TYPES = {"text", "gift_token", "gift_nft"}
GIFT_TYPES = {"gift_token", "gift_nft"}

GIFT_FIELD_MAP = {
    'imageUrl': 'media_url',
    'amount': 'amount',
    'transactionHash': 'transaction_hash',
    'mintAddress': 'mint_address',
    'tokenSymbol': 'token_symbol',
    'tokenName': 'token_name',
    'nftName': 'nft_name',
}

# Messages are stored with sender_wallet / receiver_wallet directly.
# Profile enrichment (display name, avatar) is Tapestry's job — done at the
# frontend layer.  The server only stores and queries raw message rows.
MESSAGE_COLUMNS = (
    'id, sender_wallet, receiver_wallet, content, type, media_url, amount, '
    'transaction_hash, mint_address, token_symbol, token_name, nft_name, '
    'created_at, is_read'
)


class ChatManager:
    def __init__(self, supabase_client, sio):
        self.supabase = supabase_client
        self.sio = sio

    @staticmethod
    def make_conversation_id(wallet_a: str, wallet_b: str) -> str:
        """Deterministic ID for Socket.IO room names."""
        return "_".join(sorted([wallet_a, wallet_b]))

    async def get_recent_messages(
        self, my_wallet: str, other_wallet: str, limit: int = 50
    ) -> list:
        response = await (
            self.supabase.table('messages')
            .select(MESSAGE_COLUMNS)
            .or_(
                f'and(sender_wallet.eq.{my_wallet},receiver_wallet.eq.{other_wallet}),'
                f'and(sender_wallet.eq.{other_wallet},receiver_wallet.eq.{my_wallet})'
            )
            .order('created_at')
            .limit(limit)
            .execute()
        )
        return response.data or []

    async def save_message(
        self,
        sender_wallet: str,
        recipient_wallet: str,
        content: Optional[str] = None,
        msg_type: str = "text",
        gift_data: Optional[dict] = None,
    ) -> dict:
        if msg_type not in VALID_MESSAGE_TYPES:
            raise ValueError(f"Invalid message type: {msg_type}")

        if msg_type in GIFT_TYPES:
            if not gift_data:
                raise ValueError("Gift messages require giftData")
            if not gift_data.get("transactionHash"):
                raise ValueError("Gift messages require a transactionHash")
            if not gift_data.get("mintAddress"):
                raise ValueError("Gift messages require a mintAddress")

        if msg_type == "text" and not content:
            raise ValueError("Text messages require content")

        row: dict = {
            'sender_wallet': sender_wallet,
            'receiver_wallet': recipient_wallet,
            'content': content,
            'type': msg_type,
            'is_read': False,
        }

        if gift_data:
            for client_key, db_col in GIFT_FIELD_MAP.items():
                val = gift_data.get(client_key)
                if val is not None:
                    row[db_col] = str(val) if db_col == 'amount' else val

        insert_resp = await self.supabase.table('messages').insert(row).execute()
        return insert_resp.data[0]

    async def get_user_conversations(self, wallet_address: str) -> list:
        response = await (
            self.supabase.table('messages')
            .select(MESSAGE_COLUMNS)
            .or_(
                f'sender_wallet.eq.{wallet_address},'
                f'receiver_wallet.eq.{wallet_address}'
            )
            .order('created_at', desc=True)
            .limit(500)
            .execute()
        )

        conversations: dict[str, dict] = {}
        for msg in (response.data or []):
            if msg.get('sender_wallet') == wallet_address:
                partner = msg['receiver_wallet']
            else:
                partner = msg['sender_wallet']

            if not partner:
                continue

            if partner not in conversations:
                conversations[partner] = {
                    'partnerWallet': partner,
                    'lastMessage': msg.get('content'),
                    'lastMessageType': msg.get('type'),
                    'lastMessageAt': msg.get('created_at'),
                    'lastSenderWallet': msg.get('sender_wallet'),
                    'unreadCount': 0,
                }

            if msg.get('receiver_wallet') == wallet_address and not msg.get('is_read'):
                conversations[partner]['unreadCount'] = (
                    conversations[partner].get('unreadCount', 0) + 1
                )

        return sorted(
            conversations.values(),
            key=lambda c: c.get('lastMessageAt', ''),
            reverse=True,
        )

    async def mark_messages_read(self, my_wallet: str, other_wallet: str) -> int:
        response = await (
            self.supabase.table('messages')
            .update({'is_read': True})
            .eq('receiver_wallet', my_wallet)
            .eq('sender_wallet', other_wallet)
            .eq('is_read', False)
            .execute()
        )
        return len(response.data) if response.data else 0
