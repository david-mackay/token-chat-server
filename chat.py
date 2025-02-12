import os
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from typing import Dict, Any

class ChatManager:
    def __init__(self, dynamodb_resource, socketio):
        self.MESSAGE_LIMIT = 500
        self.table_name = os.getenv('DYNAMODB_TABLE_NAME', 'token-chat-server')
        self.table = dynamodb_resource.Table(self.table_name)
        self.chat_rooms: Dict[str, Dict[str, Any]] = {}
        self.socketio = socketio

    def get_recent_messages(self, token_address: str) -> list:
        try:
            response = self.table.query(
                KeyConditionExpression=Key('tokenAddress').eq(token_address),
                ScanIndexForward=True,
                Limit=self.MESSAGE_LIMIT
            )
            return response.get('Items', [])
        except ClientError as e:
            print(f"Error querying messages: {e}")
            return []

    def handle_message(self, message: dict) -> None:
        try:
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