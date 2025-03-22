import json
import uuid
import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set, Any, Tuple
from anthropic import Anthropic
import boto3
from boto3.dynamodb.conditions import Key
import threading
import re
import random
import os
from botocore.exceptions import ClientError


class ClaudeTradingBot:
    """
    A trading bot that monitors chat conversations, analyzes messages, and executes trades
    based on user interactions. The bot has a personality and selectively engages with users
    in the chat room.
    """
    
    def __init__(self, 
                 dynamodb_resource,
                 socketio,
                 api_key: str,
                 initial_balance: float = 100000.0,
                 table_name: str = 'crypto-trading-bot',
                 portfolio_table: str = 'crypto-portfolio',
                 chat_history_table: str = 'token-chat-server'):
        """
        Initialize the ClaudeTradingBot.
        
        Args:
            dynamodb_resource: boto3 dynamodb resource
            socketio: Flask SocketIO instance
            api_key: Anthropic API key for Claude
            initial_balance: Starting cash balance for paper trading
            table_name: DynamoDB table name for trade records
            portfolio_table: DynamoDB table name for portfolio data
            chat_history_table: DynamoDB table name for chat history
        """
        # Initialize Claude client
        self.claude = Anthropic(api_key=api_key)
        
        # Initialize DynamoDB resources
        self.dynamodb = dynamodb_resource
        self.trade_table = self.dynamodb.Table(table_name)
        self.portfolio_table = self.dynamodb.Table(portfolio_table)
        self.chat_table = self.dynamodb.Table(chat_history_table)
        
        # Initialize Flask-SocketIO
        self.socketio = socketio
        
        # Portfolio tracking
        self.cash_balance = initial_balance
        self.holdings = {}  # Symbol -> quantity
        self.initial_balance = initial_balance
        
        # Bot personality and state
        self.personality = {
            'name': 'TradeBot',
            'response_rate': 0.3,  # Probability of responding to non-direct messages
            'risk_tolerance': 0.5,  # 0-1 scale, higher means more aggressive trading
            'curiosity': 0.7,  # 0-1 scale, higher means more likely to ask questions
            'humor': 0.6,  # 0-1 scale, higher means more jokes and lighthearted responses
            'expertise_focus': ['BTC', 'ETH', 'SOL'],  # Tokens the bot is especially knowledgeable about
            'activity_hours': [(0, 23)],  # 24-hour format, multiple ranges allowed
            'cooldown_period': 60,  # Seconds to wait between messages in same room
        }
        
        # Tracking state
        self.active_rooms = set()  # Set of token addresses the bot is actively monitoring
        self.last_message_time = {}  # token_address -> timestamp
        self.mentioned_tokens = {}  # token_address -> {mention_count, last_mention_time}
        self.direct_queries = {}  # track if users have directly addressed the bot
        self.last_trade_time = {}  # token_address -> timestamp
        
        # Available tokens for trading
        self.available_tokens = {
            "SOL": {
                "name": "Solana",
                "address": "So11111111111111111111111111111111111111112"
            },
            "HELP": {
                "name": "Helper",
                "address": "HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"
            },
            "BNKR": {
                "name": "Bankroll",
                "address": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
            }
        }
        
        # Map token addresses to symbols
        self.address_to_symbol = {v["address"]: k for k, v in self.available_tokens.items()}
        
        # Conversation memory
        self.conversation_context = {}  # token_address -> list of recent messages
        self.max_context_messages = 10  # Number of messages to keep for context
        
        # User influence tracking
        self.user_influence = {}  # wallet_address -> influence score
        
        # Start the background tasks
        self.initialize_tables()
        self.load_portfolio()
        self.bot_thread = threading.Thread(target=self.start_background_tasks)
        self.bot_thread.daemon = True
        self.bot_thread.start()

    def initialize_tables(self):
        """Initialize DynamoDB tables if they don't exist."""
        # Check if trade table exists
        try:
            self.trade_table.table_status
        except:
            print(f"Creating trade table: {self.trade_table.name}")
            self.dynamodb.create_table(
                TableName=self.trade_table.name,
                KeySchema=[
                    {'AttributeName': 'trade_id', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'trade_id', 'AttributeType': 'S'},
                    {'AttributeName': 'token_address', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'token-index',
                        'KeySchema': [
                            {'AttributeName': 'token_address', 'KeyType': 'HASH'},
                            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        
        # Check if portfolio table exists
        try:
            self.portfolio_table.table_status
        except:
            print(f"Creating portfolio table: {self.portfolio_table.name}")
            self.dynamodb.create_table(
                TableName=self.portfolio_table.name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

    def load_portfolio(self):
        """Load the portfolio from DynamoDB."""
        try:
            response = self.portfolio_table.get_item(Key={'id': 'bot_portfolio'})
            if 'Item' in response:
                portfolio = response['Item']
                self.cash_balance = float(portfolio.get('cash_balance', self.initial_balance))
                self.holdings = {k: float(v) for k, v in portfolio.get('holdings', {}).items()}
                self.initial_balance = float(portfolio.get('initial_balance', self.initial_balance))
            else:
                # Initialize portfolio
                self.save_portfolio()
        except Exception as e:
            print(f"Error loading portfolio: {e}")
            # Initialize portfolio
            self.save_portfolio()

    def save_portfolio(self):
        """Save the portfolio to DynamoDB."""
        try:
            portfolio = {
                'id': 'bot_portfolio',
                'cash_balance': Decimal(str(self.cash_balance)),
                'holdings': {k: Decimal(str(v)) for k, v in self.holdings.items()},
                'initial_balance': Decimal(str(self.initial_balance)),
                'timestamp': int(time.time())
            }
            self.portfolio_table.put_item(Item=portfolio)
        except Exception as e:
            print(f"Error saving portfolio: {e}")

    def start_background_tasks(self):
        """Start background tasks for the bot."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_tasks():
            while True:
                try:
                    await self.check_for_trade_opportunities()
                    await asyncio.sleep(30)  # Check for trade opportunities every 30 seconds
                except Exception as e:
                    print(f"Error in background tasks: {e}")
                    await asyncio.sleep(60)  # Wait a bit longer if there was an error
        
        loop.run_until_complete(run_tasks())

    def join_room(self, token_address: str):
        """Join a chat room to start monitoring."""
        self.active_rooms.add(token_address)
        self.last_message_time[token_address] = time.time()
        self.conversation_context[token_address] = []
        
        # Get recent messages to build context
        self.load_conversation_context(token_address)
        
        print(f"Bot joined room: {token_address}")

    def leave_room(self, token_address: str):
        """Leave a chat room."""
        if token_address in self.active_rooms:
            self.active_rooms.remove(token_address)
        
        if token_address in self.last_message_time:
            del self.last_message_time[token_address]
        
        if token_address in self.conversation_context:
            del self.conversation_context[token_address]
        
        print(f"Bot left room: {token_address}")

    def load_conversation_context(self, token_address: str):
        """Load recent conversation context from DynamoDB."""
        try:
            response = self.chat_table.query(
                KeyConditionExpression=Key('tokenAddress').eq(token_address),
                ScanIndexForward=False,  # Descending order (newest first)
                Limit=self.max_context_messages
            )
            
            # Store messages in chronological order
            messages = response.get('Items', [])
            messages.reverse()
            
            self.conversation_context[token_address] = messages
        except Exception as e:
            print(f"Error loading conversation context: {e}")
            self.conversation_context[token_address] = []

    async def _fetch_price(self, symbol: str) -> float:
        """
        Fetch current price for a symbol using Jupiter API.
        
        Args:
            symbol: The trading symbol (e.g., 'SOL', 'HELP')
            
        Returns:
            Current price of the symbol in USD
        """
        try:
            # Get token address from symbol
            token_address = None
            if symbol in self.available_tokens:
                token_address = self.available_tokens[symbol]['address']
            elif symbol in self.address_to_symbol:
                token_address = symbol
            
            if not token_address:
                print(f"No token address found for symbol {symbol}")
                # Fallback to default values if we can't get the real price
                default_prices = {
                    'SOL': 120.0,
                    'HELP': 0.05,
                    'BNKR': 0.5
                }
                return default_prices.get(symbol.upper(), 1.0)
                
            # Make request to Jupiter API
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                url = f'https://api.jup.ag/price/v2?ids={token_address}'
                async with session.get(url) as response:
                    if response.status == 200:
                        price_data = await response.json()
                        
                        # Extract price from response
                        if token_address in price_data and 'price' in price_data[token_address]:
                            return float(price_data[token_address]['price'])
                    
                    # If we get here, something went wrong with the request
                    print(f"Error fetching price from Jupiter API: {response.status}")
            
            # Fallback to default values if API request fails
            default_prices = {
                'SOL': 120.0,
                'HELP': 0.05,
                'BNKR': 0.5
            }
            return default_prices.get(symbol.upper(), 1.0)
            
        except Exception as e:
            print(f"Error fetching price: {e}")
            # Fallback values in case of error
            default_prices = {
                'SOL': 120.0,
                'HELP': 0.05,
                'BNKR': 0.5
            }
            return default_prices.get(symbol.upper(), 1.0)

    def execute_trade(self, 
                      token_address: str, 
                      action: str, 
                      quantity: float, 
                      influencers: Optional[List[str]] = None,
                      reason: str = "") -> Dict:
        """
        Execute a trade and record it in DynamoDB.
        
        Args:
            token_address: Token address
            action: 'buy' or 'sell'
            quantity: Amount to buy or sell
            influencers: List of wallet addresses who influenced this trade
            reason: Explanation for the trade decision
            
        Returns:
            Trade record details
        """
        # Get symbol from token address
        symbol = self.address_to_symbol.get(token_address, token_address)
        
        # Standardize inputs
        action = action.lower()
        
        if action not in ['buy', 'sell']:
            raise ValueError("Action must be 'buy' or 'sell'")
        
        # Get current price
        price = self._fetch_price(symbol)
        
        # Calculate trade value
        trade_value = price * quantity
        
        # Execute trade
        if action == 'buy':
            # Ensure sufficient funds
            if trade_value > self.cash_balance:
                raise ValueError(f"Insufficient funds. Required: ${trade_value:.2f}, Available: ${self.cash_balance:.2f}")
            
            # Update portfolio
            self.cash_balance -= trade_value
            if symbol in self.holdings:
                self.holdings[symbol] += quantity
            else:
                self.holdings[symbol] = quantity
        else:  # sell
            # Ensure sufficient holdings
            if symbol not in self.holdings or self.holdings[symbol] < quantity:
                current_holding = self.holdings.get(symbol, 0)
                raise ValueError(f"Insufficient holdings. Required: {quantity} {symbol}, Available: {current_holding} {symbol}")
            
            # Update portfolio
            self.cash_balance += trade_value
            self.holdings[symbol] -= quantity
            
            # Remove symbol if no more holdings
            if self.holdings[symbol] == 0:
                del self.holdings[symbol]
        
        # Generate trade record
        trade_id = str(uuid.uuid4())
        timestamp = int(time.time())
        
        trade_record = {
            'trade_id': trade_id,
            'token_address': token_address,
            'symbol': symbol,
            'timestamp': timestamp,
            'action': action,
            'quantity': Decimal(str(quantity)),
            'price': Decimal(str(price)),
            'trade_value': Decimal(str(trade_value)),
            'record_type': 'trade'
        }
        
        # Add optional fields if provided
        if influencers:
            trade_record['influencers'] = influencers
            
        if reason:
            trade_record['reason'] = reason
        
        # Save trade to DynamoDB
        self.trade_table.put_item(Item=trade_record)
        
        # Update last trade time
        self.last_trade_time[token_address] = timestamp
        
        # Update portfolio in DB
        self.save_portfolio()
        
        # Update user influence scores
        if influencers:
            for wallet_address in influencers:
                if wallet_address not in self.user_influence:
                    self.user_influence[wallet_address] = {'positive': 0, 'negative': 0, 'trades': []}
                
                self.user_influence[wallet_address]['trades'].append(trade_id)
        
        # Convert Decimal back to float for return
        for key in trade_record:
            if isinstance(trade_record[key], Decimal):
                trade_record[key] = float(trade_record[key])
        
        # Announce the trade in the chat
        self.send_trade_message(token_address, trade_record)
        
        return trade_record

    def send_trade_message(self, token_address: str, trade_record: Dict):
        """Send a message to the chat announcing a trade."""
        symbol = trade_record['symbol']
        action = trade_record['action']
        quantity = trade_record['quantity']
        price = trade_record['price']
        value = trade_record['trade_value']
        
        message = {
            'id': str(uuid.uuid4()),
            'tokenAddress': token_address,
            'walletAddress': 'TRADEBOT',  # The bot's identifier
            'displayName': self.personality['name'],
            'text': f"🤖 I just {action.upper()}ED {quantity} {symbol} @ ${price:.6f} (${value:.2f} total)",
            'timestamp': datetime.now().isoformat(),
            'verified': True,
            'botMessage': True
        }
        
        # If there's a reason, include it
        if 'reason' in trade_record and trade_record['reason']:
            message['text'] += f"\n\nReason: {trade_record['reason']}"
        
        # Emit to the room
        self.socketio.emit('chat_message', {
            'type': 'chat_message',
            'data': message
        }, room=token_address)

    def handle_message(self, message: Dict):
        """
        Process an incoming chat message and decide whether to respond.
        
        Args:
            message: The chat message
        """
        token_address = message.get('tokenAddress')
        wallet_address = message.get('walletAddress')
        text = message.get('text', '')
        
        # Skip if it's our own message or missing critical data
        if (wallet_address == 'TRADEBOT' or 
            not token_address or
            not wallet_address or
            not text or
            token_address not in self.active_rooms):
            return
        
        # Add message to conversation context
        if token_address in self.conversation_context:
            self.conversation_context[token_address].append(message)
            # Keep only the most recent messages
            if len(self.conversation_context[token_address]) > self.max_context_messages:
                self.conversation_context[token_address] = self.conversation_context[token_address][-self.max_context_messages:]
        
        # Check for direct mentions of the bot
        is_direct_mention = self.is_bot_mentioned(text)
        
        # Update direct query tracking
        if is_direct_mention:
            if token_address not in self.direct_queries:
                self.direct_queries[token_address] = set()
            self.direct_queries[token_address].add(wallet_address)
        
        # Track token mentions
        self.track_token_mentions(token_address, text)
        
        # Decide whether to respond
        should_respond = self.should_respond_to_message(token_address, is_direct_mention)
        
        if should_respond:
            asyncio.run(self.generate_response(token_address, message))

    def is_bot_mentioned(self, text: str) -> bool:
        """Check if the bot is directly mentioned in the message."""
        bot_name = self.personality['name']
        bot_mentions = [
            f"@{bot_name}",
            f"@tradebot",
            f"hey {bot_name}",
            f"hey tradebot",
            f"hi {bot_name}",
            f"hi tradebot",
            "trading bot",
            "tradingbot"
        ]
        
        lower_text = text.lower()
        return any(mention.lower() in lower_text for mention in bot_mentions)

    def track_token_mentions(self, token_address: str, text: str):
        """Track mentions of tokens in messages."""
        now = time.time()
        
        if token_address not in self.mentioned_tokens:
            self.mentioned_tokens[token_address] = {
                'mention_count': 0,
                'last_mention_time': now,
                'tokens': {}
            }
        
        # Check for token mentions
        for symbol in self.available_tokens.keys():
            if symbol.lower() in text.lower():
                if symbol not in self.mentioned_tokens[token_address]['tokens']:
                    self.mentioned_tokens[token_address]['tokens'][symbol] = {
                        'mention_count': 0,
                        'last_mention_time': 0
                    }
                
                self.mentioned_tokens[token_address]['tokens'][symbol]['mention_count'] += 1
                self.mentioned_tokens[token_address]['tokens'][symbol]['last_mention_time'] = now
                self.mentioned_tokens[token_address]['mention_count'] += 1
                self.mentioned_tokens[token_address]['last_mention_time'] = now

    def should_respond_to_message(self, token_address: str, is_direct_mention: bool) -> bool:
        """
        Decide whether the bot should respond to a message.
        
        Args:
            token_address: Token address of the room
            is_direct_mention: Whether the message directly mentions the bot
            
        Returns:
            Boolean indicating whether to respond
        """
        now = time.time()
        
        # Always respond to direct mentions, but respect cooldown
        if is_direct_mention:
            last_msg_time = self.last_message_time.get(token_address, 0)
            if now - last_msg_time > self.personality['cooldown_period']:
                return True
            return False
        
        # Check if within activity hours
        current_hour = datetime.now().hour
        is_active_hour = any(start <= current_hour <= end for start, end in self.personality['activity_hours'])
        if not is_active_hour:
            return False
        
        # Check cooldown period
        last_msg_time = self.last_message_time.get(token_address, 0)
        if now - last_msg_time < self.personality['cooldown_period']:
            return False
        
        # Probabilistic response based on personality
        return random.random() < self.personality['response_rate']

    async def generate_response(self, token_address: str, trigger_message: Dict):
        """
        Generate a response to a message using Claude.
        
        Args:
            token_address: Token address of the room
            trigger_message: The message that triggered the response
        """
        try:
            # Build context from recent messages
            context = self.build_conversation_context(token_address)
            
            # Get token symbol
            symbol = self.address_to_symbol.get(token_address, token_address)
            
            # Include portfolio information
            portfolio = self.get_portfolio_state()
            
            # Get price information
            price = self._fetch_price(symbol)
            
            # Build the prompt
            prompt = f"""
            You are {self.personality['name']}, an AI trading bot in a crypto chat room focused on {symbol}. 
            Current price of {symbol} is ${price:.6f}.
            
            Your portfolio:
            Cash balance: ${portfolio['cash_balance']:.2f}
            Holdings: {self.format_holdings(portfolio['holdings'])}
            
            Personality traits:
            - Risk tolerance: {'High' if self.personality['risk_tolerance'] > 0.6 else 'Medium' if self.personality['risk_tolerance'] > 0.3 else 'Low'}
            - Curiosity: {'High' if self.personality['curiosity'] > 0.6 else 'Medium' if self.personality['curiosity'] > 0.3 else 'Low'}
            - Humor: {'High' if self.personality['humor'] > 0.6 else 'Medium' if self.personality['humor'] > 0.3 else 'Low'}
            
            Recent conversation in the chat room:
            {context}
            
            Please respond to this conversation in a helpful, concise way. 
            Keep your response under 2-3 sentences unless you need to explain something complex.
            
            If you identify a good trading opportunity, indicate it with [TRADE SIGNAL] at the end of your message,
            including the action (BUY/SELL), token symbol, and a percentage of your available capital (for BUY) 
            or holdings (for SELL) to use.
            
            Example trade signal: [TRADE SIGNAL] BUY SOL 5%
            """
            
            # Call Claude API
            response = await self.claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract response text
            response_text = response.content[0].text
            
            # Process trade signals
            trade_signal = None
            if '[TRADE SIGNAL]' in response_text:
                # Split response and trade signal
                parts = response_text.split('[TRADE SIGNAL]')
                response_text = parts[0].strip()
                trade_signal = parts[1].strip()
            
            # Send the response message
            self.send_response(token_address, response_text)
            
            # Update last message time
            self.last_message_time[token_address] = time.time()
            
            # Process trade signal if present
            if trade_signal:
                self.process_trade_signal(token_address, trade_signal, trigger_message['walletAddress'])
                
        except Exception as e:
            print(f"Error generating response: {e}")

    def format_holdings(self, holdings: Dict) -> str:
        """Format holdings for display in the prompt."""
        if not holdings:
            return "None"
        
        formatted = []
        for symbol, quantity in holdings.items():
            price = self._fetch_price(symbol)
            value = price * quantity
            formatted.append(f"{quantity} {symbol} (${value:.2f})")
        
        return ", ".join(formatted)

    def build_conversation_context(self, token_address: str) -> str:
        """Build a string representation of recent conversation."""
        if token_address not in self.conversation_context:
            return "No recent messages."
        
        context_messages = self.conversation_context[token_address]
        if not context_messages:
            return "No recent messages."
        
        # Format messages
        formatted_messages = []
        for msg in context_messages:
            name = msg.get('displayName', 'User')
            if not name:
                name = msg.get('walletAddress', 'User')[:8] + '...'
                
            text = msg.get('text', '')
            formatted_messages.append(f"{name}: {text}")
        
        return "\n".join(formatted_messages)

    def send_response(self, token_address: str, text: str):
        """Send a response message to the chat."""
        message = {
            'id': str(uuid.uuid4()),
            'tokenAddress': token_address,
            'walletAddress': 'TRADEBOT',  # The bot's identifier
            'displayName': self.personality['name'],
            'text': text,
            'timestamp': datetime.now().isoformat(),
            'verified': True,
            'botMessage': True
        }
        
        # Emit to the room
        self.socketio.emit('chat_message', {
            'type': 'chat_message',
            'data': message
        }, room=token_address)
        
        # Store in conversation context
        if token_address in self.conversation_context:
            self.conversation_context[token_address].append(message)
            if len(self.conversation_context[token_address]) > self.max_context_messages:
                self.conversation_context[token_address] = self.conversation_context[token_address][-self.max_context_messages:]

    def process_trade_signal(self, token_address: str, signal: str, influencer_address: str = None):
        """
        Process a trade signal from Claude.
        
        Args:
            token_address: Token address of the room
            signal: The trade signal string (e.g., "BUY SOL 5%")
            influencer_address: Wallet address of the user who influenced this trade
        """
        try:
            # Parse the signal
            pattern = r'(BUY|SELL)\s+(\w+)\s+(\d+(?:\.\d+)?)%'
            match = re.search(pattern, signal, re.IGNORECASE)
            
            if not match:
                print(f"Invalid trade signal format: {signal}")
                return
            
            action, symbol, percentage = match.groups()
            percentage = float(percentage)
            
            # Get the token address if the symbol is provided
            if symbol in self.available_tokens:
                token_address = self.available_tokens[symbol]['address']
            
            # Calculate quantity based on percentage
            if action.upper() == 'BUY':
                # Calculate amount based on cash balance
                trade_value = (percentage / 100.0) * self.cash_balance
                price = self._fetch_price(symbol)
                quantity = trade_value / price
            else:  # SELL
                # Calculate amount based on holdings
                if symbol in self.holdings:
                    quantity = (percentage / 100.0) * self.holdings[symbol]
                else:
                    print(f"Cannot sell {symbol}: not in holdings")
                    return
            
            # Execute the trade
            influencers = [influencer_address] if influencer_address else None
            self.execute_trade(
                token_address=token_address,
                action=action.lower(),
                quantity=quantity,
                influencers=influencers,
                reason=f"Signal: {signal}"
            )
            
        except Exception as e:
            print(f"Error processing trade signal: {e}")

    def get_portfolio_state(self) -> Dict:
        """Get the current portfolio state."""
        holdings_value = 0
        for symbol, quantity in self.holdings.items():
            price = self._fetch_price(symbol)
            holdings_value += price * quantity
        
        total_value = self.cash_balance + holdings_value
        
        return {
            'cash_balance': self.cash_balance,
            'holdings': self.holdings,
            'holdings_value': holdings_value,
            'total_value': total_value,
            'initial_balance': self.initial_balance,
            'profit_loss': total_value - self.initial_balance,
            'profit_loss_pct': ((total_value - self.initial_balance) / self.initial_balance) * 100
        }

    async def check_for_trade_opportunities(self):
        """
        Periodically check for trade opportunities based on conversation activity.
        This runs in the background to identify potential trades without direct user prompting.
        """
        for token_address in self.active_rooms:
            now = time.time()
            
            # Skip if we recently made a trade in this room
            last_trade = self.last_trade_time.get(token_address, 0)
            if now - last_trade < 3600:  # Don't trade more than once per hour per room
                continue
            
            # Check if there's been recent token mentions
            token_mentions = self.mentioned_tokens.get(token_address, {})
            last_mention = token_mentions.get('last_mention_time', 0)
            
            # Only consider trading if there have been recent mentions
            if now - last_mention < 1800:  # 30 minutes
                symbol = self.address_to_symbol.get(token_address, token_address)
                
                # Build context from recent messages
                context = self.build_conversation_context(token_address)
                
                # Get sentiment and trading signals
                await self.analyze_trading_opportunity(token_address, symbol, context)

    async def analyze_trading_opportunity(self, token_address: str, symbol: str, context: str):
        """
        Analyze conversation context to identify trading opportunities.
        
        Args:
            token_address: Token address of the room
            symbol: Token symbol
            context: Recent conversation context
        """
        try:
            # Get portfolio information
            portfolio = self.get_portfolio_state()
            
            # Get price information
            price = self._fetch_price(symbol)
            
            # Build the prompt for Claude
            prompt = f"""
            You are {self.personality['name']}, an AI trading bot monitoring a crypto chat room focused on {symbol}.
            Current price of {symbol} is ${price:.6f}.
            
            Your portfolio:
            Cash balance: ${portfolio['cash_balance']:.2f}
            Holdings: {self.format_holdings(portfolio['holdings'])}
            
            Recent conversation in the chat room:
            {context}
            
            Based on this conversation, determine if there's a trading opportunity.
            Consider:
            1. User sentiment about {symbol}
            2. Any technical analysis mentions
            3. News or events discussed
            4. Overall market trends mentioned
            
            If there's a trading opportunity, respond with ONLY a JSON object in this format:
            {{
                "decision": "BUY" or "SELL" or "HOLD",
                "confidence": [0.0-1.0 scale],
                "percentage": [1-20 percentage to trade],
                "reason": "Brief explanation"
            }}
            
            If there's not enough information or no clear signal, use "HOLD" as the decision.
            Be conservative - only trade when there's a clear signal with confidence > 0.7.
            """
            
            # Call Claude API
            response = await self.claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Parse response
            response_text = response.content[0].text
            
            try:
                # Extract JSON from response (may be wrapped in code block)
                if "```json" in response_text:
                    json_text = response_text.split("```json")[1].split("```")[0].strip()
                elif "```" in response_text:
                    json_text = response_text.split("```")[1].strip()
                else:
                    json_text = response_text.strip()
                
                decision = json.loads(json_text)
                
                # Process trading decision
                if (decision['decision'] in ['BUY', 'SELL'] and 
                    decision['confidence'] > 0.7 and
                    decision['percentage'] > 0):
                    
                    # Calculate quantity
                    if decision['decision'] == 'BUY':
                        trade_value = (decision['percentage'] / 100.0) * self.cash_balance
                        quantity = trade_value / price
                    else:  # SELL
                        if symbol in self.holdings:
                            quantity = (decision['percentage'] / 100.0) * self.holdings[symbol]
                        else:
                            return  # Nothing to sell
                    
                    # Execute the trade
                    self.execute_trade(
                        token_address=token_address,
                        action=decision['decision'].lower(),
                        quantity=quantity,
                        reason=decision['reason']
                    )
            except json.JSONDecodeError:
                print(f"Failed to parse JSON from response: {response_text}")
                
        except Exception as e:
            print(f"Error analyzing trading opportunity: {e}")

    def get_user_trades(self, wallet_address: str) -> List[Dict]:
        """
        Get all trades influenced by a specific user.
        
        Args:
            wallet_address: User's wallet address
            
        Returns:
            List of trade records influenced by the user
        """
        try:
            # Scan the trades table for trades influenced by this user
            response = self.trade_table.scan(
                FilterExpression='contains(influencers, :wallet)',
                ExpressionAttributeValues={
                    ':wallet': wallet_address
                }
            )
            
            trades = response.get('Items', [])
            
            # Convert Decimal to float for easier handling
            for trade in trades:
                for key in trade:
                    if isinstance(trade[key], Decimal):
                        trade[key] = float(trade[key])
            
            return trades
        except Exception as e:
            print(f"Error getting user trades: {e}")
            return []

    def get_user_performance(self, wallet_address: str) -> Dict:
        """
        Calculate performance metrics for trades influenced by a specific user.
        
        Args:
            wallet_address: User's wallet address
            
        Returns:
            Dictionary with user performance metrics
        """
        trades = self.get_user_trades(wallet_address)
        
        if not trades:
            return {
                'wallet_address': wallet_address,
                'trade_count': 0,
                'influence_score': 0,
                'profitable_trades': 0,
                'total_influence': 0
            }
        
        # Calculate performance metrics
        profitable_trades = 0
        total_value = 0
        
        for trade in trades:
            symbol = trade['symbol']
            current_price = self._fetch_price(symbol)
            
            # Calculate profit/loss for this trade
            if trade['action'] == 'buy':
                pl_percent = (current_price - trade['price']) / trade['price'] * 100
                if pl_percent > 0:
                    profitable_trades += 1
            else:  # sell
                pl_percent = (trade['price'] - current_price) / current_price * 100
                if pl_percent > 0:
                    profitable_trades += 1
            
            # Add to total influence value
            total_value += trade['trade_value']
        
        # Calculate influence score (0-100)
        influence_score = (profitable_trades / len(trades)) * 100 if trades else 0
        
        return {
            'wallet_address': wallet_address,
            'trade_count': len(trades),
            'influence_score': influence_score,
            'profitable_trades': profitable_trades,
            'unprofitable_trades': len(trades) - profitable_trades,
            'total_influence': total_value
        }

    def get_user_rankings(self) -> List[Dict]:
        """
        Rank users by their influence score.
        
        Returns:
            List of users sorted by influence score
        """
        try:
            # Get all trades
            scan_response = self.trade_table.scan()
            all_trades = scan_response.get('Items', [])
            
            # Extract unique influencers
            influencers = set()
            for trade in all_trades:
                if 'influencers' in trade:
                    for influencer in trade['influencers']:
                        influencers.add(influencer)
            
            # Calculate performance for each influencer
            rankings = []
            for wallet_address in influencers:
                performance = self.get_user_performance(wallet_address)
                if performance['trade_count'] > 0:
                    rankings.append(performance)
            
            # Sort by influence score
            rankings.sort(key=lambda x: x['influence_score'], reverse=True)
            
            return rankings
        except Exception as e:
            print(f"Error getting user rankings: {e}")
            return []

    def get_leaderboard_message(self) -> str:
        """Generate a leaderboard message showing top influencers."""
        rankings = self.get_user_rankings()
        
        if not rankings:
            return "No trader rankings available yet. Start influencing trades to get on the leaderboard!"
        
        # Take top 5 or fewer
        top_users = rankings[:5]
        
        message = "🏆 **Trader Influence Leaderboard** 🏆\n\n"
        
        for i, user in enumerate(top_users):
            wallet = user['wallet_address']
            # Truncate wallet address for display
            short_wallet = wallet[:6] + '...' + wallet[-4:]
            
            message += f"{i+1}. {short_wallet}: {user['influence_score']:.1f}% success rate ({user['profitable_trades']}/{user['trade_count']} trades)\n"
        
        return message

    def post_portfolio_update(self, token_address: str):
        """Post a portfolio update message to the chat."""
        portfolio = self.get_portfolio_state()
        
        # Format holdings
        holdings_str = ""
        for symbol, quantity in self.holdings.items():
            price = self._fetch_price(symbol)
            value = price * quantity
            holdings_str += f"- {quantity} {symbol} @ ${price:.6f} = ${value:.2f}\n"
        
        if not holdings_str:
            holdings_str = "- No current holdings\n"
        
        message = f"""📊 **Portfolio Update** 📊

Current Balance: ${portfolio['total_value']:.2f}
Cash: ${portfolio['cash_balance']:.2f}
Holdings Value: ${portfolio['holdings_value']:.2f}

Holdings:
{holdings_str}
P&L: ${portfolio['profit_loss']:.2f} ({portfolio['profit_loss_pct']:.2f}%)"""

        self.send_response(token_address, message)

    def post_leaderboard(self, token_address: str):
        """Post the leaderboard to the chat."""
        leaderboard = self.get_leaderboard_message()
        self.send_response(token_address, leaderboard)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          