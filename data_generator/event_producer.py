import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import logging
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker(['en_GB'])
Faker.seed(42)
random.seed(42)

class EcommerceEventGenerator:
    """
    Generates realistic e-commerce events with proper user journeys
    """
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            acks='all'
        )
        
        # Initialize catalogs
        self.products = self._generate_product_catalog()
        self.users = self._generate_user_base(1000)
        self.active_sessions = {}
        
        logger.info(f"✅ Initialized with {len(self.products)} products and {len(self.users)} users")
    
    def _generate_product_catalog(self) -> List[Dict]:
        """Generate realistic product catalog"""
        categories = {
            'Electronics': [
                ('iPhone 15 Pro', 999.99, 150),
                ('Samsung Galaxy S24', 849.99, 200),
                ('MacBook Pro', 1999.99, 50),
                ('iPad Air', 599.99, 120),
                ('AirPods Pro', 249.99, 300),
                ('Dell XPS 15', 1499.99, 75),
                ('Sony WH-1000XM5', 379.99, 180),
                ('Apple Watch Series 9', 399.99, 150),
            ],
            'Clothing': [
                ('Nike Air Max 270', 129.99, 400),
                ('Levi\'s 501 Jeans', 89.99, 500),
                ('Adidas Ultraboost', 159.99, 350),
                ('North Face Jacket', 249.99, 200),
                ('Ralph Lauren Polo', 79.99, 600),
                ('Converse Chuck Taylor', 69.99, 700),
            ],
            'Home & Kitchen': [
                ('Dyson V15 Vacuum', 649.99, 80),
                ('Nespresso Machine', 199.99, 150),
                ('KitchenAid Mixer', 449.99, 100),
                ('Philips Air Fryer', 129.99, 250),
                ('Instant Pot', 99.99, 300),
            ],
            'Books': [
                ('The Lean Startup', 19.99, 1000),
                ('Atomic Habits', 14.99, 1500),
                ('Sapiens', 16.99, 1200),
                ('Educated', 13.99, 800),
            ]
        }
        
        products = []
        product_id = 1
        
        for category, items in categories.items():
            for name, price, stock in items:
                products.append({
                    'product_id': f"PROD_{product_id:05d}",
                    'name': name,
                    'category': category,
                    'price': price,
                    'stock': stock,
                    'rating': round(random.uniform(3.5, 5.0), 1)
                })
                product_id += 1
        
        return products
    
    def _generate_user_base(self, count: int) -> List[Dict]:
        """Generate user base"""
        users = []
        for i in range(count):
            signup_date = fake.date_between(start_date='-2y', end_date='today')
            users.append({
                'user_id': f"USER_{i+1:06d}",
                'name': fake.name(),
                'email': fake.email(),
                'country': random.choice(['GB'] * 70 + ['US'] * 15 + ['DE', 'FR', 'IT'] * 5),
                'city': fake.city(),
                'signup_date': signup_date.isoformat(),
                'customer_segment': random.choice(['VIP', 'Regular', 'New']),
            })
        return users
    
    def _get_or_create_session(self, user_id: str) -> str:
        """Manage user sessions (30-minute timeout)"""
        now = datetime.utcnow()
        
        if user_id in self.active_sessions:
            session_data = self.active_sessions[user_id]
            last_activity = datetime.fromisoformat(session_data['last_activity'])
            
            if (now - last_activity).total_seconds() < 1800:
                session_data['last_activity'] = now.isoformat()
                return session_data['session_id']
        
        # Create new session
        session_id = f"SESS_{fake.uuid4()[:8].upper()}"
        self.active_sessions[user_id] = {
            'session_id': session_id,
            'last_activity': now.isoformat(),
            'cart': []
        }
        return session_id
    
    def generate_page_view(self) -> Dict:
        """User lands on page"""
        user = random.choice(self.users)
        session_id = self._get_or_create_session(user['user_id'])
        
        return {
            'event_type': 'page_view',
            'event_id': fake.uuid4(),
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': user['user_id'],
            'session_id': session_id,
            'page_type': random.choice(['home', 'category', 'search', 'product']),
            'device': random.choice(['mobile'] * 60 + ['desktop'] * 30 + ['tablet'] * 10),
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
            'referrer': random.choice(['google', 'facebook', 'direct', 'email', 'instagram'])
        }
    
    def generate_product_view(self) -> Dict:
        """User views product"""
        user = random.choice(self.users)
        product = random.choice(self.products)
        session_id = self._get_or_create_session(user['user_id'])
        
        return {
            'event_type': 'product_view',
            'event_id': fake.uuid4(),
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': user['user_id'],
            'session_id': session_id,
            'product_id': product['product_id'],
            'product_name': product['name'],
            'category': product['category'],
            'price': product['price'],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'time_on_page_seconds': random.randint(5, 300)
        }
    
    def generate_add_to_cart(self) -> Dict:
        """User adds to cart"""
        user = random.choice(self.users)
        product = random.choice(self.products)
        session_id = self._get_or_create_session(user['user_id'])
        quantity = random.randint(1, 3)
        
        # Track cart
        if user['user_id'] in self.active_sessions:
            self.active_sessions[user['user_id']]['cart'].append({
                'product_id': product['product_id'],
                'quantity': quantity,
                'price': product['price']
            })
        
        return {
            'event_type': 'add_to_cart',
            'event_id': fake.uuid4(),
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': user['user_id'],
            'session_id': session_id,
            'product_id': product['product_id'],
            'product_name': product['name'],
            'category': product['category'],
            'price': product['price'],
            'quantity': quantity,
            'cart_value': product['price'] * quantity
        }
    
    def generate_purchase(self) -> Dict:
        """User completes purchase"""
        user = random.choice(self.users)
        session_id = self._get_or_create_session(user['user_id'])
        
        # Get cart items or random
        if user['user_id'] in self.active_sessions and self.active_sessions[user['user_id']]['cart']:
            cart_items = self.active_sessions[user['user_id']]['cart']
        else:
            num_items = random.randint(1, 3)
            cart_items = [
                {
                    'product_id': p['product_id'],
                    'quantity': random.randint(1, 2),
                    'price': p['price']
                }
                for p in random.sample(self.products, num_items)
            ]
        
        subtotal = sum(item['price'] * item['quantity'] for item in cart_items)
        discount = subtotal * random.choice([0, 0, 0, 0.1, 0.15, 0.2])
        shipping = 0 if subtotal > 50 else 4.99
        total = subtotal - discount + shipping
        
        # Clear cart
        if user['user_id'] in self.active_sessions:
            self.active_sessions[user['user_id']]['cart'] = []
        
        return {
            'event_type': 'purchase',
            'event_id': fake.uuid4(),
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': user['user_id'],
            'session_id': session_id,
            'order_id': f"ORD_{fake.uuid4()[:8].upper()}",
            'items': cart_items,
            'num_items': len(cart_items),
            'subtotal': round(subtotal, 2),
            'discount': round(discount, 2),
            'shipping': shipping,
            'total_amount': round(total, 2),
            'payment_method': random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay']),
            'shipping_country': user['country'],
            'is_first_purchase': random.random() < 0.15
        }
    
    def generate_inventory_update(self) -> Dict:
        """Stock level change"""
        product = random.choice(self.products)
        change = random.randint(-50, 100)
        
        return {
            'event_type': 'inventory_update',
            'event_id': fake.uuid4(),
            'timestamp': datetime.utcnow().isoformat(),
            'product_id': product['product_id'],
            'product_name': product['name'],
            'old_stock': product['stock'],
            'new_stock': max(0, product['stock'] + change),
            'change': change,
            'warehouse': random.choice(['UK_LONDON', 'UK_MANCHESTER', 'UK_BIRMINGHAM', 'EU_DUBLIN'])
        }
    
    def run_realistic_stream(self, duration_minutes=60, events_per_second=5):
        """
        Generate realistic event stream
        
        Distribution:
        - 50% page views
        - 25% product views
        - 15% add to cart
        - 7% purchases
        - 3% inventory updates
        """
        logger.info(f"🚀 Starting event stream for {duration_minutes} minutes at {events_per_second} events/sec")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        event_count = {'total': 0}
        event_types_count = {}
        
        try:
            while datetime.now() < end_time:
                rand = random.random()
                
                if rand < 0.50:
                    event = self.generate_page_view()
                    topic = 'ecommerce-events'
                elif rand < 0.75:
                    event = self.generate_product_view()
                    topic = 'ecommerce-events'
                elif rand < 0.90:
                    event = self.generate_add_to_cart()
                    topic = 'ecommerce-events'
                elif rand < 0.97:
                    event = self.generate_purchase()
                    topic = 'ecommerce-events'
                else:
                    event = self.generate_inventory_update()
                    topic = 'inventory-updates'
                
                # Send to Kafka
                key = event.get('user_id') or event.get('product_id')
                self.producer.send(topic, key=key, value=event)
                
                # Track stats
                event_count['total'] += 1
                event_type = event['event_type']
                event_types_count[event_type] = event_types_count.get(event_type, 0) + 1
                
                # Log progress
                if event_count['total'] % 100 == 0:
                    logger.info(f"📊 Sent {event_count['total']:,} events | {event_types_count}")
                
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            logger.info("⏹️  Stream stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"""
╔════════════════════════════════════════════════╗
║          EVENT GENERATION COMPLETE              ║
╠════════════════════════════════════════════════╣
║  Total Events: {event_count['total']:,}                       
║  Distribution: {event_types_count}
╚════════════════════════════════════════════════╝
            """)

if __name__ == "__main__":
    generator = EcommerceEventGenerator()
    
    # Generate for 30 minutes at 5 events/second = 9,000 events
    generator.run_realistic_stream(duration_minutes=30, events_per_second=5)
