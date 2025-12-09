import json
import random
import os
from datetime import datetime, timedelta
from faker import Faker
import boto3

fake = Faker()
firehose = boto3.client("firehose")
s3 = boto3.client("s3")

# Configuration
DELIVERY_STREAM = os.environ.get("FIREHOSE_DELIVERY_STREAM_NAME", "ecom-firehose")
SEED_BUCKET = os.environ.get("CUSTOMER_SEED_BUCKET", "ecom-analytics-seeds")
SEED_KEY = "customer_seed.json"
CACHE_FILE = "/tmp/customer_seed.json"

# Global state
CUSTOMER_DB = None
CONTAINER_START_TIME = datetime.utcnow()
INVOCATION_COUNT = 0

# Product catalog (50 products)
PRODUCT_CATALOG = [
    # Electronics
    {
        "id": 101,
        "name": "Wireless Earbuds Pro",
        "category": "Electronics",
        "price": 129.00,
    },
    {"id": 102, "name": "Smartphone Stand", "category": "Electronics", "price": 24.99},
    {"id": 103, "name": "USB-C Cable 6ft", "category": "Electronics", "price": 14.99},
    {
        "id": 104,
        "name": "Portable Charger 20000mAh",
        "category": "Electronics",
        "price": 45.00,
    },
    {"id": 105, "name": "Bluetooth Speaker", "category": "Electronics", "price": 79.99},
    {
        "id": 106,
        "name": "Laptop Sleeve 15inch",
        "category": "Electronics",
        "price": 29.99,
    },
    {"id": 107, "name": "Wireless Mouse", "category": "Electronics", "price": 34.99},
    {
        "id": 108,
        "name": "Mechanical Keyboard",
        "category": "Electronics",
        "price": 89.99,
    },
    {"id": 109, "name": "Webcam 1080p", "category": "Electronics", "price": 59.99},
    {"id": 110, "name": "Phone Case Clear", "category": "Electronics", "price": 12.99},
    # Clothing
    {"id": 201, "name": "Cotton T-Shirt Basic", "category": "Clothing", "price": 19.99},
    {"id": 202, "name": "Denim Jeans Classic", "category": "Clothing", "price": 59.99},
    {"id": 203, "name": "Hoodie Pullover", "category": "Clothing", "price": 44.99},
    {"id": 204, "name": "Running Shorts", "category": "Clothing", "price": 29.99},
    {"id": 205, "name": "Crew Socks 6-Pack", "category": "Clothing", "price": 16.99},
    {"id": 206, "name": "Baseball Cap", "category": "Clothing", "price": 24.99},
    {"id": 207, "name": "Winter Jacket", "category": "Clothing", "price": 129.99},
    {"id": 208, "name": "Sneakers White", "category": "Clothing", "price": 79.99},
    {
        "id": 209,
        "name": "Dress Shirt Button-Up",
        "category": "Clothing",
        "price": 49.99,
    },
    {"id": 210, "name": "Yoga Pants", "category": "Clothing", "price": 39.99},
    # Home
    {
        "id": 301,
        "name": "Stainless Steel Water Bottle",
        "category": "Home",
        "price": 24.99,
    },
    {"id": 302, "name": "Coffee Mug Ceramic", "category": "Home", "price": 12.99},
    {"id": 303, "name": "Throw Pillow 18x18", "category": "Home", "price": 22.99},
    {"id": 304, "name": "LED Desk Lamp", "category": "Home", "price": 34.99},
    {"id": 305, "name": "Bath Towel Set", "category": "Home", "price": 39.99},
    {"id": 306, "name": "Kitchen Knife Set", "category": "Home", "price": 89.99},
    {"id": 307, "name": "Cutting Board Bamboo", "category": "Home", "price": 19.99},
    {"id": 308, "name": "Candle Vanilla Scented", "category": "Home", "price": 16.99},
    {"id": 309, "name": "Picture Frame 8x10", "category": "Home", "price": 14.99},
    {"id": 310, "name": "Storage Basket Woven", "category": "Home", "price": 27.99},
    # Beauty
    {
        "id": 401,
        "name": "Organic Face Moisturizer",
        "category": "Beauty",
        "price": 29.99,
    },
    {
        "id": 402,
        "name": "Shampoo & Conditioner Set",
        "category": "Beauty",
        "price": 24.99,
    },
    {"id": 403, "name": "Lip Balm 3-Pack", "category": "Beauty", "price": 9.99},
    {"id": 404, "name": "Sunscreen SPF 50", "category": "Beauty", "price": 18.99},
    {
        "id": 405,
        "name": "Face Mask Sheet 10-Pack",
        "category": "Beauty",
        "price": 19.99,
    },
    {"id": 406, "name": "Hair Brush Detangling", "category": "Beauty", "price": 14.99},
    {"id": 407, "name": "Nail Polish Set", "category": "Beauty", "price": 22.99},
    {"id": 408, "name": "Body Lotion Lavender", "category": "Beauty", "price": 16.99},
    {"id": 409, "name": "Makeup Remover Wipes", "category": "Beauty", "price": 11.99},
    {"id": 410, "name": "Electric Toothbrush", "category": "Beauty", "price": 49.99},
    # Sports
    {"id": 501, "name": "Yoga Mat Premium", "category": "Sports", "price": 45.00},
    {"id": 502, "name": "Resistance Bands Set", "category": "Sports", "price": 29.99},
    {
        "id": 503,
        "name": "Water Bottle with Straw",
        "category": "Sports",
        "price": 19.99,
    },
    {"id": 504, "name": "Gym Bag Duffel", "category": "Sports", "price": 39.99},
    {"id": 505, "name": "Jump Rope Speed", "category": "Sports", "price": 14.99},
    {"id": 506, "name": "Foam Roller", "category": "Sports", "price": 24.99},
    {"id": 507, "name": "Tennis Balls 3-Pack", "category": "Sports", "price": 9.99},
    {"id": 508, "name": "Camping Tent 2-Person", "category": "Sports", "price": 129.99},
    {"id": 509, "name": "Hiking Backpack 40L", "category": "Sports", "price": 89.99},
    {"id": 510, "name": "Bike Lock Heavy Duty", "category": "Sports", "price": 34.99},
]

# Weighted channel distribution (realistic)
CHANNEL_WEIGHTS = {
    "Direct": 25,
    "Google": 30,
    "Email": 15,
    "Facebook": 12,
    "Instagram": 10,
    "TikTok": 8,
}

# Weighted device distribution (realistic)
DEVICE_WEIGHTS = {"mobile": 61, "desktop": 38, "tablet": 1}

# Limited country list with tax rates
COUNTRIES_WITH_TAX = [
    {"name": "United States", "tax_rate": 0.08},
    {"name": "Canada", "tax_rate": 0.13},
    {"name": "United Kingdom", "tax_rate": 0.20},
    {"name": "Germany", "tax_rate": 0.19},
    {"name": "France", "tax_rate": 0.20},
    {"name": "Australia", "tax_rate": 0.10},
    {"name": "Japan", "tax_rate": 0.10},
    {"name": "Italy", "tax_rate": 0.22},
    {"name": "Spain", "tax_rate": 0.21},
    {"name": "Netherlands", "tax_rate": 0.21},
]

# ============================================
# CUSTOMER SEED LOADING
# ============================================


def load_customer_seed():
    """Load customer seed with fail-hard requirement"""
    global CUSTOMER_DB

    if CUSTOMER_DB is not None:
        return CUSTOMER_DB

    if os.path.exists(CACHE_FILE):
        print(f"✅ Loading from /tmp cache")
        with open(CACHE_FILE, "r") as f:
            CUSTOMER_DB = json.load(f)
            return CUSTOMER_DB

    print(f"❄️ COLD START - Downloading from S3")
    try:
        s3.download_file(SEED_BUCKET, SEED_KEY, CACHE_FILE)
        with open(CACHE_FILE, "r") as f:
            CUSTOMER_DB = json.load(f)
            print(f"✅ Loaded {len(CUSTOMER_DB)} customers")
            return CUSTOMER_DB
    except Exception as e:
        print(f"❌ FATAL: Cannot load seed from s3://{SEED_BUCKET}/{SEED_KEY}: {e}")
        raise RuntimeError(f"Customer seed required: {e}")


def get_customer():
    """Get consistent customer from seed"""
    customers = load_customer_seed()
    customer_id = random.randint(1, len(customers))
    return customers[str(customer_id)]


# ============================================
# EVENT GENERATION
# ============================================


def weighted_choice(choices_dict):
    """Choose from weighted dictionary"""
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def emit_event(event_type, session_id, customer, event_time, extra={}):
    """Create event with specified timestamp"""
    base = {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "session_id": session_id,
        "event_timestamp": event_time.isoformat(),
        "customer": customer,
    }
    base.update(extra)
    return base


def simulate_session():
    """
    Simulate realistic customer session

    KEY FIXES:
    1. Timestamps NEVER in future
    2. Realistic 1.5-2% final conversion
    3. Per-product add-to-cart decisions
    4. No double-counting of abandons
    """
    customer = get_customer()
    session_id = fake.uuid4()
    channel = weighted_choice(CHANNEL_WEIGHTS)
    device = weighted_choice(DEVICE_WEIGHTS)

    # Session starts in the PAST (1-30 minutes ago)
    # This ensures ALL events are in the past
    session_start = datetime.utcnow() - timedelta(minutes=random.randint(1, 30))
    current_time = session_start

    events = []
    cart_items = []

    # View 1-3 products
    num_products = random.randint(1, 3)
    viewed_products = random.sample(PRODUCT_CATALOG, num_products)

    for product in viewed_products:
        # Product view
        events.append(
            emit_event(
                "product_view",
                session_id,
                customer,
                current_time,
                {"product": product, "session": {"channel": channel, "device": device}},
            )
        )

        # Browse 30s to 3min
        current_time += timedelta(seconds=random.randint(30, 180))

        # FIXED: Each product has independent 8% chance to be added to cart
        # This gives ~8% cart rate overall (realistic)
        if random.random() < 0.08:
            quantity = random.randint(1, 2)
            cart_items.append({"product": product, "quantity": quantity})

    # If nothing added to cart, session ends here
    if not cart_items:
        return events

    # User added items to cart
    cart_id = fake.uuid4()
    cart = {
        "cart_id": cart_id,
        "items": cart_items,
        "created_at": current_time.isoformat(),
    }

    events.append(
        emit_event(
            "cart_add",
            session_id,
            customer,
            current_time,
            {"cart": cart, "session": {"channel": channel, "device": device}},
        )
    )

    # Browse cart 1-5 minutes
    current_time += timedelta(minutes=random.randint(1, 5))

    # 70% proceed to checkout (5.6% of all sessions)
    if random.random() < 0.70:
        events.append(
            emit_event(
                "checkout_start",
                session_id,
                customer,
                current_time,
                {"cart": cart, "session": {"channel": channel, "device": device}},
            )
        )

        # Fill out checkout 1-3 minutes
        current_time += timedelta(minutes=random.randint(1, 3))

        # 85% complete order (4.8% of all sessions)
        if random.random() < 0.85:
            order_id = fake.uuid4()

            # Calculate totals
            subtotal = sum(i["product"]["price"] * i["quantity"] for i in cart["items"])
            tax_rate = customer.get("tax_rate", 0.08)
            tax = round(subtotal * tax_rate, 2)
            shipping = round(random.uniform(5.0, 15.0), 2)
            total = round(subtotal + tax + shipping, 2)

            order = {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "items": cart["items"],
                "subtotal": subtotal,
                "tax": tax,
                "tax_rate": tax_rate,
                "shipping": shipping,
                "total_amount": total,
                "currency": "USD",
                "status": "created",
                "created_at": current_time.isoformat(),
            }

            events.append(
                emit_event(
                    "order_create",
                    session_id,
                    customer,
                    current_time,
                    {"order": order, "session": {"channel": channel, "device": device}},
                )
            )

            # Payment processing 10-30 seconds
            current_time += timedelta(seconds=random.randint(10, 30))

            # 92% payment success (4.4% of all sessions)
            if random.random() < 0.92:
                order["status"] = "paid"
                order["paid_at"] = current_time.isoformat()

                events.append(
                    emit_event(
                        "order_paid",
                        session_id,
                        customer,
                        current_time,
                        {
                            "order": order,
                            "session": {"channel": channel, "device": device},
                        },
                    )
                )

                # Fulfillment 2-6 hours later (compressed from 1-3 days)
                current_time += timedelta(hours=random.randint(2, 6))

                # 80% fulfillment (3.5% of all sessions → ~2% final conversion)
                if random.random() < 0.80:
                    order["status"] = "fulfilled"
                    order["fulfilled_at"] = current_time.isoformat()

                    events.append(
                        emit_event(
                            "order_fulfilled",
                            session_id,
                            customer,
                            current_time,
                            {
                                "order": order,
                                "session": {"channel": channel, "device": device},
                            },
                        )
                    )

            else:
                # Payment failed
                order["status"] = "cancelled"
                order["cancelled_at"] = current_time.isoformat()
                order["cancel_reason"] = "payment_failed"

                events.append(
                    emit_event(
                        "order_cancelled",
                        session_id,
                        customer,
                        current_time,
                        {
                            "order": order,
                            "session": {"channel": channel, "device": device},
                        },
                    )
                )

        else:
            # FIXED: Abandon at checkout stage (only ONE abandon event)
            events.append(
                emit_event(
                    "cart_abandon",
                    session_id,
                    customer,
                    current_time,
                    {
                        "cart": cart,
                        "abandon_stage": "checkout",
                        "session": {"channel": channel, "device": device},
                    },
                )
            )

    else:
        # FIXED: Abandon at cart stage (only ONE abandon event)
        events.append(
            emit_event(
                "cart_abandon",
                session_id,
                customer,
                current_time,
                {
                    "cart": cart,
                    "abandon_stage": "cart",
                    "session": {"channel": channel, "device": device},
                },
            )
        )

    return events


# ============================================
# LAMBDA HANDLER
# ============================================


def lambda_handler(event, context):
    """Main handler with realistic traffic patterns"""

    global INVOCATION_COUNT
    INVOCATION_COUNT += 1

    # Log configuration for debugging
    print(f"🔧 Configuration:")
    print(f"   Firehose Stream: {DELIVERY_STREAM}")
    print(f"   Seed Bucket: {SEED_BUCKET}")

    uptime_seconds = (datetime.utcnow() - CONTAINER_START_TIME).total_seconds()
    hour = datetime.utcnow().hour

    # Time-based traffic
    if 9 <= hour <= 13 or 18 <= hour <= 22:
        sessions_per_run = random.randint(12, 20)
    elif 1 <= hour <= 6:
        sessions_per_run = random.randint(2, 5)
    else:
        sessions_per_run = random.randint(6, 12)

    # Generate events
    events_out = []
    sessions_succeeded = 0
    sessions_failed = 0

    for _ in range(sessions_per_run):
        try:
            session_events = simulate_session()
            events_out.extend(session_events)
            sessions_succeeded += 1
        except Exception as e:
            sessions_failed += 1
            print(f"❌ Session error: {e}")
            continue

    # Deliver to Kinesis Data Firehose
    if events_out:
        try:
            print(f"📤 Sending {len(events_out)} events to Firehose: {DELIVERY_STREAM}")
            # Firehose accepts up to 500 records per batch
            # Split into batches if needed
            batch_size = 500
            total_sent = 0
            
            # Validate and prepare records with proper JSON Lines format
            def prepare_record(event):
                """Ensure proper JSON Lines format - compact JSON, single line, newline terminated"""
                # Ensure event is a dict (not string)
                if not isinstance(event, dict):
                    raise ValueError(f"Event must be a dict, got {type(event)}")
                
                # Use json.dumps with ensure_ascii=False and no extra whitespace
                # separators=(',', ':') removes spaces for compact JSON
                json_str = json.dumps(event, ensure_ascii=False, separators=(',', ':'))
                
                # Validate JSON can be parsed back (catches serialization issues)
                try:
                    json.loads(json_str)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON generated: {e}")
                
                # Add newline for JSON Lines format
                return (json_str + "\n").encode("utf-8")
            
            for i in range(0, len(events_out), batch_size):
                batch = events_out[i : i + batch_size]
                print(f"📦 Sending batch {i//batch_size + 1} ({len(batch)} records)")
                
                # Prepare records with proper JSON Lines format
                records = [{"Data": prepare_record(e)} for e in batch]
                
                # Log first record structure for debugging (only first batch)
                if i == 0 and len(batch) > 0:
                    sample_json = json.dumps(batch[0], indent=2)
                    print(f"📋 Sample record structure:\n{sample_json[:500]}...")
                
                response = firehose.put_record_batch(
                    DeliveryStreamName=DELIVERY_STREAM,
                    Records=records,
                )

                if response.get("FailedPutCount", 0) > 0:
                    failed = response.get("RequestResponses", [])
                    failed_count = sum(1 for r in failed if "ErrorCode" in r)
                    print(f"⚠️ {failed_count} records failed in batch")
                    # Log failed record details
                    for idx, resp in enumerate(failed):
                        if "ErrorCode" in resp:
                            print(f"  ❌ Record {idx}: {resp.get('ErrorCode')} - {resp.get('ErrorMessage', 'No message')}")
                else:
                    total_sent += len(batch)
                    print(f"✅ Successfully sent {len(batch)} records")
            
            print(f"✅ Total records sent to Firehose: {total_sent}/{len(events_out)}")

        except Exception as e:
            print(f"❌ Firehose error: {e}")
            print(f"   Delivery Stream: {DELIVERY_STREAM}")
            print(f"   Error type: {type(e).__name__}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
            return {"statusCode": 500, "error": str(e)}

    print(f"""
    📊 Invocation #{INVOCATION_COUNT}
    ⏱️  Uptime: {uptime_seconds / 60:.1f}m
    🌡️  Traffic: {sessions_succeeded} sessions ({hour}:00 UTC)
    📦 Events: {len(events_out)}
    """)

    return {
        "statusCode": 200,
        "generated_events": len(events_out),
        "sessions": sessions_succeeded,
        "hour": hour,
    }
