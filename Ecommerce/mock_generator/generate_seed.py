import json

from faker import Faker

import boto3


fake = Faker()

s3 = boto3.client("s3")


SEED_BUCKET = "ecom-analytics-seeds"  # Using the default from lambda_function.py
SEED_KEY = "customer_seed.json"


customers = {}

for customer_id in range(1, 2001):
    customers[str(customer_id)] = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "country": fake.country(),
        "created_at": "2025-12-01T00:00:00.000Z",
    }


with open("customer_seed.json", "w") as f:
    json.dump(customers, f)


print(f"✅ Generated {len(customers)} customers")


try:
    s3.create_bucket(Bucket=SEED_BUCKET)

    print(f"✅ Created bucket: {SEED_BUCKET}")

except:
    print(f"ℹ️  Bucket already exists")


s3.upload_file("customer_seed.json", SEED_BUCKET, SEED_KEY)

print(f"✅ Uploaded to s3://{SEED_BUCKET}/{SEED_KEY}")
