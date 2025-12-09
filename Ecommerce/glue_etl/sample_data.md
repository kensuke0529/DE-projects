# Sample Raw Event Data (JSON)
{
  "event_id": "bf68c524-79a3-4367-9cc2-5730e6f2ed86",
  "event_type": "order_paid",
  "session_id": "ba8032d1-3503-41e8-b7d3-cf006a6fe518",
  "event_timestamp": "2025-12-09T00:09:02.863815",
  "customer": {
    "customer_id": 1326,
    "first_name": "Shannon",
    "last_name": "Hoffman",
    "email": "changlydia@example.net",
    "country": "Afghanistan",
    "created_at": "2025-12-01T00:00:00.000Z",
    "tax_rate": 0.08
  },
  "order": {
    "order_id": "1ed960ec-a120-4e48-b555-c245ad04ae80",
    "customer_id": 1326,
    "items": [
      {
        "product": {
          "id": 103,
          "name": "USB-C Cable 6ft",
          "category": "Electronics",
          "price": 14.99
        },
        "quantity": 2
      }
    ],
    "subtotal": 29.98,
    "tax": 2.4,
    "tax_rate": 0.08,
    "shipping": 13.04,
    "total_amount": 45.42,
    "currency": "USD",
    "status": "paid",
    "created_at": "2025-12-09T00:08:38.863815",
    "paid_at": "2025-12-09T00:09:02.863815"
  },
  "session": {
    "channel": "Google",
    "device": "mobile"
  }
}

---

# Corresponding Processed Event Data (Table format shown as header/columns + sample rows):

| event_id                             | event_type  | session_id                            | event_timestamp             | order_date  | processed_at                | customer_id | first_name | last_name | email                    | country      | tax_rate | session_channel | device   | order_id                               | subtotal | tax  | order_tax_rate | shipping | total_amount | status    | order_created                | order_paid                    | product_id | product_name      | product_category | product_price | order_item_qty |
|--------------------------------------|-------------|---------------------------------------|-----------------------------|-------------|-----------------------------|-------------|------------|-----------|--------------------------|-------------|----------|-----------------|----------|----------------------------------------|----------|------|---------------|----------|--------------|-----------|------------------------------|-------------------------------|------------|-------------------|------------------|--------------|----------------|
| 02490f85-0646-401a-b0f3-a7d6ed039440 | order_paid  | 17778613-eddb-4479-aef4-d0b9620bf23b  | 2025-12-08 21:16:11.256338  | 2025-12-08  | 2025-12-08 22:12:45.690413  | 1760        | Jessica    | Mcneil    | ryan73@example.org       | Reunion     | 0.08     | Google          | desktop  | c211c9d3-679e-4955-9ae6-71b7fc5bfab1   | 34.99    | 2.8  | 0.08          | 11.61    | 49.4         | paid      | 2025-12-08T21:16:00.256338   | 2025-12-08T21:16:11.256338    | 304        | LED Desk Lamp     | Home             | 34.99        | 1              |
| 05ef9b92-9c35-4f6f-a8fa-cef994b6b8d4 | order_paid  | 148bf1f7-c0eb-42e7-80ca-0deb11970b0f  | 2025-12-08 21:02:17.920806  | 2025-12-08  | 2025-12-08 22:12:45.690413  | 113         | Kyle       | Black     | vclark@example.net       | Argentina   | 0.08     | Facebook        | desktop  | ea51862a-4085-4d3c-9cd1-c621bdc6d639   | 79.99    | 6.4  | 0.08          | 7.08     | 93.47        | fulfilled | 2025-12-08T21:01:48.920806   | 2025-12-08T21:02:17.920806    | 105        | Bluetooth Speaker | Electronics       | 79.99        | 1              |
