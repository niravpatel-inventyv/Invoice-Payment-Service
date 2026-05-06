-- 002_seed_businesses_customers.up.sql
-- Inserts 3 dummy businesses and 3 customers for each business

WITH inserted_businesses AS (
    INSERT INTO businesses (name)
    VALUES
        ('Acme Retail Pvt Ltd'),
        ('Northstar Logistics Inc'),
        ('Bluefin Labs LLC')
    RETURNING id, name
)
INSERT INTO customers (business_id, name, email)
SELECT b.id, c.customer_name, c.customer_email
FROM inserted_businesses b
JOIN LATERAL (
    VALUES
        (b.name || ' Customer 1', 'customer1+' || REPLACE(LOWER(b.name), ' ', '') || '@example.com'),
        (b.name || ' Customer 2', 'customer2+' || REPLACE(LOWER(b.name), ' ', '') || '@example.com'),
        (b.name || ' Customer 3', 'customer3+' || REPLACE(LOWER(b.name), ' ', '') || '@example.com')
) AS c(customer_name, customer_email) ON TRUE;
