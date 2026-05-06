-- 002_seed_businesses_customers.down.sql
-- Removes seeded customers and businesses created by 002 up migration

DELETE FROM customers
WHERE email LIKE 'customer1+acmeretailpvtltd@example.com'
   OR email LIKE 'customer2+acmeretailpvtltd@example.com'
   OR email LIKE 'customer3+acmeretailpvtltd@example.com'
   OR email LIKE 'customer1+northstarlogisticsinc@example.com'
   OR email LIKE 'customer2+northstarlogisticsinc@example.com'
   OR email LIKE 'customer3+northstarlogisticsinc@example.com'
   OR email LIKE 'customer1+bluefinlabsllc@example.com'
   OR email LIKE 'customer2+bluefinlabsllc@example.com'
   OR email LIKE 'customer3+bluefinlabsllc@example.com';

DELETE FROM businesses
WHERE name IN ('Acme Retail Pvt Ltd', 'Northstar Logistics Inc', 'Bluefin Labs LLC');
