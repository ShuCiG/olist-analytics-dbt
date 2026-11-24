USE parity;
GO
BULK INSERT raw_orders FROM '/data/olist_orders_dataset.csv'
    WITH (FORMAT = 'CSV', FIELDQUOTE = '"', FIRSTROW = 2);
BULK INSERT raw_order_payments FROM '/data/olist_order_payments_dataset.csv'
    WITH (FORMAT = 'CSV', FIELDQUOTE = '"', FIRSTROW = 2);
-- raw_order_reviews: loaded from Python (quoted newlines break BULK INSERT on Linux)
GO
