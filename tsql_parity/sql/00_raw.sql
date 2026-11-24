-- Raw Olist tables as dlt lands them: text columns, no types. Typing happens in the procedures.
IF DB_ID('parity') IS NULL CREATE DATABASE parity;
GO
USE parity;
GO
DROP TABLE IF EXISTS raw_orders, raw_order_payments, raw_order_reviews;
GO
CREATE TABLE raw_orders (
    order_id nvarchar(50), customer_id nvarchar(50), order_status nvarchar(30),
    order_purchase_timestamp nvarchar(30), order_approved_at nvarchar(30),
    order_delivered_carrier_date nvarchar(30), order_delivered_customer_date nvarchar(30),
    order_estimated_delivery_date nvarchar(30));
CREATE TABLE raw_order_payments (
    order_id nvarchar(50), payment_sequential nvarchar(10), payment_type nvarchar(30),
    payment_installments nvarchar(10), payment_value nvarchar(30));
CREATE TABLE raw_order_reviews (
    review_id nvarchar(50), order_id nvarchar(50), review_score nvarchar(5),
    review_comment_title nvarchar(max), review_comment_message nvarchar(max),
    review_creation_date nvarchar(30), review_answer_timestamp nvarchar(30));
GO
