-- Legacy-style procedures: what gets migrated in a real project. The style is deliberate
-- (cursor where a set operation works, temp tables, GETDATE() as the old code had it
-- before @as_of was added). Each one fills an out_* table.
USE parity;
GO
CREATE OR ALTER PROCEDURE dbo.usp_payment_installments AS
BEGIN
    -- Cursor over payments with a running total per order, the way it was written before
    -- window functions. installment_value = value / installments, rounded to cents.
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS out_payment_installments;
    CREATE TABLE out_payment_installments (
        order_id nvarchar(50), payment_sequential int, payment_value decimal(18, 2),
        installment_value decimal(18, 2), running_total decimal(18, 2));
    DECLARE @order nvarchar(50), @seq int, @value decimal(18, 2), @inst int;
    DECLARE @prev nvarchar(50) = '', @running decimal(18, 2) = 0;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT order_id, CAST(payment_sequential AS int), CAST(payment_value AS decimal(18, 2)),
               CAST(payment_installments AS int)
        FROM raw_order_payments ORDER BY order_id, CAST(payment_sequential AS int);
    BEGIN TRANSACTION;  -- one log flush, not one per row
    OPEN c;
    FETCH NEXT FROM c INTO @order, @seq, @value, @inst;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @order <> @prev SET @running = 0;
        SET @running = @running + @value;
        SET @prev = @order;
        INSERT INTO out_payment_installments
        VALUES (@order, @seq, @value,
                ROUND(@value / CASE WHEN @inst = 0 THEN 1 ELSE @inst END, 2), @running);
        FETCH NEXT FROM c INTO @order, @seq, @value, @inst;
    END;
    CLOSE c; DEALLOCATE c;
    COMMIT;
END;
GO
CREATE OR ALTER PROCEDURE dbo.usp_delivery_metrics @as_of date AS
BEGIN
    -- The old version used GETDATE() for "days overdue of open orders"; @as_of makes it
    -- reproducible (difference 5).
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS out_delivery_metrics;
    SELECT order_id,
           DATEDIFF(day, purchased_at, delivered_at) AS days_to_deliver,
           CASE WHEN delivered_at IS NULL THEN 0
                WHEN CAST(delivered_at AS date) > estimated_on THEN 1 ELSE 0 END AS is_late,
           CASE WHEN delivered_at IS NULL AND order_status NOT IN ('canceled', 'unavailable')
                     AND estimated_on < @as_of
                THEN DATEDIFF(day, estimated_on, @as_of) END AS days_overdue_open
    INTO #typed
    FROM (SELECT order_id, order_status,
                 CAST(order_purchase_timestamp AS datetime2) AS purchased_at,
                 CAST(NULLIF(order_delivered_customer_date, '') AS datetime2) AS delivered_at,
                 CAST(NULLIF(order_estimated_delivery_date, '') AS datetime2) AS estimated_on
          FROM raw_orders) AS o;
    SELECT * INTO out_delivery_metrics FROM #typed;
END;
GO
CREATE OR ALTER PROCEDURE dbo.usp_orders_without_reviews AS
BEGIN
    -- NOT IN: empty as soon as the subquery holds one NULL (difference 1).
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS out_orders_without_reviews;
    SELECT order_id INTO out_orders_without_reviews
    FROM raw_orders
    WHERE order_id NOT IN (SELECT order_id FROM raw_order_reviews);
END;
GO
CREATE OR ALTER PROCEDURE dbo.usp_payment_type_summary AS
BEGIN
    -- Per payment type: payments, value, value per installment. Temp table, then a
    -- guarded division.
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS out_payment_type_summary;
    SELECT payment_type,
           COUNT(*) AS payments,
           SUM(CAST(payment_value AS decimal(18, 2))) AS total_value,
           SUM(CAST(payment_installments AS int)) AS total_installments
    INTO #by_type
    FROM raw_order_payments GROUP BY payment_type;
    SELECT payment_type, payments, total_value, total_installments,
           CAST(total_value / NULLIF(total_installments, 0) AS decimal(18, 4)) AS value_per_installment
    INTO out_payment_type_summary
    FROM #by_type;
END;
GO
