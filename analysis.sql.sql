
/*
Layihə: Bank Saxtakarlığının Aşkarlanması (Banking Fraud Detection)
Müəllif: Nigar Shahmuradova
Tarix: 12 Noyabr 2025
Məqsəd: Şübhəli əməliyyatların SQL sorğuları ilə tapılması.
*/


1.1 Dinamik RFM Analizi

WITH CustomerLifetime AS (
    SELECT
        customer_id,
        CAST(registration_date AS DATE) AS registration_date,
        TRUNC(SYSDATE - CAST(registration_date AS DATE)) AS total_lifetime_days,
        TRUNC((SYSDATE - CAST(registration_date AS DATE)) / 365) AS lifetime_in_years
    FROM bank_customers
),

TransactionMetrics AS (
    SELECT
        a.customer_id,
        CAST(MAX(t.transaction_date) AS DATE) AS last_transaction_date,
        COUNT(t.transaction_id) AS total_frequency,
        SUM(t.amount) AS total_monetary
    FROM transaction t
    LEFT JOIN bank_account a
        ON t.account_id = a.account_id
    GROUP BY a.customer_id
)

SELECT
    c.customer_id,
    ROUND( (CAST(m.last_transaction_date AS DATE) - c.registration_date) / NULLIF(c.total_lifetime_days, 0),2) AS dynamic_recency_ratio,

    ROUND(m.total_frequency / NULLIF(c.lifetime_in_years, 0),2) AS frequency_per_year,
    ROUND( m.total_monetary / NULLIF(c.lifetime_in_years, 0), 2) AS monetary_per_year
FROM CustomerLifetime c
LEFT JOIN TransactionMetrics m
    ON c.customer_id = m.customer_id;



/*1.2. "Məhsul Penetrasiyası" Skoru*/

WITH Bank_product AS (
    SELECT customer_id, 'Card' AS product_type  FROM bank_cards
    UNION 
    SELECT customer_id, 'Account' AS product_type FROM bank_account
    UNION
    SELECT customer_id, 'Loan' AS product_type FROM loans
),
Customer_product_count AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT product_type) AS total_product_count
    FROM Bank_product
    GROUP BY customer_id
),
Product_penetration AS (
    SELECT 
        customer_id,
        total_product_count,
        ROUND(total_product_count / 3.0, 2) AS product_penetration_score
    FROM Customer_product_count
)

SELECT 
    r.customer_id,
    r.RFM_Segment,
    r.RFM_Score,
    p.product_penetration_score
FROM rfm_segments r
LEFT JOIN Product_penetration p
ON r.customer_id = p.customer_id
WHERE r.RFM_Score >= 4        
  AND p.product_penetration_score < 0.7   
ORDER BY r.RFM_Score DESC, p.product_penetration_score ASC;


/*1.3. "Share of Wallet" Proksi Modeli*/

select c.customer_id,
       trunc(t.transaction_date,'MM') as month_date,
       sum(case when  lower(t.transaction_type)='mədaxil'  and lower(t.category)='maaş' then t.amount else 0 end) as salary,
       sum (case when lower(t.transaction_type)= 'məxaric' then t.amount else 0 end ) as expenses,
      (sum (case when lower(t.transaction_type)= 'məxaric' then t.amount else 0 end )/nullif(sum(case when  lower(t.transaction_type)='mədaxil'  and lower(t.category)='maaş' then t.amount else 0 end),0)) as procsi_amount
from bank_customers c left join bank_cards ca
on c.customer_id=ca.customer_id left join transaction t
on ca.account_id=t.account_id
group by trunc(t.transaction_date,'MM'),c.customer_id
order by month_date	



/*2.2."Sakit Fırtına" Riski

● Tapşırıq: Elə bir sorğu yazın ki, eyni anda aşağıdakı iki şərtə cavab verən müştəriləri tapsın: Bütün hesablarındakı (HESABLAR) ümumi balans bankdakı bütün 
müştərilərin 90%-dən yuxarıdır (yəni, ən varlı 10% arasındadır). Kredit ödənişlərindəki ortalama gecikmə günü isə bütün kredit 
götürənlər arasında ən pis 20%-lik qrupdadır.*/


WITH CustomerTotalBalances AS (
    SELECT
        customer_id,
        SUM(balance) AS total_balance
    FROM bank_account
    GROUP BY customer_id
),
Customer_10_percent AS (
    SELECT customer_id,
           total_balance,
           NTILE(10) OVER (ORDER BY total_balance DESC) AS rank_detail
    FROM CustomerTotalBalances
)
,
Delay_Date AS (
    SELECT 
        l.customer_id,
        round(AVG(lp.payment_date - lp.scheduled_payment_date),2) AS average_delay_days
    FROM loans l
    LEFT JOIN loan_payments lp 
        ON l.loan_id = lp.loan_id
    WHERE lp.payment_date > lp.scheduled_payment_date
    GROUP BY l.customer_id
),
Worst20Percent AS (
    SELECT customer_id,
           average_delay_days,
           NTILE(5) OVER (ORDER BY average_delay_days DESC) AS detail_rank
    FROM Delay_Date
)
SELECT 
    c10.customer_id,
    c10.total_balance,
    w20.average_delay_days
FROM Customer_10_percent c10
LEFT JOIN Worst20Percent w20 
    ON c10.customer_id = w20.customer_id
WHERE c10.rank_detail = 1   
  AND (w20.detail_rank = 1)


/*1. Hərəkətli Balans Hesablaması: 
Hər bir müştərinin hər bir hesabı üçün, hər bir əməliyyatdan sonrakı dəqiq balansı hesablayın.
Bu, hər əməliyyat üçün o tarixə qədərki bütün əməliyyatların məbləğini cəmləməyi (SUM() OVER (...)) tələb edir.*/
SELECT
    account_id,
    transaction_id,
    transaction_date,
    amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY transaction_date, transaction_id) AS running_balance
FROM
    transaction 
ORDER BY 1,2,3
 

/*3.Aylıq Artım Dinamikası (YoY & MoM): Hər bir müştərinin aylıq məxaric məbləğinin əvvəlki aya görə (Month-on-Month) və
keçən ilin eyni ayına görə (Year-on-Year) faizlə dəyişimini hesablayın.*/

WITH Monthly_Spending AS (
    SELECT
        a.customer_id,
        TRUNC(t.transaction_date, 'MM') AS month_date,
        SUM(t.amount) AS total_spending
    FROM transaction t left join bank_account a
    on t.account_id=a.account_id
    WHERE LOWER(transaction_type) = 'məxaric'  -- yalnız məxaric əməliyyatları
    GROUP BY customer_id, TRUNC(transaction_date, 'MM')
),
Calc_Changes AS (
    SELECT
        customer_id,
        month_date,
        total_spending,
        LAG(total_spending) OVER (PARTITION BY customer_id ORDER BY month_date) AS prev_month_spending,
        LAG(total_spending, 12) OVER (PARTITION BY customer_id ORDER BY month_date) AS prev_year_spending
    FROM Monthly_Spending
)
SELECT
    customer_id,
    TO_CHAR(month_date, 'YYYY-MM') AS month,
    total_spending,
    CASE
        WHEN prev_month_spending IS NULL OR prev_month_spending = 0 THEN NULL ELSE ROUND(((total_spending - prev_month_spending) / prev_month_spending) * 100, 2)
    END AS MoM_change_percent,
    CASE
        WHEN prev_year_spending IS NULL OR prev_year_spending = 0 THEN NULL ELSE ROUND(((total_spending - prev_year_spending) / prev_year_spending) * 100, 2)
    END AS YoY_change_percent
FROM Calc_Changes
ORDER BY customer_id, month_date;

/*4.Hər Kateqoriya üzrə Müştərinin Ranqı: Hər bir əməliyyat kateqoriyası (Market, Nəqliyyat və s.) üzrə ən çox xərcləyən müştəriləri sıralayın 
(RANK() OVER (PARTITION BY ...)).*/
WITH Category_Spending AS (
    SELECT
        a.customer_id,
        LOWER(t.category) AS category,
        SUM(t.amount) AS total_spending
    FROM transaction t left join bank_account a
    on t.account_id=a.account_id
    WHERE LOWER(transaction_type) = 'məxaric'   
    GROUP BY customer_id, LOWER(category)
)
SELECT
    category,
    customer_id,
    total_spending,
    RANK() OVER (PARTITION BY category ORDER BY total_spending DESC) AS customer_rank
FROM Category_Spending
ORDER BY category, customer_rank;



/*5. Müştərinin "Ən Aktiv" 3 Ayı: Hər bir müştəri üçün, onun ən çox sayda əməliyyat etdiyi TOP 3 ayı müəyyən edin.*/

WITH Monthly_Transactions AS (
    SELECT
        a.customer_id,
        TRUNC(t.transaction_date, 'MM') AS month_date,
        COUNT(t.transaction_id) AS transaction_count
    FROM transaction t left join bank_account a
    on t.account_id= a.account_id
    GROUP BY customer_id, TRUNC(transaction_date, 'MM')
),
Ranked_Months AS (
    SELECT
        customer_id,
        month_date,
        transaction_count,
        RANK() OVER ( PARTITION BY customer_id ORDER BY transaction_count DESC) AS month_rank
    FROM Monthly_Transactions
)
SELECT
    customer_id,
    TO_CHAR(month_date, 'YYYY-MM') AS active_month,
    transaction_count
FROM Ranked_Months
WHERE month_rank <= 3
ORDER BY customer_id, month_rank;


/*6."İlk Əməliyyat" Kohortu: Hər bir müştərinin ilk əməliyyatının hansı kanaldan (Mobil Bank, ATM...) olduğunu müəyyən edin 
və bu "ilk kanal" kohortlarına görə müştərilərin sonrakı 6 aydakı ortalama xərc məbləğini müqayisə edin.*/
WITH  all_transaction AS (
    SELECT 
        a.customer_id,
        t.transaction_date,
        t.channel,
        t.amount,
        t.transaction_type,
        RANK() OVER (PARTITION BY a.customer_id ORDER BY t.transaction_date) AS rn
    FROM transaction t LEFT JOIN bank_account a 
        ON t.account_id = a.account_id
)
    SELECT 
        customer_id,
        channel AS first_channel,
        transaction_date AS first_transaction_date,
        rn,
        amount
    FROM all_transaction
    WHERE rn = 1
    
    
    
/*7.  Sessiya Analizi: Bir müştərinin 30 dəqiqə içində etdiyi əməliyyatları bir "sessiya" olaraq qəbul edin. 
Hər müştəri üçün ümumi sessiya sayını və hər sessiyadakı orta əməliyyat sayını hesablayın.*/  

WITH table2 AS (
    SELECT 
        a.customer_id,
        t.transaction_date,
        CASE
            WHEN t.transaction_date- LAG(t.transaction_date) OVER (PARTITION BY a.customer_id ORDER BY t.transaction_date)> INTERVAL '30' MINUTE THEN 1 ELSE 0
        END AS new_session
    FROM transaction t
    JOIN bank_account a ON t.account_id = a.account_id
),
sessions AS (
    SELECT
        customer_id,
        transaction_date,
        SUM(new_session) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS session_id
    FROM table2
),
session_stats AS (
    SELECT
        customer_id,
        session_id,
        COUNT(session_id) AS transactions_in_session
    FROM sessions
    GROUP BY customer_id, session_id
)
    SELECT 
        customer_id,
        COUNT(*) AS total_sessions,
       round( AVG(transactions_in_session),2) AS avg_transactions_per_session
    FROM session_stats
    GROUP BY customer_id	
    
    
/*8.Hər Müştərinin Ən Uzun Fasiləsiz Aktivlik Dövrü: Hər bir müştərinin, ən azı ayda bir dəfə əməliyyat etdiyi ardıcıl ayların sayını tapın (ən uzun fasiləsiz aktivlik zənciri).*/
    
 izaha ehtiyac var    
    
    
/*9."Qızıl Üçbucaq": Müştəri-Kredit-Kart Əlaqəsi: 
Həm aktiv krediti, həm də kredit kartı olan, amma son 3 ayda kredit kartı ilə heç bir əməliyyat etməyən müştəriləri tapın.    */

select * from bank_cards
select * from loans
select * from transaction

with merge_table as(
select c.customer_id,
       c.card_id_number,
       c.card_type,
       l.loan_id,
       l.status,
       t.transaction_id,
       t.transaction_date,
       t.amount
 from loans l left join bank_cards c
 on c.customer_id= l.customer_id
 left join transaction t 
 on c.account_id=t.account_id
 where lower(c.card_type)='credit'
 and lower (l.status)='aktiv'
),

Last3Months AS (
    SELECT  customer_id,
        MAX(CASE  WHEN transaction_date >= ADD_MONTHS(TRUNC(SYSDATE,'MM'), -3)  THEN 1 ELSE 0 
            END) AS active_3_months
    FROM Merge_Table
    GROUP BY customer_id
)
select customer_id
from last3months
where active_3_months =0



WITH Loan_Info AS (
    SELECT
        c.customer_id,
        lower(c.customer_category) AS customer_category,
        l.loan_id,
        COALESCE(MAX(p.penalty_number),0) AS max_penalty
    FROM bank_customers c
    JOIN loans l
      ON c.customer_id = l.customer_id
    LEFT JOIN loan_payments p
      ON l.loan_id = p.loan_id
    GROUP BY c.customer_id, c.customer_category, l.loan_id
),
Customer_Summary AS (
    SELECT
        customer_id,
        customer_category,
        COUNT(loan_id) AS total_loans,
        MAX(max_penalty) AS max_penalty_over_all_loans
    FROM Loan_Info
    GROUP BY customer_id, customer_category
)
SELECT
    customer_id AS VIP_customer
FROM Customer_Summary
WHERE customer_category = 'vip'
  AND total_loans >= 2
  AND max_penalty_over_all_loans = 0















