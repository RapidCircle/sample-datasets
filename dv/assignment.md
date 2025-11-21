# Data Engineering Assignment: Multi-Source Data Vault Integration & Analytics

## Background

Your organization is building a modern data platform to unify and analyze data from multiple business systems:

- **ERP System** (on-premises): Customers, Products, Customer Addresses
- **SaaS Order Management**: Users, Orders, Order Items
- **Payments Platform**: Payments, Payment Methods

Each system has its own data quality issues, such as inconsistent IDs, missing values, and mismatched references. Your goal is to design a robust, scalable data integration and analytics solution using Data Vault 2.0 methodology.

**Sample datasets are available here:** 
[https://github.com/RpidCircle/sample-datasets](https://github.com/RpidCircle/sample-datasets)

---

## Assignment Objectives

1. **Data Integration:** Ingest and integrate data from all source systems.
2. **Data Vault Modeling:** Design and implement a Data Vault 2.0 model (Hubs, Links, Satellites) to unify and historize the data.
3. **Construct Silver Layer:** adhering to Data Vault 2.0
4. **Communication:** Demonstrate your solution.

---

## Deliverables

1. **Data Loading Scripts:** Scripts (Python/SQL) to ingest the provided CSV/JSON files.
2. **Data Vault Model:**
   - Implementation of Hubs, Links, Satellites in your database and/or Fabric
   - Documentation of modeling choices and how you handle source discrepancies
   - Models for raw, staging, Data Vault
   - Data quality checks(uniqueness, referential integrity, null checks)
   - Code comments
4. **Data Vault Insights:**
   - Build a PowerBI model directly on RAW Data Vault to show raw truth
5. **Documentation & Presentation:**
   - Architecture and data flow diagram

---

## Business Questions & KPIs

Your solution should enable answering the following advanced questions and KPIs on RAW Vault:

1. **Customer Churn & Lifetime Value**
   - Which customers have stopped placing orders for more than 6 months, and what was their average order value before churn?
   - What is the lifetime value (LTV) of each customer, considering all orders and payments?

2. **Order-Payment Anomalies**
   - Are there any orders where the payment amount does not match the order amount, and how frequently does this occur by customer segment?

3. **Multi-Source Data Consistency**
   - Are there discrepancies between customer records in the ERP and those referenced in SaaS orders or payments? What is the impact on revenue reporting?

4. **Payment Timeliness**
   - What is the average number of days between order date and payment date, tracked monthly and by customer segment?

5. **Data Quality & Completeness**
   - What percentage of orders have missing or incomplete payment information, and how does this trend over time?

---

**Dataset files:**  
- ERP: `erp_customers.csv`, `erp_customer_addresses.csv`, `erp_products.csv`  
- SaaS: `saas_users.csv`, `saas_orders.json`, `saas_order_items.csv`  
- Payments: `payments.csv`, `payment_methods.csv`  

