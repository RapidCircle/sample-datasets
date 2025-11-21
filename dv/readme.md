# DV Dataset Overview

Synthetic but realistic ERP + SaaS + Payments data for testing pipelines, data quality checks, entity resolution, and Data Vault modeling.

1. ERP System Data

| File Name                      | Description                       | Key Columns                                              | Row Count | Notes                                                    |
| ------------------------------ | --------------------------------- | -------------------------------------------------------- | --------- | -------------------------------------------------------- |
| **erp_customers.csv**          | Master list of customers from ERP | `customer_id`, `customer_name`, `country`, `created_at`  | ~3000     | Includes inconsistent IDs, null countries, mixed formats |
| **erp_customer_addresses.csv** | Addresses linked to ERP customers | `customer_id`, `address`, `city`, `state`, `postal_code` | ~4000     | Some addresses have no customer; used for enrichment     |
| **erp_products.csv**           | Product master data               | `product_id`, `product_name`, `category`, `price`        | ~2000     | Realistic product names, categories, prices              |

2. SaaS System Data

| File Name                | Description                   | Key Columns                                                              | Row Count | Notes                                                                     |
| ------------------------ | ----------------------------- | ------------------------------------------------------------------------ | --------- | ------------------------------------------------------------------------- |
| **saas_users.csv**       | Users of the SaaS application | `user_id`, `name`, `email`                                               | ~3500     | Realistic user names/emails                                               |
| **saas_orders.json**     | Orders placed in SaaS system  | `order_id`, `customer_ref`, `order_date`, `amount`, `currency`, `status` | ~5000     | Includes invalid customer references, null amounts, inconsistent statuses |
| **saas_order_items.csv** | Line items for each order     | `order_id`, `product_id`, `quantity`, `discount_pct`                     | ~15000    | Links orders to products with random quantities                           |


3. Payments Data

| File Name               | Description                   | Key Columns                                                                   | Row Count | Notes                                                       |
| ----------------------- | ----------------------------- | ----------------------------------------------------------------------------- | --------- | ----------------------------------------------------------- |
| **payment_methods.csv** | Allowed payment method values | `payment_method`                                                              | 5         | Contains inconsistent labels (“Credit Card” / “CreditCard”) |
| **payments.csv**        | Payments linked to orders     | `payment_id`, `order_ref`, `payment_date`, `payment_amount`, `payment_method` | ~6000     | Includes orphan payments with missing `order_ref`           |


