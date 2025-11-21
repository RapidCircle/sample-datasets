from pathlib import Path
import pandas as pd
import numpy as np
import json
import random
import string
from datetime import date, timedelta
from faker import Faker

num_customers = 3000
num_addresses = 4000
num_products = 2000
num_orders = 5000
num_order_items = 15000
num_users = 3500
num_payments = 6000
num_payment_methods = 5

# Base path
BASE_PATH = Path(__file__).parent.parent / "data"
BASE_PATH.mkdir(parents=True, exist_ok=True)

# Seeds
random.seed(42)
np.random.seed(42)
fake = Faker()
Faker.seed(42)


def random_string(prefix, length=5):
    return prefix + "".join(random.choices(string.digits, k=length))


def random_country():
    countries = [
        "United States",
        "USA",
        "Germany",
        "DE",
        "India",
        "IND",
        "Canada",
        "CA",
        "US",
        fake.country(),
    ]
    return random.choice(countries)


def random_customer_name():
    return random.choice(
        [
            fake.company(),
            fake.company_suffix() + " " + fake.last_name(),
            fake.bs().title(),
        ]
    )


def introduce_null(value, prob=0.05):
    return value if random.random() > prob else None


def inconsistent_id(base_id: str):
    """
    Introduce simple ID inconsistencies:
    - Sometimes add a dash after prefix
    - Sometimes lowercase
    """
    if random.random() < 0.2:
        base_id = base_id.replace("CUST", "CUST-")
    if random.random() < 0.1:
        base_id = base_id.lower()
    return base_id


# ---------------------------
# CDC helper for ERP tables
# ---------------------------
def _mutate_row_for_table(row: pd.Series, table_name: str) -> pd.Series:
    """
    Apply a small 'business-meaningful' change per table
    to simulate UPDATE events.
    """
    row = row.copy()
    if table_name == "erp_customers":
        # Change country or name
        if random.random() < 0.5:
            row["country"] = random_country()
        else:
            row["customer_name"] = random_customer_name()
    elif table_name == "erp_customer_addresses":
        # Change city/state/postal code
        if random.random() < 0.33:
            row["city"] = fake.city()
        elif random.random() < 0.66:
            row["state"] = fake.state()
        else:
            row["postal_code"] = fake.postcode()
    elif table_name == "erp_products":
        # Change price a bit or tweak name
        if random.random() < 0.6:
            old_price = float(row["price"])
            factor = random.choice([0.9, 0.95, 1.05, 1.1])
            row["price"] = round(old_price * factor, 2)
        else:
            row["product_name"] = row["product_name"] + " v2"
    return row


def generate_erp_cdc(
    base_df: pd.DataFrame,
    key_col: str,
    table_name: str,
    out_dir: Path,
    start: date = date(2020, 1, 1),
    end: date = date(2022, 12, 31),
    p_update: float = 0.4,
    p_delete: float = 0.15,
) -> pd.DataFrame:
    """
    Generate a CDC stream for an ERP table based on a static snapshot.
    Outputs a CSV named `<table_name>_cdc.csv` with:
      - cdc_table
      - cdc_op (I/U/D)
      - cdc_ts
      - cdc_seq (1,2,3...) per key
      - all original columns

    Only rows with non-null key_col are included.
    """
    records = []

    # Only include rows with a business key
    working_df = base_df[base_df[key_col].notna()].reset_index(drop=True)

    for _, row in working_df.iterrows():
        key_value = row[key_col]

        # Base INSERT event
        # Use created_at if present; else random in range
        if "created_at" in row.index and pd.notna(row["created_at"]):
            try:
                base_ts = pd.to_datetime(row["created_at"]).date()
            except Exception:
                base_ts = fake.date_between(start_date=start, end_date=end)
        else:
            base_ts = fake.date_between(start_date=start, end_date=end)

        cdc_seq = 1
        insert_event = {
            "cdc_table": table_name,
            "cdc_op": "I",
            "cdc_ts": base_ts.isoformat(),
            "cdc_seq": cdc_seq,
        }
        insert_event.update(row.to_dict())
        records.append(insert_event)

        current_ts = base_ts

        # Optional UPDATE event
        if random.random() < p_update:
            cdc_seq += 1
            # ensure ts moves forward
            delta_days = random.randint(1, 365)
            upd_ts = current_ts + timedelta(days=delta_days)
            if upd_ts > end:
                upd_ts = end

            updated_row = _mutate_row_for_table(row, table_name)

            update_event = {
                "cdc_table": table_name,
                "cdc_op": "U",
                "cdc_ts": upd_ts.isoformat(),
                "cdc_seq": cdc_seq,
            }
            update_event.update(updated_row.to_dict())
            records.append(update_event)
            current_ts = upd_ts
            row = updated_row  # latest state

        # Optional DELETE event
        if random.random() < p_delete:
            cdc_seq += 1
            delta_days = random.randint(1, 365)
            del_ts = current_ts + timedelta(days=delta_days)
            if del_ts > end:
                del_ts = end

            delete_event = {
                "cdc_table": table_name,
                "cdc_op": "D",
                "cdc_ts": del_ts.isoformat(),
                "cdc_seq": cdc_seq,
            }
            delete_event.update(row.to_dict())
            records.append(delete_event)

    cdc_df = pd.DataFrame(records)

    # Order by key + timestamp + seq
    cdc_df.sort_values(
        by=[key_col, "cdc_ts", "cdc_seq"],
        inplace=True,
        ignore_index=True,
    )

    out_path = out_dir / f"{table_name}_cdc.csv"
    cdc_df.to_csv(out_path, index=False)
    return cdc_df


# ---------------------------
# ERP data (snapshots + CDC)
# ---------------------------
def generate_erp_data(
    num_customers: int,
    num_addresses: int,
    num_products: int,
    out_dir: Path = BASE_PATH,
):
    """
    Generate sample ERP data:
    - erp_customers.csv (+ erp_customers_cdc.csv)
    - erp_customer_addresses.csv (+ erp_customer_addresses_cdc.csv)
    - erp_products.csv (+ erp_products_cdc.csv)
    """

    # --- Customers snapshot ---
    erp_customers = []
    for _ in range(num_customers):
        raw_id = random_string("CUST")
        # ~2.5% missing IDs
        cust_id = inconsistent_id(raw_id) if random.random() > 0.025 else None

        name = random_customer_name()
        country = introduce_null(random_country(), prob=0.05)
        created_at = fake.date_between(
            start_date=date(2020, 1, 1),
            end_date=date(2022, 12, 31),
        )

        erp_customers.append(
            [
                cust_id,
                name,
                country,
                str(created_at),
            ]
        )

    erp_customers_df = pd.DataFrame(
        erp_customers,
        columns=["customer_id", "customer_name", "country", "created_at"],
    )
    erp_customers_df.to_csv(out_dir / "erp_customers.csv", index=False)

    # --- Addresses snapshot ---
    erp_addresses = []
    customer_ids = erp_customers_df["customer_id"].dropna().tolist()
    for _ in range(num_addresses):
        cust_id = random.choice(customer_ids + [None])
        address = fake.street_address()
        city = fake.city()
        postal_code = fake.postcode()
        state = fake.state()
        erp_addresses.append([cust_id, address, city, state, postal_code])

    erp_addresses_df = pd.DataFrame(
        erp_addresses,
        columns=["customer_id", "address", "city", "state", "postal_code"],
    )
    erp_addresses_df.to_csv(out_dir / "erp_customer_addresses.csv", index=False)

    # --- Products snapshot ---
    erp_products = []
    for _ in range(num_products):
        product_id = random_string("PROD")
        product_name = random.choice(
            [
                fake.word().title(),
                fake.catch_phrase(),
                fake.color_name() + " " + fake.word().title(),
            ]
        )
        price = round(random.uniform(10, 500), 2)
        category = random.choice(
            ["Hardware", "Software", "Subscription", "Service", "Consumables"]
        )
        erp_products.append([product_id, product_name, category, price])

    erp_products_df = pd.DataFrame(
        erp_products,
        columns=["product_id", "product_name", "category", "price"],
    )
    erp_products_df.to_csv(out_dir / "erp_products.csv", index=False)

    # --- CDC generation for ERP tables ---
    customers_cdc_df = generate_erp_cdc(
        erp_customers_df,
        key_col="customer_id",
        table_name="erp_customers",
        out_dir=out_dir,
        start=date(2020, 1, 1),
        end=date(2024, 12, 31),
    )

    addresses_cdc_df = generate_erp_cdc(
        erp_addresses_df,
        key_col="customer_id",
        table_name="erp_customer_addresses",
        out_dir=out_dir,
        start=date(2020, 1, 1),
        end=date(2024, 12, 31),
    )

    products_cdc_df = generate_erp_cdc(
        erp_products_df,
        key_col="product_id",
        table_name="erp_products",
        out_dir=out_dir,
        start=date(2020, 1, 1),
        end=date(2024, 12, 31),
    )

    return (
        erp_customers_df,
        erp_products_df,
        customers_cdc_df,
        addresses_cdc_df,
        products_cdc_df,
    )


# ---------------------------
# SaaS + Payments (unchanged)
# ---------------------------
def generate_saas_data(
    num_orders: int,
    num_order_items: int,
    num_users: int,
    erp_customers_df: pd.DataFrame,
    erp_products_df: pd.DataFrame,
    out_dir: Path = BASE_PATH,
):
    """
    Generate SaaS system data:
    - saas_users.csv
    - saas_orders.json
    - saas_order_items.csv
    """
    saas_users = []
    for _ in range(num_users):
        user_id = random_string("USER")
        person_name = fake.name()
        email_local = person_name.replace(" ", ".").replace("'", "").lower()
        domain = random.choice(["example.com", "acme.io", "saasapp.com"])
        email = f"{email_local}{random.randint(1, 999)}@{domain}"
        saas_users.append([user_id, person_name, email])

    saas_users_df = pd.DataFrame(saas_users, columns=["user_id", "name", "email"])
    saas_users_df.to_csv(out_dir / "saas_users.csv", index=False)

    saas_orders = []
    customer_ids = erp_customers_df["customer_id"].dropna().tolist()
    statuses = ["Shipped", "shipped", "Pending", "pending", "Delivered"]

    for _ in range(num_orders):
        order_id = random_string("ORD")
        customer_ref = random.choice(customer_ids + [random_string("CUST")])
        order_date = fake.date_between(
            start_date=date(2020, 1, 1),
            end_date=date(2022, 12, 31),
        )

        amount = introduce_null(
            round(random.uniform(100, 5000), 2),
            prob=0.035,
        )
        status = random.choice(statuses)
        currency = random.choice(["USD", "EUR", "INR", "CAD"])

        saas_orders.append(
            {
                "order_id": order_id,
                "customer_ref": customer_ref,
                "order_date": str(order_date),
                "amount": amount,
                "currency": currency,
                "status": status,
            }
        )

    with open(out_dir / "saas_orders.json", "w") as f:
        json.dump(saas_orders, f, indent=2)

    order_items = []
    product_ids = erp_products_df["product_id"].tolist()
    order_ids = [o["order_id"] for o in saas_orders]

    for _ in range(num_order_items):
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        discount_pct = random.choice([0, 0, 0, 5, 10, 15])
        order_items.append([order_id, product_id, quantity, discount_pct])

    order_items_df = pd.DataFrame(
        order_items,
        columns=["order_id", "product_id", "quantity", "discount_pct"],
    )
    order_items_df.to_csv(out_dir / "saas_order_items.csv", index=False)

    return saas_orders, saas_users_df, order_items_df


def generate_payments_data(
    num_payments: int,
    saas_orders: list[dict],
    out_dir: Path = BASE_PATH,
):
    """
    Generate payments data:
    - payment_methods.csv
    - payments.csv
    """
    payment_methods = [
        "Credit Card",
        "CreditCard",
        "Bank Transfer",
        "BankTransfer",
        "Cash",
    ]
    payment_methods_df = pd.DataFrame(payment_methods, columns=["payment_method"])
    payment_methods_df.to_csv(out_dir / "payment_methods.csv", index=False)

    order_ids = [o["order_id"] for o in saas_orders]

    payments = []
    for _ in range(num_payments):
        payment_id = random_string("PAY")
        if random.random() > 0.065:
            order_ref = random.choice(order_ids + [None])
        else:
            order_ref = None

        payment_date = fake.date_between(
            start_date=date(2020, 1, 1),
            end_date=date(2022, 12, 31),
        )
        payment_amount = round(random.uniform(50, 5000), 2)
        payment_method = random.choice(payment_methods)

        payments.append(
            [
                payment_id,
                order_ref,
                str(payment_date),
                payment_amount,
                payment_method,
            ]
        )

    payments_df = pd.DataFrame(
        payments,
        columns=[
            "payment_id",
            "order_ref",
            "payment_date",
            "payment_amount",
            "payment_method",
        ],
    )
    payments_df.to_csv(out_dir / "payments.csv", index=False)

    return payments_df


if __name__ == "__main__":
    (
        erp_customers_df,
        erp_products_df,
        customers_cdc_df,
        addresses_cdc_df,
        products_cdc_df,
    ) = generate_erp_data(
        num_customers=num_customers,
        num_addresses=num_addresses,
        num_products=num_products,
        out_dir=BASE_PATH,
    )

    saas_orders, saas_users_df, order_items_df = generate_saas_data(
        num_orders=num_orders,
        num_order_items=num_order_items,
        num_users=num_users,
        erp_customers_df=erp_customers_df,
        erp_products_df=erp_products_df,
        out_dir=BASE_PATH,
    )

    payments_df = generate_payments_data(
        num_payments=num_payments,
        saas_orders=saas_orders,
        out_dir=BASE_PATH,
    )

    print(f"Data (snapshots + CDC) generated under: {BASE_PATH.resolve()}")
