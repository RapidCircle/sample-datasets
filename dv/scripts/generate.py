from pathlib import Path
import pandas as pd
import numpy as np
import json
import random
import string
from datetime import date
from faker import Faker

num_customers = 3000
num_addresses = 4000
num_products = 2000
num_orders = 5000
num_order_items = 15000
num_users = 3500
num_payments = 6000
num_payment_methods = 5


BASE_PATH = Path(__file__).parent / "data"
BASE_PATH.mkdir(parents=True, exist_ok=True)

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


def generate_erp_data(
    num_customers: int,
    num_addresses: int,
    num_products: int,
    out_dir: Path = BASE_PATH,
):
    """
    Generate sample ERP data:
    - erp_customers.csv
    - erp_customer_addresses.csv
    - erp_products.csv
    """
    
    erp_customers = []
    for _ in range(num_customers):
        raw_id = random_string("CUST")
        cust_id = inconsistent_id(raw_id) if random.random() > 0.025 else None

        name = random_customer_name()
        country = introduce_null(random_country(), prob=0.05)
        created_at = fake.date_between(
                        start_date=date(2020, 1, 1),
                        end_date=date(2022, 12, 31)
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

    erp_addresses = []
    customer_ids = erp_customers_df["customer_id"].dropna().tolist()
    for _ in range(num_addresses):
        cust_id = random.choice(customer_ids + [None])
        address = fake.street_address()
        city = fake.city()
        postal_code = fake.postcode()
        state = fake.state()
        erp_addresses.append(
            [cust_id, address, city, state, postal_code]
        )

    erp_addresses_df = pd.DataFrame(
        erp_addresses,
        columns=["customer_id", "address", "city", "state", "postal_code"],
    )
    erp_addresses_df.to_csv(out_dir / "erp_customer_addresses.csv", index=False)

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

    return erp_customers_df, erp_products_df


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
       
        customer_ref = random.choice(
            customer_ids + [random_string("CUST")]
        )
        order_date = fake.date_between(
                        start_date=date(2020, 1, 1),
                        end_date=date(2022, 12, 31)
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
                            end_date=date(2022, 12, 31)
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
    erp_customers_df, erp_products_df = generate_erp_data(
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

    print(f"Data generated under: {BASE_PATH.resolve()}")
