import random
import os
from dotenv import load_dotenv
import time
import psycopg2
from faker import Faker
from datetime import datetime
import pytz

fake = Faker()
# Charger les variables d'environnement du fichier .env
load_dotenv()
# Connexion à PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")  
)
cursor = conn.cursor()

# Vérifier si la table existe et la créer si elle n'existe pas
def check_and_create_table():
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'transactions7'
        );
    """)
    exists = cursor.fetchone()[0]

    if not exists:
        cursor.execute("""
            CREATE TABLE transactions7 (
                transactionid UUID PRIMARY KEY,
                productid VARCHAR(255),
                productname VARCHAR(255),
                productcategory VARCHAR(255),
                productprice DECIMAL,
                productquantity INT,
                productbrand VARCHAR(255),
                currency VARCHAR(10),
                customerid VARCHAR(255),
                transactiondate TIMESTAMP WITHOUT TIME ZONE,
                paymentmethod VARCHAR(50),
                totalamount DECIMAL  -- Nouvelle colonne pour le montant total
            );
        """)
        conn.commit()
        print("Table 'transactions7' created.")
    else:
        print("Table 'transactions7' already exists.")

# Définir le fuseau horaire d'Antananarivo
madagascar_timezone = pytz.timezone('Indian/Antananarivo')

def generate_sales_transactions():
    user = fake.simple_profile()
   # Obtenir l'heure actuelle avec le fuseau horaire d'Antananarivo
    transaction_date_local = datetime.now(madagascar_timezone)  
  # Convertir la date locale en UTC
    transaction_date_utc = transaction_date_local.astimezone(pytz.utc)

    product_price = round(random.uniform(10, 1000), 2)
    product_quantity = random.randint(1, 10)
    total_amount = product_price * product_quantity

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': product_price,
        'productQuantity': product_quantity,
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': transaction_date_utc.isoformat(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "totalAmount": total_amount  # Calcul du montant total
    }

def insert_into_postgresql(transaction):
    try:
        # Insérer la transaction dans la base de données
        cursor.execute("""
            INSERT INTO transactions7 (
                transactionid, productid, productname, productcategory, 
                productprice, productquantity, productbrand, currency, 
                customerid, transactiondate, paymentmethod, totalamount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction['transactionId'], transaction['productId'], transaction['productName'], 
            transaction['productCategory'], transaction['productPrice'], transaction['productQuantity'], 
            transaction['productBrand'], transaction['currency'], transaction['customerId'], 
            transaction['transactionDate'], transaction['paymentMethod'], transaction['totalAmount']
        ))

        # Valider la transaction
        conn.commit()
    except Exception as e:
        print(f"Erreur lors de l'insertion dans PostgreSQL : {e}")
        conn.rollback()

def main():
    check_and_create_table()  # Vérifier et créer la table si nécessaire

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            print(transaction)

            # Envoyer à PostgreSQL
            insert_into_postgresql(transaction)

            # Attendre 5 secondes avant d'envoyer la prochaine transaction
            time.sleep(5)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()

# Fermer la connexion à PostgreSQL à la fin du script
cursor.close()
conn.close()
