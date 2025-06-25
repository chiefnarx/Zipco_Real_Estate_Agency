import requests
import pandas as pd
import psycopg2


api_key = '8b4757e62f99439d93678edeebb440e7'
url = 'https://api.rentcast.io/v1/properties'
headers = {
    'Accept': 'application/json',
    'X-API-Key': api_key
}
property_limit = 20

cities = [
    {"city": "Austin", "state": "TX"},
    {"city": "Seattle", "state": "WA"},
    {"city": "Phoenix", "state": "AZ"},
    {"city": "Miami", "state": "FL"}
]


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="Zipco_Real_Estate_Agency",
        user="postgres",
        password="MongoDB4luv"
    )

def clean_property_record(record):
    df = pd.DataFrame([record])
    df['assessed_value'] = pd.to_numeric(df['assessed_value'], errors='coerce')
    df['tax_total'] = pd.to_numeric(df['tax_total'], errors='coerce')
    df.drop_duplicates(subset='property_id', keep='last', inplace=True)
    return df.iloc[0].to_dict()


def load_property_record(record):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # --- Insert owner ---
        cursor.execute('''
            INSERT INTO owner (name, mailing_address, email)
            VALUES (%s, %s, %s)
            RETURNING owner_id
        ''', (record['owner_name'], record['mailing_address'], record['email']))
        result = cursor.fetchone()

        if result:
            owner_id = result[0]
        else:
            cursor.execute('''
                SELECT owner_id FROM owner WHERE name = %s AND mailing_address = %s
            ''', (record['owner_name'], record['mailing_address']))
            owner_id = cursor.fetchone()[0]

        # --- Insert property ---
        cursor.execute("SELECT property_id FROM property WHERE formatted_address = %s", (record['formatted_address'],))
        existing = cursor.fetchone()

        if existing:
            print("Property already exists, skipping insert.")
            property_id = existing[0]
        else:
            cursor.execute('''
                INSERT INTO property (formatted_address, property_type, zip_code, county, year_built, assessed_value, tax_total, owner_id, owner_occupied)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING property_id
            ''', (
                record['formatted_address'],
                record['property_type'],
                record['zip_code'],
                record['county'],
                record['year_built'],
                record['assessed_value'],
                record['tax_total'],
                owner_id,
                record['owner_occupied']
            ))
            property_id = cursor.fetchone()[0]

        # --- Insert address ---
        cursor.execute('''
            INSERT INTO address (address_line, county, city, state, zip_code, property_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            record['address_line'],
            record['county'],
            record['city'],
            record['state'],
            record['zip_code'],
            property_id
        ))

        # --- Insert tax_fact ---
        cursor.execute('''
            INSERT INTO tax_fact (property_id, year, assessed_value, improvements_value, total_tax)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            property_id,
            record['year'],
            record['assessed_value'],
            record['improvements_value'],
            record['tax_total']
        ))

        conn.commit()

    except Exception as e:
        print(f"Insert error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


for loc in cities:
    print(f"\n📦 Fetching properties for {loc['city']}, {loc['state']}...")
    response = requests.get(url, headers=headers, params={**loc, "limit": property_limit})

    if response.status_code == 200:
        data = response.json()
        for property in data:
            raw_record = {
                'property_id': property.get('id'),
                'formatted_address': property.get('formattedAddress'),
                'address_line': property.get('addressLine1'),
                'city': property.get('city'),
                'state': property.get('state'),
                'zip_code': property.get('zipCode'),
                'county': property.get('county'),
                'property_type': property.get('propertyType'),
                'year_built': property.get('taxAssessments', {}).get('2024', {}).get('year'),
                'assessed_value': property.get('taxAssessments', {}).get('2024', {}).get('value'),
                'tax_total': property.get('propertyTaxes', {}).get('2024', {}).get('total'),
                'improvements_value': property.get('taxAssessments', {}).get('2024', {}).get('improvementsValue'),
                'year': 2024,
                'owner_name': property.get('owner', {}).get('names', [None])[0],
                'mailing_address': property.get('owner', {}).get('mailingAddress', {}).get('formattedAddress'),
                'email': property.get('owner', {}).get('email'),
                'owner_occupied': property.get('ownerOccupied')
            }

            cleaned_record = clean_property_record(raw_record)
            load_property_record(cleaned_record)
            print("Data inserted successfully.")

    else:
        print(f"API Error {response.status_code}: {response.text}")
