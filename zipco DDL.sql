-- Owner Table
CREATE TABLE owner (
    owner_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    mailing_address TEXT,
    email VARCHAR(100) UNIQUE
);

-- Property Table
CREATE TABLE property (
    property_id SERIAL PRIMARY KEY,
    formatted_address TEXT,
    property_type VARCHAR(100),
    zip_code VARCHAR(10),
    county VARCHAR(100),
    year_built INT,
    assessed_value DOUBLE PRECISION,
    tax_total DOUBLE PRECISION,
    owner_id INT,
    FOREIGN KEY (owner_id) REFERENCES owner(owner_id) ON DELETE SET NULL
	-- I used SET NULL so if an owner gets deleted here, it wont delete the property info associated with them.
	-- It would just show NULL in that column.
);

-- Address Table
CREATE TABLE address (
    address_id SERIAL PRIMARY KEY,
    address_line TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    property_id INT,
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
);

-- Tax Fact Table
CREATE TABLE tax_fact (
    property_id INT REFERENCES property(property_id) ON DELETE CASCADE,
    year INT,
    assessed_value DOUBLE PRECISION,
    improvements_value DOUBLE PRECISION,
    total_tax DOUBLE PRECISION,
    PRIMARY KEY (property_id, year)
);

ALTER TABLE property ADD COLUMN owner_occupied BOOLEAN;
ALTER TABLE owner ADD CONSTRAINT unique_owner UNIQUE (name, mailing_address);
SELECT * FROM owner ORDER BY owner_id DESC LIMIT 20;
SELECT * FROM property ORDER BY property_id DESC;
SELECT * FROM tax_fact LIMIT 5;


ALTER TABLE address ADD COLUMN county VARCHAR(50);
UPDATE address
SET county = p.county
FROM property p
WHERE address.property_id = p.property_id
  AND (address.county IS NULL OR address.county = '');

SELECT address_line, county FROM address LIMIT 10;

