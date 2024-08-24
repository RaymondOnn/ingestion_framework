
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public
    CREATE TABLE category (
        id TEXT PRIMARY KEY,
        category TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    )
    CREATE TABLE merchant (
        id TEXT PRIMARY KEY,
        merchant_name TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    )
    CREATE TABLE merchant_category (
        id TEXT PRIMARY KEY,
        merchant_id TEXT REFERENCES merchant(id),
        category_id TEXT REFERENCES category(id),
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        UNIQUE (merchant_id, category_id)
    )
    CREATE TABLE city (
        id TEXT PRIMARY KEY,
        city_name TEXT NOT NULL,
        state CHAR(2) NOT NULL,
        city_pop INT NOT NULL,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        UNIQUE (city_name, state)
    )
    CREATE TABLE address (
        id TEXT PRIMARY KEY,
        street TEXT NOT NULL UNIQUE,
        city_id TEXT REFERENCES city(id),
        zip CHAR(5) NOT NULL,
        lat FLOAT8,
        long FLOAT8,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        CHECK(lat >= -90 AND lat <= 90),
        CHECK(long >= -180 AND long <= 190)
    )
    CREATE TABLE customer (
        id TEXT PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(20),
        gender CHAR(1) NOT NULL,
        job VARCHAR(50) NOT NULL,
        birth_date DATE NOT NULL,
        cc_num VARCHAR(20) NOT NULL,
        address_id TEXT REFERENCES address(id),
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        UNIQUE (first_name, last_name, gender, job, birth_date, cc_num)
    )
    CREATE TABLE transactions(
        id TEXT PRIMARY KEY,
        transaction_num TEXT NOT NULL,
        transaction_ts TIMESTAMP NOT NULL,
        customer_id TEXT REFERENCES customer(id),
        merchant_category_id TEXT REFERENCES merchant_category(id),
        amount FLOAT8 NOT NULL,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        UNIQUE (transaction_num, transaction_ts, amount)
    );
CREATE SCHEMA staging
    CREATE TABLE transactions(
        id TEXT,
        transaction_num TEXT NOT NULL,
        transaction_ts TIMESTAMP NOT NULL,
        customer_id TEXT,
        first_name VARCHAR(20),
        last_name VARCHAR(20),
        gender CHAR(1) NOT NULL,
        job VARCHAR(50) NOT NULL,
        birth_date DATE NOT NULL,
        cc_num VARCHAR(20) NOT NULL,
        address_id TEXT,
        street TEXT NOT NULL,
        city_id TEXT,
        city_name TEXT NOT NULL,
        state CHAR(2) NOT NULL,
        city_pop INT NOT NULL,
        zip CHAR(5) NOT NULL,
        lat FLOAT8,
        long FLOAT8,
        merchant_category_id TEXT,
        merchant_id TEXT,
        merchant_name TEXT NOT NULL,
        category_id TEXT,
        category TEXT NOT NULL,
        amount FLOAT8 NOT NULL,
        created_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        updated_at TIMESTAMP  DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
        CHECK(lat >= -90 AND lat <= 90),
        CHECK(long >= -180 AND long <= 180)
    );

-- Trigger Functions
CREATE OR REPLACE FUNCTION phase1()
    RETURNS TRIGGER AS
    $BODY$
        BEGIN
            INSERT INTO public.category(id, category)
                VALUES(new.category_id, new.category)
                ON CONFLICT DO NOTHING;
            INSERT INTO public.merchant(id, merchant_name)
                VALUES(new.merchant_id, new.merchant_name)
                ON CONFLICT DO NOTHING;
            INSERT INTO public.city(id, city_name, state, city_pop)
                VALUES(new.city_id, new.city_name, new.state, new.city_pop)
                ON CONFLICT DO NOTHING;
            RETURN new;
        END;
    $BODY$ language plpgsql;
CREATE OR REPLACE FUNCTION phase2()
    RETURNS TRIGGER AS
    $BODY$
        BEGIN
            WITH ins_bridge AS (
                INSERT INTO public.merchant_category(id, merchant_id, category_id)
                    SELECT DISTINCT
                        transactions.merchant_category_id
                        , transactions.merchant_id
                        , transactions.category_id
                    FROM staging.transactions
                    INNER JOIN public.category cat ON transactions.category_id = cat.id
                    INNER JOIN public.merchant mer ON transactions.merchant_id = mer.id
                    WHERE transactions.merchant_category_id NOT IN (
                        SELECT id FROM public.merchant_category
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
            ),
            ins_address AS (
                INSERT INTO public.address(id, street, city_id, zip, lat, long)
                    SELECT
                        transactions.address_id AS id
                        , transactions.street
                        , transactions.city_id
                        , transactions.zip
                        , transactions.lat
                        , transactions.long
                    FROM staging.transactions
                    INNER JOIN public.city ON transactions.city_id = city.id
                    WHERE transactions.address_id NOT IN (
                        SELECT id FROM public.address
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
            ),
            ins_customer AS (
                INSERT INTO public.customer(
                    id, first_name, last_name, gender, job, birth_date, cc_num, address_id
                )
                    SELECT
                        transactions.customer_id
                        , transactions.first_name
                        , transactions.last_name
                        , transactions.gender
                        , transactions.job
                        , transactions.birth_date
                        , transactions.cc_num
                        , transactions.address_id
                    FROM staging.transactions
                    INNER JOIN ins_address ON transactions.address_id = ins_address.id
                    WHERE transactions.customer_id NOT IN (
                        SELECT id FROM public.customer
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
            )
            INSERT INTO public.transactions(
                id, transaction_num, transaction_ts,
                customer_id, merchant_category_id, amount
            )
                SELECT
                    transactions.id
                    , transactions.transaction_num
                    , transactions.transaction_ts
                    , transactions.customer_id
                    , transactions.merchant_category_id
                    , transactions.amount
                FROM staging.transactions
                INNER JOIN ins_bridge ON transactions.merchant_category_id = ins_bridge.id
                INNER JOIN ins_customer ON transactions.customer_id = ins_customer.id
                ON CONFLICT DO NOTHING;
            RETURN NULL;
        END;
    $BODY$ language plpgsql;


-- The Trigger

CREATE TRIGGER trigger_phase1
    AFTER INSERT ON staging.transactions
    FOR EACH ROW
    EXECUTE FUNCTION phase1();
CREATE TRIGGER trigger_phase2
    AFTER INSERT ON staging.transactions
    FOR EACH STATEMENT
    EXECUTE FUNCTION phase2();

