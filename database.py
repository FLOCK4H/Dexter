import asyncpg
import asyncio

async def initialize_db():
    # Connect to the default 'postgres' database as superuser
    conn = await asyncpg.connect(
        database="postgres",
        user="postgres",
        password="admin123",  # Replace with your postgres password
        host="localhost"
    )

    # Enable autocommit for CREATE DATABASE
    await conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    await conn.execute("COMMIT")

    # Check and create user if it does not exist
    user_exists = await conn.fetchval("""
        SELECT 1 FROM pg_roles WHERE rolname = 'dexter_user';
    """)
    if not user_exists:
        await conn.execute("CREATE USER dexter_user WITH PASSWORD 'admin123';")
        print("User 'dexter_user' created.")

    # Check and create database if it does not exist
    db_exists = await conn.fetchval("""
        SELECT 1 FROM pg_database WHERE datname = 'dexter_db';
    """)
    if not db_exists:
        await conn.execute("COMMIT")
        await conn.execute("CREATE DATABASE dexter_db OWNER dexter_user;")
        print("Database 'dexter_db' created.")

    await conn.close()

    # Reconnect to the 'dexter_db' database as the new user
    conn = await asyncpg.connect(
        database="dexter_db",
        user="dexter_user",
        password="admin123",
        host="localhost"
    )

    # Create tables
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS mints (
        mint_id TEXT PRIMARY KEY,
        name TEXT,
        symbol TEXT,
        owner TEXT,
        market_cap DOUBLE PRECISION,
        price_history TEXT,
        price_usd DOUBLE PRECISION,
        liquidity DOUBLE PRECISION,
        open_price DOUBLE PRECISION,
        high_price DOUBLE PRECISION,
        low_price DOUBLE PRECISION,
        current_price DOUBLE PRECISION,
        age DOUBLE PRECISION DEFAULT 0,
        tx_counts TEXT,
        volume TEXT,
        holders TEXT,
        mint_sig TEXT,
        bonding_curve TEXT,
        created INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS stagnant_mints (
        mint_id TEXT PRIMARY KEY,
        name TEXT,
        symbol TEXT,
        owner TEXT,
        holders TEXT,
        price_history TEXT,
        tx_counts TEXT,
        volume TEXT,
        peak_price_change DOUBLE PRECISION,
        peak_market_cap DOUBLE PRECISION,
        final_market_cap DOUBLE PRECISION,
        final_ohlc TEXT,
        mint_sig TEXT,
        bonding_curve TEXT,
        slot_delay TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Create indexes for frequently queried fields
    await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_mints_mint_id ON mints(mint_id);
    CREATE INDEX IF NOT EXISTS idx_stagnant_mints_mint_id ON stagnant_mints(mint_id);
    CREATE INDEX IF NOT EXISTS idx_mints_timestamp ON mints(timestamp);
    CREATE INDEX IF NOT EXISTS idx_stagnant_mints_timestamp ON stagnant_mints(timestamp);
    """)

    await conn.close()
    print("PostgreSQL database, tables, and indexes initialized successfully.")

if __name__ == "__main__":
    asyncio.run(initialize_db())
