from sqlalchemy import create_engine

db_connection_url = f"postgresql://postgres:postgres@127.0.0.1:5432/postgres"

engine = create_engine(db_connection_url)

