from models import drop_db, init_db

print("Dropping existing database tables...")
drop_db()

print("Recreating database tables...")
init_db()

print("Database reset complete!")
