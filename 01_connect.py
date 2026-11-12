"""Step 1: connect to MongoDB Atlas and prove it works."""
from config import DB_NAME, get_client

client = get_client()
print("ping:", client.admin.command("ping"))          # {'ok': 1.0} means we are connected
print("server version:", client.server_info()["version"])
print("using database:", DB_NAME)
print("existing databases:", client.list_database_names())
