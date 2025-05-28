# SQL Server Configuration
DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'your_server_name',  # or 'localhost' if local
    'database': 'SubscriptionDB',
    'user': 'your_username',
    'password': 'your_password',
    'trusted_connection': 'no',  # 'yes' for Windows auth
    'port': '1433'
}

# Connection string for pyodbc
CONNECTION_STRING = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['user']};PWD={DB_CONFIG['password']}"