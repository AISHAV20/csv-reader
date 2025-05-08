# csv-reader

1. Clone the Repository
    git clone https://github.com/your-username/csv-reader.git
    cd csv-reader

2. Create Python Virtual Environment (Python 3.11)
    python3.11 -m venv venv
    source venv/bin/activate 

3. Install Dependencies
    pip install -r requirements.txt

4. Configure Environment Variables
    Create a .env file in the root directory with the following content:
    DATABASE_URL=postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_SCHEMA}

5. Run the code 
    python main.py
