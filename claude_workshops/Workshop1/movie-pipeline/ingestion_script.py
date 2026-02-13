#!/usr/bin/env python
# coding: utf-8

# In[3]:


import argparse
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# In[5]:


dtype = {
    "rating_id": "Int64",
    "movie_title": "string",
    "user_id": "Int64",
    "rating": "float64",
    "review_length": "Int64"
}

parse_dates = ["rating_date"]


def main():
    parser = argparse.ArgumentParser(description='Ingest movie ratings into PostgreSQL')

    parser.add_argument('--csv-path', required=True, help='Path to CSV file')
    parser.add_argument('--pg-host', required=True, help='PostgreSQL host')
    parser.add_argument('--pg-port', required=True, help='PostgreSQL port')
    parser.add_argument('--pg-user', required=True, help='PostgreSQL user')
    parser.add_argument('--pg-password', required=True, help='PostgreSQL password')
    parser.add_argument('--pg-db', required=True, help='PostgreSQL database')
    parser.add_argument('--table-name', required=True, help='Target table name')

    args = parser.parse_args()

    # Create engine
    engine = create_engine(f'postgresql://{args.pg_user}:{args.pg_password}@{args.pg_host}:{args.pg_port}/{args.pg_db}')

    # Read CSV in chunks
    df_iter = pd.read_csv('data/movie_ratings.csv',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=2000
)
    # Create table with first chunk
    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(name="movie_ratings", con=engine, if_exists="append")

    print("Inserted first chunk:", len(first_chunk))
    # Insert remaining chunks
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
        name="movie_ratings",
        con=engine,
        if_exists="append"
    )
        print("Inserted chunk:", len(df_chunk))

if __name__ == '__main__':
    main()


# In[ ]:




