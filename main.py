import pandas as pd
import configparser
import hashlib

from config.pg_config import PostgresConfig
from sqlalchemy.dialects.postgresql import insert

config = configparser.ConfigParser()
config.read("config.ini")


def insert_on_conflict_update(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data)
    conflict_update = insert_statement.on_conflict_do_update(
        constraint="banana_data_pk",
        set_={column.key: column for column in insert_statement.excluded},
    )
    result = conn.execute(conflict_update)
    return result.rowcount


def generate_id(name: str):
    return hashlib.md5(name.encode("utf-8")).hexdigest()


def main():
    # Get Data From CSV Into Dataframe
    df = pd.read_csv(
        "resources/banana_quality_good.csv",
        sep=";"
    )

    # Standardization Column, a convert type data, and create id column
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.astype(
        {
            "size": float,
            "weight": float,
            "sweetness": float,
            "harvest_time": float,
            "ripeness": float,
            "acidity": float
        }
    )
    df["id"] = df.apply(lambda x: generate_id(x["name"]), axis=1)

    # Initialization class postgres config
    connect = PostgresConfig(**config["MY_PG"]).client_connect()
    row_count = df.to_sql(
        "banana_data",
        index=False,
        con=connect,
        if_exists="append",
        method=insert_on_conflict_update
    )
    print("Inserted:", row_count)
    connect.commit()
    connect.close()


if __name__ == '__main__':
    main()
