import argparse
import os
import sys

import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Impor dataset RetailRocket (events.csv) ke database Neo4J."
    )
    parser.add_argument("-u", "--user", default="neo4j",
                        help="Username Neo4j (default: neo4j)")
    parser.add_argument("-p", "--password", default="password123",
                        help="Password Neo4J (default: password123)")
    parser.add_argument("-b", "--bolt", default="bolt://127.0.0.1:7687",
                        help="Neo4J URI / Bolt (default: bolt://127.0.0.1:7687)")
    parser.add_argument("-s", "--source", default="data",
                        help="Direktori/folder sumber data (default: data)")
    parser.add_argument("-f", "--file", default="events.csv",
                        help="Nama file CSV (default: events.csv)")
    parser.add_argument("-n", "--limit", type=int, default=None,
                        help="Batasi jumlah baris (untuk uji coba)")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Jumlah baris per batch (default: 10000)")
    return parser.parse_args()


def create_constraints(session):
    # Constraint = unique + index, membuat MERGE jauh lebih cepat
    session.run(
        "CREATE CONSTRAINT visitor_id IF NOT EXISTS "
        "FOR (v:Visitor) REQUIRE v.visitorId IS UNIQUE"
    )
    session.run(
        "CREATE CONSTRAINT item_id IF NOT EXISTS "
        "FOR (i:Item) REQUIRE i.itemId IS UNIQUE"
    )


def import_batch(tx, batch):
    cypher_query = """
    UNWIND $batch AS row
    MERGE (v:Visitor {visitorId: row.visitorid})
    MERGE (i:Item {itemId: row.itemid})

    WITH v, i, row
    CALL {
        WITH v, i, row
        WITH v, i, row WHERE row.event = 'view'
        CREATE (v)-[:VIEWED {timestamp: row.timestamp}]->(i)

        UNION ALL

        WITH v, i, row
        WITH v, i, row WHERE row.event = 'addtocart'
        CREATE (v)-[:ADDED_TO_CART {timestamp: row.timestamp}]->(i)

        UNION ALL

        WITH v, i, row
        WITH v, i, row WHERE row.event = 'transaction'
        CREATE (v)-[:BOUGHT {timestamp: row.timestamp,
                             transactionId: row.transactionid}]->(i)
    }
    """
    tx.run(cypher_query, batch=batch)


def main():
    args = parse_arguments()

    # 1. Cek file
    csv_file_path = os.path.join(args.source, args.file)
    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' tidak ditemukan!")
        sys.exit(1)

    # 2. Koneksi
    try:
        driver = GraphDatabase.driver(args.bolt, auth=(args.user, args.password))
        driver.verify_connectivity()
    except Exception as e:
        print(f"Gagal terhubung ke database: {e}")
        sys.exit(1)

    # 3. Impor batch
    batch_size = args.batch_size

    with driver.session(database="neo4j") as session:
        create_constraints(session)

        print(f"Membaca file data dari: {csv_file_path}")
        chunks = pd.read_csv(
            csv_file_path,
            chunksize=batch_size,
            nrows=args.limit,
            dtype={"transactionid": "string"},  
        )

        # Hitung total baris (dikurangi header)
        with open(csv_file_path, encoding="utf-8") as f:
            total_rows = sum(1 for _ in f) - 1
        if args.limit:
            total_rows = min(total_rows, args.limit)
        total_chunks = (total_rows // batch_size) + 1

        print("Memulai proses impor data ke Neo4j...")
        for chunk in tqdm(chunks, total=total_chunks, desc="Mengimpor Batch Data"):
            # Membersihkan data null/NaN pada transactionid
            chunk["transactionid"] = chunk["transactionid"].fillna("").astype(str)

            records = chunk.to_dict(orient="records")
            session.execute_write(import_batch, records)

    driver.close()
    print("Proses impor berhasil.")


if __name__ == "__main__":
    main()