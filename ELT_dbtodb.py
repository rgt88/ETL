import jaydebeapi
import psycopg2
import pandas as pd
import os
from io import StringIO

# Koneksi ke Netezza
nz_host = "000000"
nz_port = "0000"
nz_db = "aaaaa"
nz_user = "aaaaa"
nz_password = "aaaaa"
jdbc_driver_loc = os.path.join("D:\\nzjdbc.jar")

# Koneksi ke PostgreSQL
pg_conn = psycopg2.connect(
    dbname="Data_Quality",
    user="postgres",
    password="aaaaa",
    host="localhost",
    port="5432"
)
pg_cursor = pg_conn.cursor()

# Table name
table_pq = "gen_debitur_cust"

try:
    # Koneksi ke Netezza
    conn_nz = jaydebeapi.connect(
        "org.netezza.Driver",
        f"jdbc:netezza://{nz_host}:{nz_port}/{nz_db}",
        [nz_user, nz_password],
        jdbc_driver_loc
    )
    cursor_nz = conn_nz.cursor()

    # Menghapus tabel dan membuat ulang di PostgreSQL
    print(f"Menghapus tabel {table_pq} pada PostgreSQL...")
    pg_cursor.execute(f'DROP TABLE IF EXISTS "{table_pq}";')
    pg_cursor.execute(f'''
        CREATE TABLE "{table_pq}" (
            periode VARCHAR(10),
            ceiilingamount FLOAT,
            loanbalance FLOAT,
            opendate DATE,
            maturitydate DATE,
            overdueamount FLOAT,
            overdueinstallment FLOAT,
            overdueinterest FLOAT,
            penalty FLOAT,
            monthlyinstallment FLOAT,
            tenor FLOAT,
            dateofbirth DATE,
            installmentdate DATE,
            downpayment FLOAT,
            penghasilan VARCHAR(255),
            marital_status VARCHAR(50),
            education VARCHAR(50),
            customerid VARCHAR(50),
            accountno VARCHAR(50),
            collectibility VARCHAR(50),
            financingtypecode VARCHAR(50),
            branchcode VARCHAR(50),
            occupation VARCHAR(100),
            gender VARCHAR(10),
            segmentasi VARCHAR(100),
            collateral_category VARCHAR(100)
        );
    ''')
    pg_conn.commit()
    print(f"Tabel {table_pq} telah dibuat ulang.")

    # Ambil data dalam batch
    batch_size = 100000
    offset = 0

    while True:
        query = f"""
        SELECT
            PERIODE,
            CEIILINGAMOUNT,
            LOANBALANCE,
            OPENDATE,
            MATURITYDATE,
            OVERDUEAMOUNT,
            OVERDUEINSTALLMENT,
            OVERDUEINTEREST,
            PENALTY,
            MONTHLYINSTALLMENT,
            TENOR,
            DATEOFBIRTH,
            INSTALLMENTDATE,
            DOWNPAYMENT,
            PENGHASILAN,
            MARITAL_STATUS,
            EDUCATION,
            CUSTOMERID,
            ACCOUNTNO,
            COLLECTIBILITY,
            FINANCINGTYPECODE,
            BRANCHCODE,
            OCCUPATION,
            GENDER,
            SEGMENTASI,
            COLLATERAL_CATEGORY
        FROM aaaa.GEN_DEBITUR_CONSUMER
        WHERE PERIODE = '202501'
        ORDER BY PERIODE DESC
        LIMIT {batch_size} OFFSET {offset};
        """

        cursor_nz.execute(query)
        batch = cursor_nz.fetchall()

        if not batch:
            break  # Hentikan jika sudah tidak ada data

        # Ambil nama kolom dan ubah ke huruf kecil
        columns = [desc[0].lower() for desc in cursor_nz.description]

        # Konversi ke DataFrame dengan nama kolom kecil
        df = pd.DataFrame(batch, columns=columns)

        # Hapus karakter NULL di seluruh data
        df = df.map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x)

        # Hapus newline untuk mencegah error "literal newline found in data"
        df = df.map(lambda x: x.replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x)

        # Konversi kolom numerik dan tanggal ke tipe data yang benar
        numeric_cols = ["ceiilingamount", "loanbalance", "overdueamount", "overdueinstallment", "overdueinterest", "penalty", "monthlyinstallment", "downpayment", "tenor"]
        date_cols = ["opendate", "maturitydate", "dateofbirth", "installmentdate"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # Simpan ke buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep="|", encoding="utf-8", na_rep="", quotechar='"')
        buffer.seek(0)

        # Copy ke PostgreSQL dengan kolom yang sesuai
        pg_cursor.copy_from(buffer, table_pq, sep="|", null="", columns=df.columns.tolist())
        pg_conn.commit()

        offset += batch_size
        print(f"Batch {offset} inserted.")

        # Kosongkan batch dari memori untuk efisiensi
        del batch

except Exception as e:
    print(f"Error: {e}")

finally:
    # Pastikan koneksi ditutup
    if "cursor_nz" in locals():
        cursor_nz.close()
    if "conn_nz" in locals():
        conn_nz.close()
    pg_cursor.close()
    pg_conn.close()

print("ELT selesai.")
