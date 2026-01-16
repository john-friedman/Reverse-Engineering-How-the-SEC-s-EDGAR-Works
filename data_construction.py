from datamule import Sheet, Book, format_accession
import polars as pl
from datetime import datetime


def construct_filings_csv():
    sheet = Sheet('data')
    results = sheet.get_table(
        'sec-filings-lookup',
        filingDate=('2025-06-01', datetime.now().strftime('%Y-%m-%d')),
        returnCols=[
            'accessionNumber',
            'submissionType', 
            'detectedTime',
        ],
        pageSize=25000
    )

    df = pl.DataFrame(
    results,
    schema={
        'accessionNumber': pl.Utf8,
        'submissionType': pl.Utf8,
        'detectedTime': pl.Utf8  # Keep as string initially
    }
    )
    df = df.filter(pl.col('detectedTime').is_not_null())
    df.write_csv('filings.csv')

def download_master_submissions_parquet():
    book = Book()
    book.download_dataset(
        dataset='sec_master_submissions',
        filename = 'sec_master_submissions.parquet'
    )

def construct_detected_time_csv():
    # Load filings csv
    filings_df = pl.read_csv('filings.csv')
    
    # Convert accession to dash format (master already in dash format)
    filings_df = filings_df.with_columns(
        pl.col('accessionNumber').map_elements(
            lambda x: format_accession(x, 'dash'),
            return_dtype=pl.Utf8
        ).alias('accessionNumber')
    )
    
    # Load just accessionNumber, acceptanceDateTime, size columns from master and merge
    master_df = pl.read_parquet(
        'sec_master_submissions.parquet',
        columns=['accessionNumber', 'acceptanceDateTime', 'size']
    )
    
    # Convert acceptanceDateTime to string in ISO format to match detectedTime format
    master_df = master_df.with_columns(
        pl.col('acceptanceDateTime').dt.to_string("%Y-%m-%dT%H:%M:%S.%3fZ").alias('acceptanceDateTime')
    )
    
    # Merge the datasets
    merged_df = filings_df.join(
        master_df,
        on='accessionNumber',
        how='left'
    )
    
    # Output to csv
    merged_df.write_csv('detected_time.csv')
    print(merged_df.head)
    
    print(f"Successfully wrote {len(merged_df)} records to detected_time.csv")
