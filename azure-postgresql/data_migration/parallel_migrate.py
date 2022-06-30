import os


def _url(d: str) -> str:
    host: str = os.environ[f'{d}_HOST']
    port: str = os.environ.get(f'{d}_PORT', '5432')
    dbname: str = os.environ[f'{d}_DBNAME']
    user: str = os.environ[f'{d}_USER']
    password: str = os.environ[f'{d}_PASSWORD']
    sslmode: str = os.environ.get(f'{d}_SSLMODE', 'require')
    return f'postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}'


def main() -> None:
    source_url: str = _url('SOURCE')
    source_table: str = os.environ['SOURCE_TABLE']
    dest_url: str = _url('DEST')
    dest_table: str = os.environ['DEST_TABLE']
    total_threads: int = int(os.environ.get('TOTAL_THREADS', 1))  # single threaded by default
    chunk_size = int(os.environ.get('CHUNK_SIZE'), 8)
    interval: int = chunk_size / total_threads
    start: int = 0
    end: int = start + interval

    # "threading", a.k.a. fork to background with os.system and '&' calls
    for i in range(total_threads):
        if(i != total_threads - 1):
            select_query = f'"\COPY (SELECT * from {source_table} WHERE id>={start} AND id<{end}) TO STDOUT"'
            read_query = f"psql \"{source_url}\" -c {select_query}"
            write_query = f"psql \"{dest_url}\" -c \"\COPY {dest_table} FROM STDIN\""
            os.system(f'{read_query} | {write_query} &')
        else:
            read_query = f"psql \"{source_url}\" -c {select_query}"
            write_query = f"psql \"{dest_url}\" -c \"\COPY {dest_table} FROM STDIN\""
            os.system(f'{read_query} | {write_query}')
            start = end
            end = start + interval


if __name__ == '__main__':
    main()
