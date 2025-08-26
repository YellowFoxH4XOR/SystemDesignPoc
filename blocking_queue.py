import sqlalchemy as sa
import threading
import queue
import time

def create_mysql_connection():
    """
    Creates and returns a connection to the MySQL database.

    Returns:
        sqlalchemy.engine.base.Connection: A connection to the MySQL database.
    """
    try:
        # Create SQLAlchemy engine for MySQL
        engine = sa.create_engine('mysql+pymysql://root:password@localhost/testdb')

        # Create a connection
        connection = engine.connect()

        print("Successfully connected to MySQL database: testdb")
        return connection
    except Exception as e:
        print(f"Error connecting to MySQL database: {e}")
        raise


def run_non_polling_benchmark(n_threads: int):

    """
    Runs a benchmark where each thread creates a new MySQL database connection and executes a SLEEP query.

    Args:
        n_threads (int): Number of threads to create for the benchmark.
    """
    print(f"Starting non-polling benchmark with {n_threads} threads")
    start_time = time.time()
    
    def worker():
        try:
            # Create a new connection for each thread
            conn = create_mysql_connection()

            # Execute SLEEP query using MySQL's built-in SLEEP function
            conn.execute(sa.text("SELECT SLEEP(0.01);"))

            # Close the connection
            conn.close()
            print("Thread completed successfully")
        except Exception as e:
            print(f"Error in worker thread: {e}")
    
    # Create and start threads
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Non-polling benchmark completed in {duration:.2f} seconds")
    return duration

def run_polling_benchmark(n_threads: int):
    print(f"Starting polling benchmark with {n_threads} threads")
    start_time = time.time()

    def worker():
        try:
            # Get a connection from the pool (blocking if none available)
            conn = connection_pool.get()
            
            # Execute SLEEP query using MySQL's built-in SLEEP function
            conn.execute(sa.text("SELECT SLEEP(0.01);"))
            
            # Return the connection to the pool
            connection_pool.put(conn)
            print("Thread completed successfully")
        except Exception as e:
            print(f"Error in worker thread: {e}")
            # Make sure to return connection to pool even on error
            if 'conn' in locals():
                connection_pool.put(conn)
    
    # Create a blocking queue (connection pool) with limited size
    pool_size = min(10, n_threads)  # Limit pool size to reasonable number
    connection_pool = queue.Queue(maxsize=pool_size)
    
    # Pre-populate the connection pool
    print(f"Creating connection pool with {pool_size} connections")
    for _ in range(pool_size):
        try:
            conn = create_mysql_connection()
            connection_pool.put(conn)
        except Exception as e:
            print(f"Error creating connection for pool: {e}")
    
    # Create and start threads
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Clean up connections in the pool
    print("Cleaning up connection pool")
    while not connection_pool.empty():
        try:
            conn = connection_pool.get_nowait()
            conn.close()
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error closing pooled connection: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Polling benchmark completed in {duration:.2f} seconds")
    return duration



def main():
    # run_non_polling_benchmark(1200)
    run_polling_benchmark(4000)
    
if __name__ == "__main__":
    main()