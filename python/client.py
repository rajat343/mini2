import csv
import threading
import time
import grpc
import data_pb2
import data_pb2_grpc

# The channel to Node A
A_ADDRESS = 'localhost:50051'  # adjust if A is remote

def send_records(thread_id, records):
    """Send a batch of CSV rows to Node A."""
    channel = grpc.insecure_channel(A_ADDRESS)
    stub = data_pb2_grpc.DataServiceStub(channel)

    for row in records:
        # Each row is a list of columns, join them or parse as needed
        line = ','.join(row)
        request = data_pb2.Record(row_data=line)
        stub.SendRecord(request)  # one-way push
    print(f"[Thread-{thread_id}] Done sending {len(records)} rows.")

def main():
    # Read the CSV
    filename = "../data_dir/Motor_Vehicle_Collisions.csv"
    all_rows = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # If there's a header, optionally skip it
        # next(reader, None)
        for r in reader:
            all_rows.append(r)

    # Decide how to split among threads
    num_threads = 3
    chunk_size = len(all_rows) // num_threads
    threads = []

    for i in range(num_threads):
        start_index = i * chunk_size
        # last thread takes the remainder
        end_index = (i+1)*chunk_size if i < num_threads - 1 else len(all_rows)
        thread_data = all_rows[start_index:end_index]

        t = threading.Thread(target=send_records, args=(i, thread_data))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("All threads finished sending data.")

if __name__ == "__main__":
    main()
