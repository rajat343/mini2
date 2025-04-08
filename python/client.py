import os
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
    print(f"[Thread-{thread_id}] Starting. Will send {len(records)} rows to A_ADDRESS={A_ADDRESS}")

    channel = grpc.insecure_channel(A_ADDRESS)
    stub = data_pb2_grpc.DataServiceStub(channel)

    sent_count = 0
    for row in records:
        # Each row is a list of columns, join them or parse as needed
        line = ','.join(row)
        request = data_pb2.Record(row_data=line)
        try:
            stub.SendRecord(request)  # one-way push
            sent_count += 1
        except grpc.RpcError as e:
            # LOG: any exception details
            print(f"[Thread-{thread_id}] gRPC Error: {e.code()} - {e.details()}")
            break  # or continue, depending on how you want to handle it

    print(f"[Thread-{thread_id}] Done sending {sent_count} rows (out of {len(records)}).")

def main():
    # LOG: show the CSV path
    filename = "../data_dir/Motor_Vehicle_Collisions.csv"
    csv_path = os.path.abspath(filename)
    print(f"[Client] Reading CSV from: {csv_path}")

    all_rows = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # next(reader, None) # if you want to skip header
            for r in reader:
                all_rows.append(r)
        print(f"[Client] CSV loaded. Total rows = {len(all_rows)}")
    except FileNotFoundError:
        print(f"[Client] ERROR: CSV file not found at {csv_path}")
        return

    # Decide how to split among threads
    num_threads = 3
    chunk_size = len(all_rows) // num_threads
    threads = []

    print(f"[Client] Creating {num_threads} threads, chunk_size={chunk_size}")

    for i in range(num_threads):
        start_index = i * chunk_size
        end_index = (i+1)*chunk_size if i < num_threads - 1 else len(all_rows)
        thread_data = all_rows[start_index:end_index]

        t = threading.Thread(target=send_records, args=(i, thread_data))
        threads.append(t)
        t.start()
        print(f"[Client] Started Thread-{i} with {len(thread_data)} rows")

    for t in threads:
        t.join()

    print("[Client] All threads finished sending data.")

if __name__ == "__main__":
    main()
