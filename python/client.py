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
    
    # Start timing for this thread
    thread_start_time = time.time()

    channel = grpc.insecure_channel(A_ADDRESS)
    stub = data_pb2_grpc.DataServiceStub(channel)

    sent_count = 0
    for idx, row in enumerate(records):
        # Track time per record
        record_start_time = time.time()
        
        # Each row is a list of columns, join them or parse as needed
        line = ','.join(row)
        request = data_pb2.Record(row_data=line)
        try:
            stub.SendRecord(request)  # one-way push
            sent_count += 1
            
            # Log timing for some records (not all to avoid flooding logs)
            if idx % 100 == 0:
                record_time = time.time() - record_start_time
                print(f"[Thread-{thread_id}] Record {idx} sent in {record_time:.6f} seconds")
                
        except grpc.RpcError as e:
            # LOG: any exception details
            print(f"[Thread-{thread_id}] gRPC Error: {e.code()} - {e.details()}")
            break  # or continue, depending on how you want to handle it

    thread_time = time.time() - thread_start_time
    print(f"[Thread-{thread_id}] Done sending {sent_count} rows (out of {len(records)}) in {thread_time:.2f} seconds.")
    return thread_time, sent_count

def main():
    # Start overall timing
    total_start_time = time.time()
    
    # LOG: show the CSV path
    filename = "../data_dir/Motor_Vehicle_Collisions.csv"
    csv_path = os.path.abspath(filename)
    print(f"[Client] Reading CSV from: {csv_path}")

    # Track CSV loading time
    csv_load_start = time.time()
    all_rows = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # next(reader, None) # if you want to skip header
            for r in reader:
                all_rows.append(r)
        csv_load_time = time.time() - csv_load_start
        print(f"[Client] CSV loaded in {csv_load_time:.2f} seconds. Total rows = {len(all_rows)}")
    except FileNotFoundError:
        print(f"[Client] ERROR: CSV file not found at {csv_path}")
        return

    # Decide how to split among threads
    num_threads = 3
    chunk_size = len(all_rows) // num_threads
    threads = []
    thread_objects = []

    
    # Send "__START__" signal to Node E before starting
    try:
        channel_e = grpc.insecure_channel("localhost:50055")
        stub_e = data_pb2_grpc.DataServiceStub(channel_e)
        stub_e.SendRecord(data_pb2.Record(row_data="__START__"))
        print("[Client] Sent __START__ signal to Node E.")
    except Exception as e:
        print(f"[Client] Failed to send __START__ signal: {e}")

    print(f"[Client] Creating {num_threads} threads, chunk_size={chunk_size}")

    # Track thread results
    thread_results = []
    
    for i in range(num_threads):
        start_index = i * chunk_size
        end_index = (i+1)*chunk_size if i < num_threads - 1 else len(all_rows)
        thread_data = all_rows[start_index:end_index]

        t = threading.Thread(target=lambda i=i, data=thread_data: 
                            thread_results.append(send_records(i, data)))
        threads.append(t)
        thread_objects.append(t)
        t.start()
        print(f"[Client] Started Thread-{i} with {len(thread_data)} rows")

    for t in thread_objects:
        t.join()

    # Calculate total stats
    total_time = time.time() - total_start_time
    total_sent = sum(result[1] for result in thread_results)
    avg_thread_time = sum(result[0] for result in thread_results) / len(thread_results)
    
    print("\n[Client] TIMING SUMMARY:")
    print(f"[Client] Total execution time: {total_time:.2f} seconds")
    print(f"[Client] CSV loading time: {csv_load_time:.2f} seconds")
    print(f"[Client] Average thread processing time: {avg_thread_time:.2f} seconds")
    print(f"[Client] Records sent: {total_sent}")
    print(f"[Client] Average throughput: {total_sent/total_time:.2f} records/second")
    print("[Client] All threads finished sending data.")

if __name__ == "__main__":
    main()