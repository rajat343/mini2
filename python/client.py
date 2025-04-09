import os
import csv
import threading
import time
import grpc
import data_pb2
import data_pb2_grpc

# The channel to Node A
A_ADDRESS = 'localhost:50051'  # adjust if A is remote
BATCH_SIZE = 5119  # Configure batch size here

def send_records(thread_id, records):
    """Send batches of CSV rows to Node A."""
    print(f"[Thread-{thread_id}] Starting. Will send {len(records)} rows to A_ADDRESS={A_ADDRESS}")
    
    # Start timing for this thread
    thread_start_time = time.time()

    channel = grpc.insecure_channel(A_ADDRESS)
    stub = data_pb2_grpc.DataServiceStub(channel)

    sent_count = 0
    batch_count = 0
    
    # Process records in batches
    for i in range(0, len(records), BATCH_SIZE):
        batch_start_time = time.time()
        
        # Create a batch of records
        batch = records[i:i+BATCH_SIZE]
        batch_proto = data_pb2.RecordBatch()
        
        for row in batch:
            record = batch_proto.records.add()
            record.row_data = ','.join(row)
        
        try:
            # Send the batch
            stub.SendRecordBatch(batch_proto)
            sent_count += len(batch)
            batch_count += 1
            
            batch_time = time.time() - batch_start_time
            if batch_count % 10 == 0:  # Log every 10 batches
                print(f"[Thread-{thread_id}] Batch {batch_count} with {len(batch)} records sent in {batch_time:.6f} seconds")
                
        except grpc.RpcError as e:
            print(f"[Thread-{thread_id}] gRPC Error: {e.code()} - {e.details()}")
            break

    thread_time = time.time() - thread_start_time
    print(f"[Thread-{thread_id}] Done sending {sent_count} rows in {batch_count} batches in {thread_time:.2f} seconds.")
    print(f"[Thread-{thread_id}] Average time per batch: {thread_time/batch_count if batch_count else 0:.6f} seconds")
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
    print(f"[Client] BATCH SIZE used: {BATCH_SIZE}")
    print("[Client] All threads finished sending data.")

if __name__ == "__main__":
    main()