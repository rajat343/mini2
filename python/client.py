# python/client_async.py
import os
import csv
import asyncio
import grpc
import data_pb2
import data_pb2_grpc

A_ADDRESS = 'localhost:50051'  # adjust if needed

async def send_batch(records, batch_id):
    # Add concurrency limit
    semaphore = asyncio.Semaphore(1000)  # Limit concurrent requests
    
    async def send_one(row):
        async with semaphore:
            try:
                await stub.SendRecord(data_pb2.Record(row_data=','.join(row)))
                return True
            except Exception as e:
                print(f"Error sending row: {e}")
                return False
    
    tasks = [send_one(row) for row in records]
    results = await asyncio.gather(*tasks)
    sent = sum(results)
    print(f"[Batch {batch_id}] Completed: {sent}/{len(records)}")

async def main():
    channel = grpc.aio.insecure_channel(A_ADDRESS)
    global stub  # Make stub accessible to send_batch function
    stub = data_pb2_grpc.DataServiceStub(channel)
    
    filename = "../data_dir/Motor_Vehicle_Collisions.csv"
    if not os.path.exists(filename):
        print(f"CSV not found: {filename}")
        return
    with open(filename, newline='', encoding='utf-8') as f:
        rows = list(csv.reader(f))
    total = len(rows)
    print(f"[Client] Loaded {total} rows.")

    num_batches = 3
    chunk = total // num_batches
    batches = [rows[i*chunk : (i+1)*chunk if i<num_batches-1 else total]
               for i in range(num_batches)]

    await asyncio.gather(*[
        send_batch(batches[i], i) for i in range(num_batches)
    ])

if __name__ == "__main__":
    asyncio.run(main())
