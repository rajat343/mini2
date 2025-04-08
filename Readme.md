# Mini 2 Project – How to Run

Below are **step-by-step** instructions on installing dependencies and running this multi-process system (C++ servers + Python client).

## 1. Install Dependencies

1. Homebrew (macOS only):
    ```
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```
2. Protobuf & gRPC:
    ```
    brew update
    brew install protobuf grpc
    ```
3. CMake & a C++14 compiler:
    - For example: `brew install cmake`
    - AppleClang on macOS M1/M2 is fine if it supports C++14 or newer.
4. Python 3 (for the client).

## 2. Build the C++ Servers

1. **Clone** or enter this project’s root folder:
    ```
    cd mini2
    ```
2. **(Optional) Generate gRPC stubs** (if not already):
    ```
    protoc -I=proto \
      --grpc_out=proto --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
      --cpp_out=proto proto/data.proto
    ```
3. **Create a build folder & run CMake**:
    ```
    mkdir build
    cd build
    cmake ..
    ```
4. **Compile**:
    ```
    make
    ```
5. You should now see executables like `serverA`, `serverB`, etc. in `build/src/nodeA/`, `build/src/nodeB/`, and so on.

## 3. Python Client Setup

1. From the project root, go to `python/`:
    ```
    cd ../python
    ```
2. Create and activate a virtual environment:
    ```
    python3 -m venv .venv
    source .venv/bin/activate
    ```
3. Install Python dependencies:
    ```
    pip install --upgrade pip
    pip install grpcio grpcio-tools protobuf
    ```
4. (Optional) Re-generate Python stubs if needed:
    ```
    python -m grpc_tools.protoc -I ../proto \
      --python_out=. --grpc_python_out=. \
      ../proto/data.proto
    ```

## 4. Run Each Node

-   **Node A** (receives CSV rows from Python client, writes to shared memory)
-   **Node B** (reads from shared memory, splits data, sends to C & D)
-   **Node C**, **Node D**, **Node E** (receive data via gRPC; D can forward partial data to E)

Example commands (from the project root):

```
./build/src/nodeA/serverA config/overlay_config.json A

./build/src/nodeB/serverB config/overlay_config.json B

./build/src/nodeC/serverC config/overlay_config.json C

./build/src/nodeD/serverD config/overlay_config.json D

./build/src/nodeE/serverE config/overlay_config.json E
```

If using two machines, run A/B on one (for shared memory) and C/D/E on the other. Adjust IPs/ports in `overlay_config.json` as needed.

## 5. Run the Python Client

1. Make sure your virtual environment is active:
    ```
    cd python
    source .venv/bin/activate
    ```
2. Run the client:
    ```
    python client.py
    ```
3. The client reads `data_dir/Motor_Vehicle_Collisions.csv`, spawns threads, and sends rows to **Node A**. You’ll see logs in each C++ node about how data is distributed.

## 6. Done!

You now have:

-   A tree-based overlay (A→B, B→C, B→D→E).
-   Shared memory for A→B on the same machine.
-   gRPC for cross-machine edges (B→C, B→D).
-   gRPC for same-machine interaction (D→E).
-   A Python client concurrently sending CSV data.
