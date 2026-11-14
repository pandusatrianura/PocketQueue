import os
import socket
import subprocess
import sys
import time

try:
    import redis
except ImportError:
    print("Error: redis package is required. Install via 'pip install redis'")
    sys.exit(1)


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_tests():
    redis_port = find_free_port()
    kafka_port = find_free_port()

    # Find or build pocketqueue binary
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    bin_name = "pocketqueue_test_compat.exe" if os.name == "nt" else "pocketqueue_test_compat"
    bin_path = os.path.join(root_dir, bin_name)

    print(f"[*] Building test binary at {bin_path}...")
    build_cmd = ["go", "build", "-o", bin_path, "./cmd/pocketqueue"]
    subprocess.check_call(build_cmd, cwd=root_dir)

    print(f"[*] Starting PocketQueue on Redis port {redis_port}, Kafka port {kafka_port}...")
    proc = subprocess.Popen(
        [
            bin_path,
            "--headless",
            "--host",
            "127.0.0.1",
            "--redis-port",
            str(redis_port),
            "--kafka-port",
            str(kafka_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for server ready
        connected = False
        client = redis.Redis(host="127.0.0.1", port=redis_port, decode_responses=True, protocol=2)
        for _ in range(30):
            try:
                if client.ping():
                    connected = True
                    break
            except Exception:
                time.sleep(0.1)
        if not connected:
            poll = proc.poll()
            out, err = proc.communicate(timeout=1)
            raise RuntimeError(f"Server failed to start (exit code {poll}): {err.decode('utf-8', errors='ignore')}")

        print("[*] 1. Testing PING...")
        assert client.ping() is True, "PING failed"
        print("    -> PASS")

        print("[*] 2. Testing XADD...")
        e1_id = client.xadd("orders", {"order_id": "1001", "amount": "99.50"})
        assert e1_id == "0-0", f"Unexpected entry ID {e1_id}"
        print(f"    -> PASS (ID: {e1_id})")

        print("[*] 3. Testing XLEN...")
        length = client.xlen("orders")
        assert length == 1, f"Expected length 1, got {length}"
        print(f"    -> PASS (Length: {length})")

        print("[*] 4. Testing XRANGE...")
        entries = client.xrange("orders", min="-", max="+")
        assert len(entries) == 1, f"Expected 1 entry, got {len(entries)}"
        assert entries[0][0] == "0-0"
        print(f"    -> PASS (Entries: {entries})")

        print("[*] 5. Testing XGROUP CREATE...")
        grp_ok = client.xgroup_create("orders", "workers", id="$", mkstream=True)
        assert grp_ok is True, "XGROUP CREATE failed"
        print("    -> PASS")

        print("[*] 6. Testing XADD second entry for consumer group...")
        e2_id = client.xadd("orders", {"order_id": "1002", "amount": "150.00"})
        assert e2_id == "1-0", f"Unexpected entry ID {e2_id}"
        print(f"    -> PASS (ID: {e2_id})")

        print("[*] 7. Testing XREADGROUP...")
        read_res = client.xreadgroup("workers", "consumer-1", {"orders": ">"}, count=10)
        assert len(read_res) == 1, f"Expected 1 stream, got {len(read_res)}"
        stream_name, stream_entries = read_res[0]
        assert stream_name == "orders"
        assert len(stream_entries) == 1
        assert stream_entries[0][0] == "1-0"
        print(f"    -> PASS (ReadGroup: {read_res})")

        print("[*] 8. Testing XACK...")
        acked = client.xack("orders", "workers", "1-0")
        assert acked == 1, f"Expected 1 acked, got {acked}"
        print(f"    -> PASS (Acked: {acked})")

        print("[*] 9. Testing XREVRANGE...")
        rev_entries = client.xrevrange("orders", max="+", min="-", count=1)
        assert len(rev_entries) == 1
        assert rev_entries[0][0] == "1-0"
        print(f"    -> PASS (RevRange: {rev_entries})")

        print("[*] 10. Testing XDEL...")
        deleted = client.xdel("orders", "0-0")
        assert deleted == 1, f"Expected 1 deleted, got {deleted}"
        print(f"    -> PASS (Deleted: {deleted})")

        print("\n[SUCCESS] All Python redis-py compatibility tests passed!")

    finally:
        print("[*] Cleaning up test server and binary...")
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        if os.path.exists(bin_path):
            try:
                os.remove(bin_path)
            except Exception:
                pass


if __name__ == "__main__":
    run_tests()
