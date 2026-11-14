import os
import socket
import struct
import subprocess
import time


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def send_kafka_request(sock, api_key, api_version, correlation_id, client_id, body_bytes):
    # Header: api_key (int16), api_version (int16), correlation_id (int32), client_id (string)
    cid_bytes = client_id.encode("utf-8")
    header = struct.pack(">hhih", api_key, api_version, correlation_id, len(cid_bytes)) + cid_bytes
    payload = header + body_bytes
    frame = struct.pack(">i", len(payload)) + payload
    sock.sendall(frame)

    # Read response
    resp_len_bytes = sock.recv(4)
    if len(resp_len_bytes) < 4:
        raise RuntimeError("Failed to read response length")
    resp_len = struct.unpack(">i", resp_len_bytes)[0]
    data = b""
    while len(data) < resp_len:
        chunk = sock.recv(resp_len - len(data))
        if not chunk:
            break
        data += chunk
    return data


def run_tests():
    kafka_port = find_free_port()
    redis_port = find_free_port()

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    bin_name = "pocketqueue_kafka_compat.exe" if os.name == "nt" else "pocketqueue_kafka_compat"
    bin_path = os.path.join(root_dir, bin_name)

    print(f"[*] Building test binary at {bin_path}...")
    subprocess.check_call(["go", "build", "-o", bin_path, "./cmd/pocketqueue"], cwd=root_dir)

    print(f"[*] Starting PocketQueue on Kafka port {kafka_port}, Redis port {redis_port}...")
    proc = subprocess.Popen(
        [
            bin_path,
            "--headless",
            "--host",
            "127.0.0.1",
            "--kafka-port",
            str(kafka_port),
            "--redis-port",
            str(redis_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for Kafka port ready
        connected = False
        sock = None
        for _ in range(30):
            try:
                sock = socket.create_connection(("127.0.0.1", kafka_port), timeout=1)
                connected = True
                break
            except Exception:
                time.sleep(0.1)

        if not connected or sock is None:
            raise RuntimeError("Failed to connect to Kafka port")

        print("[*] 1. Testing ApiVersions (API 18)...")
        # ApiVersions body: empty for v0
        res = send_kafka_request(sock, 18, 0, 101, "python-test", b"")
        corr_id, error_code, num_keys = struct.unpack(">ihi", res[:10])
        assert corr_id == 101, f"Expected correlation 101, got {corr_id}"
        assert error_code == 0, f"Expected error_code 0, got {error_code}"
        assert num_keys > 0, "Expected non-zero api keys"
        print(f"    -> PASS (ApiKeys advertised: {num_keys})")

        print("[*] 2. Testing CreateTopics (API 19)...")
        # CreateTopics v0 body:
        # array_len (int32 = 1)
        # topic_name (int16 len + str)
        # num_partitions (int32 = 2)
        # replication_factor (int16 = 1)
        # replica_assignments array (int32 = 0)
        # configs array (int32 = 0)
        # timeout_ms (int32 = 5000)
        topic = "python-orders"
        topic_b = topic.encode("utf-8")
        body = (
            struct.pack(">i", 1)
            + struct.pack(">h", len(topic_b))
            + topic_b
            + struct.pack(">ihiii", 2, 1, 0, 0, 5000)
        )
        res = send_kafka_request(sock, 19, 0, 102, "python-test", body)
        corr_id, arr_len = struct.unpack(">ii", res[:8])
        assert corr_id == 102
        assert arr_len == 1
        t_len = struct.unpack(">h", res[8:10])[0]
        t_name = res[10 : 10 + t_len].decode("utf-8")
        err = struct.unpack(">h", res[10 + t_len : 12 + t_len])[0]
        assert t_name == topic
        assert err == 0, f"Expected err 0 from CreateTopics, got {err}"
        print(f"    -> PASS (Created topic '{topic}' with 2 partitions)")

        print("[*] 3. Testing Metadata (API 3)...")
        # Metadata v0 body: topics array (int32 = 1), topic_name
        body = struct.pack(">ih", 1, len(topic_b)) + topic_b
        res = send_kafka_request(sock, 3, 0, 103, "python-test", body)
        corr_id = struct.unpack(">i", res[:4])[0]
        assert corr_id == 103
        print("    -> PASS")

        print("[*] 4. Testing DeleteTopics (API 20)...")
        # DeleteTopics v0 body: topics array (int32 = 1), topic_name (int16 len + str), timeout_ms (int32)
        body = struct.pack(">ih", 1, len(topic_b)) + topic_b + struct.pack(">i", 5000)
        res = send_kafka_request(sock, 20, 0, 104, "python-test", body)
        corr_id, arr_len = struct.unpack(">ii", res[:8])
        assert corr_id == 104
        assert arr_len == 1
        t_len = struct.unpack(">h", res[8:10])[0]
        t_name = res[10 : 10 + t_len].decode("utf-8")
        err = struct.unpack(">h", res[10 + t_len : 12 + t_len])[0]
        assert t_name == topic
        assert err == 0, f"Expected err 0 from DeleteTopics, got {err}"
        print(f"    -> PASS (Deleted topic '{topic}')")

        sock.close()
        print("\n[SUCCESS] All Python Kafka wire protocol compatibility tests passed!")

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
