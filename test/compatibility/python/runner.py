import subprocess
import sys


def main():
    print("=" * 60)
    print("  Running PocketQueue Python SDK Compatibility Test Suite")
    print("=" * 60)

    print("\n--- 1. Running Redis Streams Compatibility (redis-py) ---")
    ret = subprocess.call([sys.executable, "test/compatibility/python/test_redis.py"])
    if ret != 0:
        print("\n[FAIL] redis-py compatibility tests failed!")
        sys.exit(ret)

    print("\n--- 2. Running Kafka Wire Protocol Compatibility ---")
    ret = subprocess.call([sys.executable, "test/compatibility/python/test_kafka.py"])
    if ret != 0:
        print("\n[FAIL] Kafka wire protocol compatibility tests failed!")
        sys.exit(ret)

    print("\n" + "=" * 60)
    print("  All Python Compatibility Suites Passed Successfully!  ")
    print("=" * 60)


if __name__ == "__main__":
    main()
