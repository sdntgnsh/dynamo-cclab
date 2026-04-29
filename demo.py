import httpx
import time
import sys
import subprocess

BASE_URL = "http://localhost:8001"
KEY = "student1"

def print_step(step):
    print(f"\n{'='*60}\n{step}\n{'='*60}")

def put_data(key, value, context=None):
    payload = {"value": value}
    if context:
        payload["context"] = context
        
    try:
        response = httpx.put(f"{BASE_URL}/kv/{key}", json=payload)
        print(f"PUT /kv/{key} -> Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"Error: {e}")

def get_data(key):
    try:
        response = httpx.get(f"{BASE_URL}/kv/{key}")
        print(f"GET /kv/{key} -> Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"Error: {e}")

def run_cmd(cmd):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True)

def main():
    print("Waiting for cluster to be ready...")
    time.sleep(2)
    
    print_step("1. Baseline: Standard Write")
    print(f"Writing data to key '{KEY}' via node1 (Coordinator)...")
    put_res = put_data(KEY, {"name": "Siddhant", "grade": "A"})
    context = put_res.get("context")
    
    print_step("2. Baseline: Standard Read")
    get_data(KEY)
    
    print_step("3. The Kill Switch: Simulating Node Failure")
    print("We will find which node holds the data and stop one of them.")
    print("For simplicity, we'll just stop node3 and node4 (one of them is likely a replica).")
    run_cmd("docker stop dynamo-node3-1")
    
    print_step("4. High Availability Proof (W=2)")
    print("We are performing another write while a node is down.")
    print("It should succeed because 2 replicas are still alive to form a quorum.")
    put_res = put_data(KEY, {"name": "Siddhant", "grade": "A+"}, context=context)
    new_context = put_res.get("context") if put_res else context
    
    print_step("5. Read during failure (R=2)")
    get_data(KEY)
    
    print_step("6. Eventual Consistency Proof")
    print("Bringing node3 back online...")
    run_cmd("docker start dynamo-node3-1")
    print("Waiting for node to initialize...")
    time.sleep(3)
    
    print("Performing a read. The coordinator might reach out to node3 which has stale data.")
    print("However, vector clocks will resolve the conflict or return multiple versions.")
    get_data(KEY)
    
    print_step("Demo Complete")

if __name__ == "__main__":
    main()
