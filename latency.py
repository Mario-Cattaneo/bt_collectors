import time
import requests
import statistics
import matplotlib.pyplot as plt

def measure_latency(url, trials=20):
    latencies = []

    for i in range(trials):
        start = time.perf_counter()
        resp = requests.get(url)
        end = time.perf_counter()

        if resp.status_code == 200:
            latency = (end - start) * 1000  # ms
            latencies.append(latency)
            print(f"Trial {i+1}: {latency:.2f} ms")
        else:
            print(f"Trial {i+1}: Request failed with status {resp.status_code}")

    return latencies

def analyze_and_plot(latencies):
    if not latencies:
        print("No successful measurements.")
        return

    mean_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    variance_latency = statistics.variance(latencies) if len(latencies) > 1 else 0

    print("\nLatency Statistics (ms):")
    print(f"  Mean:     {mean_latency:.2f}")
    print(f"  Median:   {median_latency:.2f}")
    print(f"  Variance: {variance_latency:.2f}")

    # Plot histogram
    plt.figure(figsize=(8, 5))
    plt.hist(latencies, bins=10, edgecolor="black")
    plt.title("HTTP Request Latency Distribution")
    plt.xlabel("Latency (ms)")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()

if __name__ == "__main__":
    url = "https://gamma-api.polymarket.com/markets?limit=500&offset=0"
    trials = 20  # number of requests

    latencies = measure_latency(url, trials=trials)
    analyze_and_plot(latencies)
