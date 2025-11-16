# consumer_fault_tolerant_partial.py
from kafka import KafkaConsumer
import networkx as nx
import time, random, os, pickle

TOPIC = "wiki-vote"
BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_FILE = "graph_checkpoint.pkl"
PRODUCER_CHECKPOINT = "producer_checkpoint.json"

GROUND_TRUTH = {
    "nodes": 7115,
    "edges": 103689,
    "largest_wcc_nodes": 7066,
    "largest_wcc_edges": 103663,
    "largest_scc_nodes": 1300,
    "largest_scc_edges": 39456,
    "clustering_coeff": 0.1409,
    "triangles": 608389,
    "closed_triangle_fraction": 0.04564,
    "diameter": 7,
    "eff_diameter": 3.8,
}

def load_graph_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "rb") as f:
            G = pickle.load(f)
        print(f"[♻️] Loaded checkpoint graph with {G.number_of_edges()} edges.")
        return G
    else:
        print("[ℹ️] No checkpoint found, starting fresh graph.")
        return nx.DiGraph()

def save_graph_checkpoint(G):
    with open(CHECKPOINT_FILE, "wb") as f:
        pickle.dump(G, f)

def approx_effective_diameter(G, k=300, percentile=0.9):
    if G.number_of_nodes() == 0:
        return 0
    nodes = list(G.nodes())
    samples = random.sample(nodes, min(k, len(nodes)))
    distances = []
    for s in samples:
        lengths = nx.single_source_shortest_path_length(G, s)
        distances.extend(lengths.values())
    distances.sort()
    if not distances:
        return 0
    idx = int(percentile * len(distances))
    return distances[idx]

def partial_metrics(G, edges):
    nodes = G.number_of_nodes()
    GU = G.to_undirected()

    print(f"\n[⏱ PARTIAL SNAPSHOT @ {edges} edges]")
    print(f"Nodes: {nodes}")

    try:
        largest_wcc = max(nx.weakly_connected_components(G), key=len)
        largest_scc = max(nx.strongly_connected_components(G), key=len)
        lwcc_n, lscc_n = len(largest_wcc), len(largest_scc)
        lwcc_e = sum(1 for u, v in G.edges() if u in largest_wcc and v in largest_wcc)
        lscc_e = sum(1 for u, v in G.edges() if u in largest_scc and v in largest_scc)
    except:
        lwcc_n, lwcc_e, lscc_n, lscc_e = 0, 0, 0, 0

    sample_nodes = random.sample(list(G.nodes()), min(600, nodes))
    subG = G.subgraph(sample_nodes).to_undirected()

    try:
        clustering = nx.average_clustering(subG)
        tri_map = nx.triangles(subG)
        tri_count = sum(tri_map.values()) // 3
        tri_frac = nx.transitivity(subG)
    except:
        clustering, tri_count, tri_frac = 0, 0, 0

    try:
        sample_nodes = random.sample(list(GU.nodes()), min(100, len(GU)))
        diam = 0
        for s in sample_nodes:
            lengths = nx.single_source_shortest_path_length(GU, s)
            diam = max(diam, max(lengths.values(), default=0))
        eff_diam = approx_effective_diameter(G)
    except:
        diam, eff_diam = 0, 0

    print(f"Largest WCC: {lwcc_n} nodes, {lwcc_e} edges")
    print(f"Largest SCC: {lscc_n} nodes, {lscc_e} edges")
    print(f"Clustering coeff: {clustering:.4f}")
    print(f"Triangles: {tri_count} | Closed triangle fraction: {tri_frac:.5f}")
    print(f"Diameter (sampled): {diam} | Effective diameter (approx): {eff_diam:.2f}")

def final_metrics(G):
    GU = G.to_undirected()
    result = {}

    result["nodes"] = G.number_of_nodes()
    result["edges"] = G.number_of_edges()

    try:
        wcc = max(nx.weakly_connected_components(G), key=len)
        result["largest_wcc_nodes"] = len(wcc)
        result["largest_wcc_edges"] = sum(1 for u, v in G.edges() if u in wcc and v in wcc)
    except:
        result["largest_wcc_nodes"] = result["largest_wcc_edges"] = 0

    try:
        scc = max(nx.strongly_connected_components(G), key=len)
        result["largest_scc_nodes"] = len(scc)
        result["largest_scc_edges"] = sum(1 for u, v in G.edges() if u in scc and v in scc)
    except:
        result["largest_scc_nodes"] = result["largest_scc_edges"] = 0

    try:
        result["clustering_coeff"] = nx.average_clustering(GU)
        tri_map = nx.triangles(GU)
        result["triangles"] = sum(tri_map.values()) // 3
        result["closed_triangle_fraction"] = nx.transitivity(GU)
    except:
        result["clustering_coeff"] = result["triangles"] = result["closed_triangle_fraction"] = 0

    try:
        sample_nodes = random.sample(list(GU.nodes()), min(400, len(GU)))
        diam = 0
        for s in sample_nodes:
            lengths = nx.single_source_shortest_path_length(GU, s)
            diam = max(diam, max(lengths.values(), default=0))
        result["diameter"] = diam
    except:
        result["diameter"] = 0

    try:
        result["eff_diameter"] = approx_effective_diameter(G)
    except:
        result["eff_diameter"] = 0

    return result

def main():
    print(f"[INFO] Connecting to Kafka topic: {TOPIC}...")

    # 🧹 Auto-reset if producer has restarted (no producer checkpoint found)
    if not os.path.exists(PRODUCER_CHECKPOINT) and os.path.exists(CHECKPOINT_FILE):
        print("[🧹] Producer restarted — clearing old consumer checkpoint for fresh start.")
        os.remove(CHECKPOINT_FILE)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="wiki-consumer-group",
        consumer_timeout_ms=120000
    )
    print("[✅] Consumer connected, waiting for data...")

    G = load_graph_checkpoint()
    start = time.time()
    last_checkpoint = time.time()
    last_snapshot = start

    try:
        for msg in consumer:
            src, tgt = msg.value.decode("utf-8").split(",")
            G.add_edge(src, tgt)
            edges = G.number_of_edges()

            if edges % 10000 == 0:
                now = time.time()
                elapsed_total = now - start
                elapsed_since_last = now - last_snapshot
                throughput_recent = 10000 / elapsed_since_last if elapsed_since_last > 0 else 0
                throughput_avg = edges / elapsed_total if elapsed_total > 0 else 0
                remaining_edges = max(0, GROUND_TRUTH["edges"] - edges)
                eta = remaining_edges / throughput_avg if throughput_avg > 0 else float("inf")

                print(f"\n[📈] Edges processed: {edges}/{GROUND_TRUTH['edges']} | Nodes: {G.number_of_nodes()}")
                print(f"[⚡ Throughput: {throughput_recent:.2f} edges/sec (recent), {throughput_avg:.2f} avg | ⏳ ETA: {eta:.1f} sec]")

                partial_metrics(G, edges)
                last_snapshot = now

            if time.time() - last_checkpoint > 30:
                save_graph_checkpoint(G)
                print(f"[💾] Graph checkpoint saved ({edges} edges).")
                last_checkpoint = time.time()

            if edges >= GROUND_TRUTH["edges"]:
                print("\n[✅] Full stream received, computing final metrics...")
                break

    except KeyboardInterrupt:
        print("\n[⚠️] Interrupted manually. Saving checkpoint...")
        save_graph_checkpoint(G)
    finally:
        save_graph_checkpoint(G)
        FM = final_metrics(G)
        print(f"\n[🏁 Completed in {time.time() - start:.2f} seconds total]")
        print("\n=========== FINAL METRICS ===========")
        for k, v in FM.items():
            gt = GROUND_TRUTH.get(k, "-")
            print(f"{k:28s}: {v}  (GT={gt})")
        print("=====================================")
        consumer.close()

if __name__ == "__main__":
    main()
