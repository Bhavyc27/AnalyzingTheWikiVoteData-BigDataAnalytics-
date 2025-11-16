# producer_fault_tolerant.py
from kafka import KafkaProducer
import time, os, sys, json

TOPIC = "wiki-vote"
BOOTSTRAP_SERVERS = "localhost:9092"

# 🗂️ Update this path if your dataset is elsewhere
DATA_PATH = "/Users/bhavychawla/Downloads/Wiki-Vote.txt"
CHECKPOINT_FILE = os.path.abspath("producer_checkpoint.json")

# ⚙️ Tune this for speed vs stability
DELAY = 0.05  # seconds between sends

# ---------------------------------------------------------------------
def create_producer():
    """Create a resilient Kafka producer with retries and acks."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            retries=15,
            linger_ms=5,
            max_in_flight_requests_per_connection=1,
        )
        print(f"[✅] Kafka Producer connected → Topic: {TOPIC}")
        return producer
    except Exception as e:
        print(f"[❌] Kafka connection failed: {e}")
        sys.exit(1)


def load_checkpoint():
    """Load saved progress (line number)."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_line", 0)
    return 0


def save_checkpoint(line_num):
    """Save current progress to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_line": line_num}, f)
    # flush to disk for safety
    os.sync() if hasattr(os, "sync") else None


def reset_checkpoint():
    """Erase checkpoint when stream fully completes."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            time.sleep(0.2)  # allow FS to sync
            print(f"[🧹] Stream complete — checkpoint reset.")
        else:
            print("[ℹ️] No checkpoint found (already clean).")
    except Exception as e:
        print(f"[⚠️] Could not reset checkpoint: {e}")


def stream_data(producer):
    """Main streaming logic."""
    if not os.path.exists(DATA_PATH):
        print(f"[❌] File not found: {DATA_PATH}")
        sys.exit(1)

    start_line = load_checkpoint()
    count = 0
    print(f"[♻️] Resuming from line {start_line}...")

    try:
        with open(DATA_PATH, "r") as file:
            for idx, line in enumerate(file):
                # skip header/comment lines and already processed ones
                if idx < start_line or line.startswith("#") or not line.strip():
                    continue

                # send each edge to Kafka
                src, tgt = line.strip().split()
                producer.send(TOPIC, f"{src},{tgt}".encode("utf-8"))
                count += 1

                # periodic progress + checkpoint
                if count % 5000 == 0:
                    save_checkpoint(idx)
                    print(f"[STREAM] Sent {count} edges... (line {idx})")

                time.sleep(DELAY)

        producer.flush()
        print(f"\n[✅] Streaming complete — Total edges sent: {count}")
        reset_checkpoint()

    except KeyboardInterrupt:
        print("\n[⚠️] Producer interrupted manually, saving progress...")
        save_checkpoint(idx)
    finally:
        producer.close()


# ---------------------------------------------------------------------
if __name__ == "__main__":
    producer = create_producer()
    stream_data(producer)
