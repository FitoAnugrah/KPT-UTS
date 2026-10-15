import os
import random
import re
import math
import string
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from queue import Queue
from threading import Thread


# =========================================================
# KONFIGURASI PRIBADI
# =========================================================

NIM = "247006111156"
RANDOM_SEED = 247006111156

THREADS = 2
PROCESSES = 4
TOTAL_FILES = 1560

DATASET_DIR = "dataset"
QUEUE_SIZE = 64


# =========================================================
# DATASET GENERATOR
# =========================================================

WORDS = [
    "parallel", "computing", "distributed", "system",
    "algorithm", "process", "thread", "database",
    "network", "server", "python", "data",
    "performance", "optimization", "software",
    "hardware", "security", "programming"
]


def generate_dataset():
    os.makedirs(DATASET_DIR, exist_ok=True)

    rng = random.Random(RANDOM_SEED)

    existing = [
        f for f in os.listdir(DATASET_DIR)
        if f.endswith(".txt")
    ]

    if len(existing) == TOTAL_FILES:
        print("[DATASET] Dataset sudah tersedia.")
        return

    # Bersihkan dataset lama
    for filename in existing:
        os.remove(os.path.join(DATASET_DIR, filename))

    print(f"[DATASET] Membuat {TOTAL_FILES} file teks...")

    for i in range(TOTAL_FILES):
        paragraph_count = rng.randint(5, 10)
        paragraphs = []

        for _ in range(paragraph_count):
            word_count = rng.randint(80, 150)

            words = [
                rng.choice(WORDS)
                for _ in range(word_count)
            ]

            # Tambahkan angka dan simbol
            numbers = str(rng.randint(1000, 999999))
            symbols = "".join(
                rng.choice("!@#$%^&*")
                for _ in range(5)
            )

            text = (
                " ".join(words)
                + " "
                + numbers
                + " "
                + symbols
            )

            paragraphs.append(text)

        content = "\n\n".join(paragraphs)

        filepath = os.path.join(
            DATASET_DIR,
            f"file_{i + 1:04d}.txt"
        )

        with open(filepath, "w", encoding="utf-8") as file:
            file.write(content)

    print("[DATASET] Dataset selesai dibuat.")


# =========================================================
# CPU-BOUND ANALYSIS
# =========================================================

def cpu_analyze_text(item):
    filepath, text = item

    start = time.perf_counter()

    total_characters = len(text)

    # Huruf
    letters = [c for c in text if c.isalpha()]

    # Vokal
    vowels = sum(
        1 for c in text.lower()
        if c in "aiueo"
    )

    # Konsonan
    consonants = sum(
        1 for c in text.lower()
        if c.isalpha() and c not in "aiueo"
    )

    # Digit
    digits = sum(
        1 for c in text
        if c.isdigit()
    )

    # Simbol
    symbols = sum(
        1 for c in text
        if not c.isalnum() and not c.isspace()
    )

    # Kata
    words = re.findall(r"\b\w+\b", text.lower())

    total_words = len(words)
    unique_words = len(set(words))

    # Shannon Entropy
    frequency = Counter(text)

    entropy = 0.0
    text_length = len(text)

    if text_length > 0:
        for count in frequency.values():
            probability = count / text_length
            entropy -= probability * math.log2(probability)

    cpu_time = time.perf_counter() - start

    return {
        "file": filepath,
        "characters": total_characters,
        "words": total_words,
        "unique_words": unique_words,
        "vowels": vowels,
        "consonants": consonants,
        "digits": digits,
        "symbols": symbols,
        "entropy": entropy,
        "cpu_time": cpu_time
    }


# =========================================================
# THREAD LOADER
# =========================================================

def thread_loader(filepaths, data_queue):
    for filepath in filepaths:
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                content = file.read()

            data_queue.put((filepath, content))

        except Exception as error:
            print(f"[ERROR] {filepath}: {error}")

    # Sentinel
    data_queue.put(None)


# =========================================================
# SEQUENTIAL BASELINE
# =========================================================

def run_sequential(filepaths):
    start = time.perf_counter()

    results = []

    for filepath in filepaths:
        with open(filepath, "r", encoding="utf-8") as file:
            content = file.read()

        results.append(
            cpu_analyze_text((filepath, content))
        )

    elapsed = time.perf_counter() - start

    return elapsed, results


# =========================================================
# HYBRID PIPELINE
# =========================================================

def run_hybrid(filepaths):
    start = time.perf_counter()

    data_queue = Queue(maxsize=QUEUE_SIZE)

    # Pembagian file ke 2 thread
    chunks = [
        filepaths[i::THREADS]
        for i in range(THREADS)
    ]

    threads = []

    for chunk in chunks:
        thread = Thread(
            target=thread_loader,
            args=(chunk, data_queue)
        )

        thread.start()
        threads.append(thread)

    results = []
    futures = []

    with ProcessPoolExecutor(
        max_workers=PROCESSES
    ) as process_pool:

        finished_threads = 0

        while finished_threads < THREADS:

            item = data_queue.get()

            if item is None:
                finished_threads += 1
                continue

            future = process_pool.submit(
                cpu_analyze_text,
                item
            )

            futures.append(future)

        # Tunggu seluruh proses selesai
        for future in as_completed(futures):
            results.append(future.result())

    # Tunggu semua thread selesai
    for thread in threads:
        thread.join()

    elapsed = time.perf_counter() - start

    return elapsed, results


# =========================================================
# AGGREGATION
# =========================================================

def aggregate_results(results):

    summary = {
        "files": len(results),
        "characters": 0,
        "words": 0,
        "unique_words": 0,
        "vowels": 0,
        "consonants": 0,
        "digits": 0,
        "symbols": 0,
        "entropy": 0.0
    }

    for result in results:

        summary["characters"] += result["characters"]
        summary["words"] += result["words"]
        summary["unique_words"] += result["unique_words"]
        summary["vowels"] += result["vowels"]
        summary["consonants"] += result["consonants"]
        summary["digits"] += result["digits"]
        summary["symbols"] += result["symbols"]
        summary["entropy"] += result["entropy"]

    if summary["files"] > 0:
        summary["average_entropy"] = (
            summary["entropy"]
            / summary["files"]
        )
    else:
        summary["average_entropy"] = 0

    return summary


# =========================================================
# MAIN PROGRAM
# =========================================================

def main():

    print("=" * 60)
    print("HYBRID COMPUTING: PARALLEL FILE ANALYZER")
    print("=" * 60)

    print(f"NIM           : {NIM}")
    print(f"Random Seed   : {RANDOM_SEED}")
    print(f"Threads       : {THREADS}")
    print(f"Processes     : {PROCESSES}")
    print(f"Target Data   : {TOTAL_FILES}")
    print("=" * 60)

    # 1. Generate dataset
    generate_dataset()

    filepaths = sorted([
        os.path.join(DATASET_DIR, filename)
        for filename in os.listdir(DATASET_DIR)
        if filename.endswith(".txt")
    ])

    # Pastikan jumlah file benar
    filepaths = filepaths[:TOTAL_FILES]

    print(f"File tersedia : {len(filepaths)}")
    print("=" * 60)

    # 2. Sequential benchmark
    print("[1/2] Menjalankan benchmark sequential...")

    sequential_time, sequential_results = run_sequential(
        filepaths
    )

    print(
        f"Sequential Time : "
        f"{sequential_time:.4f} s"
    )

    # 3. Hybrid benchmark
    print("[2/2] Menjalankan pipeline hybrid...")

    hybrid_time, hybrid_results = run_hybrid(
        filepaths
    )

    print(
        f"Hybrid Time     : "
        f"{hybrid_time:.4f} s"
    )

    # 4. Performance
    speedup = sequential_time / hybrid_time

    efficiency = (
        speedup / PROCESSES
    ) * 100

    throughput = (
        len(filepaths) / hybrid_time
    )

    # 5. Aggregation
    summary = aggregate_results(
        hybrid_results
    )

    print("=" * 60)
    print("HASIL PERFORMANCE")
    print("=" * 60)

    print(f"Threads       : {THREADS}")
    print(f"Processes     : {PROCESSES}")
    print(f"Dataset       : {len(filepaths)}")
    print(f"Sequential    : {sequential_time:.4f} s")
    print(f"Hybrid        : {hybrid_time:.4f} s")
    print(f"Speedup       : {speedup:.2f}x")
    print(f"Efficiency    : {efficiency:.2f}%")
    print(f"Throughput    : {throughput:.2f} file/s")

    print("\nHASIL ANALISIS DATA")
    print("-" * 60)

    print(
        f"Total File      : "
        f"{summary['files']}"
    )

    print(
        f"Total Karakter  : "
        f"{summary['characters']:,}"
    )

    print(
        f"Total Kata      : "
        f"{summary['words']:,}"
    )

    print(
        f"Unique Words    : "
        f"{summary['unique_words']:,}"
    )

    print(
        f"Total Vokal     : "
        f"{summary['vowels']:,}"
    )

    print(
        f"Total Konsonan  : "
        f"{summary['consonants']:,}"
    )

    print(
        f"Total Digit     : "
        f"{summary['digits']:,}"
    )

    print(
        f"Total Simbol    : "
        f"{summary['symbols']:,}"
    )

    print(
        f"Average Entropy : "
        f"{summary['average_entropy']:.4f}"
    )

    print("=" * 60)
    print("Hybrid Project selesai.")
    print("=" * 60)


if __name__ == "__main__":
    main()