import os
import csv
import re
import math
import random
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from queue import Queue
from threading import Thread
import matplotlib.pyplot as plt


# =========================================================
# IDENTITAS & PARAMETER PRIBADI
# =========================================================

NAMA = "Fito Anugrah Nurzaman"
NIM = "247006111156"

RANDOM_SEED = 247006111156
TARGET_DATA = 1560

DATASET_DIR = "dataset"
RESULT_DIR = "results"
QUEUE_SIZE = 64

os.makedirs(RESULT_DIR, exist_ok=True)


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

    if len(existing) == TARGET_DATA:
        print(f"[DATASET] {TARGET_DATA} file sudah tersedia.")
        return

    # Hapus dataset lama agar benar-benar sesuai seed dan jumlah data
    for filename in existing:
        os.remove(os.path.join(DATASET_DIR, filename))

    print(f"[DATASET] Membuat {TARGET_DATA} file teks...")

    for i in range(TARGET_DATA):
        paragraph_count = rng.randint(5, 10)
        paragraphs = []

        for _ in range(paragraph_count):
            word_count = rng.randint(80, 150)

            words = [rng.choice(WORDS) for _ in range(word_count)]

            numbers = str(rng.randint(1000, 999999))
            symbols = "".join(rng.choice("!@#$%^&*") for _ in range(5))

            paragraphs.append(
                " ".join(words) + " " + numbers + " " + symbols
            )

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

    total_characters = len(text)

    vowels = sum(
        1 for c in text.lower()
        if c in "aiueo"
    )

    consonants = sum(
        1 for c in text.lower()
        if c.isalpha() and c not in "aiueo"
    )

    digits = sum(
        1 for c in text
        if c.isdigit()
    )

    symbols = sum(
        1 for c in text
        if not c.isalnum() and not c.isspace()
    )

    words = re.findall(r"\b\w+\b", text.lower())

    total_words = len(words)
    unique_words = len(set(words))

    # Shannon Entropy
    frequency = Counter(text)
    entropy = 0.0

    if total_characters > 0:
        for count in frequency.values():
            probability = count / total_characters
            entropy -= probability * math.log2(probability)

    return {
        "file": filepath,
        "characters": total_characters,
        "words": total_words,
        "unique_words": unique_words,
        "vowels": vowels,
        "consonants": consonants,
        "digits": digits,
        "symbols": symbols,
        "entropy": entropy
    }


# =========================================================
# THREAD I/O LOADER
# =========================================================

def thread_loader(filepaths, data_queue):
    try:
        for filepath in filepaths:
            try:
                with open(filepath, "r", encoding="utf-8") as file:
                    content = file.read()

                data_queue.put((filepath, content))

            except Exception as error:
                print(f"[ERROR] {filepath}: {error}")

    finally:
        # Sentinel: tanda bahwa thread selesai
        data_queue.put(None)


# =========================================================
# HYBRID EXECUTION
# =========================================================

def run_hybrid(filepaths, thread_count, process_count):
    start = time.perf_counter()

    data_queue = Queue(maxsize=QUEUE_SIZE)

    # Membagi file secara merata ke thread
    chunks = [
        filepaths[i::thread_count]
        for i in range(thread_count)
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
    finished_threads = 0

    with ProcessPoolExecutor(
        max_workers=process_count
    ) as process_pool:

        while finished_threads < thread_count:
            item = data_queue.get()

            if item is None:
                finished_threads += 1
                continue

            futures.append(
                process_pool.submit(
                    cpu_analyze_text,
                    item
                )
            )

        for future in as_completed(futures):
            results.append(future.result())

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
            summary["entropy"] / summary["files"]
        )
    else:
        summary["average_entropy"] = 0.0

    return summary


# =========================================================
# KONFIGURASI EKSPERIMEN
# =========================================================
# C5 adalah konfigurasi utama berdasarkan NIM:
# 2 Thread + 4 Process + 1560 Data

CONFIGS = [
    {"no": 1, "threads": 1, "processes": 1, "data": 1560},
    {"no": 2, "threads": 1, "processes": 4, "data": 1560},
    {"no": 3, "threads": 2, "processes": 2, "data": 1560},
    {"no": 4, "threads": 2, "processes": 3, "data": 1560},
    {"no": 5, "threads": 2, "processes": 4, "data": 1560},
    {"no": 6, "threads": 3, "processes": 4, "data": 1560},
    {"no": 7, "threads": 4, "processes": 4, "data": 1560},
    {"no": 8, "threads": 2, "processes": 1, "data": 1560},
    {"no": 9, "threads": 2, "processes": 6, "data": 1560},
    {"no": 10, "threads": 2, "processes": 4, "data": 780},
]


# =========================================================
# SIMPAN CSV
# =========================================================

def save_csv(rows):
    csv_path = os.path.join(
        RESULT_DIR,
        "hasil_eksperimen.csv"
    )

    fieldnames = [
        "No",
        "Thread",
        "Process",
        "Data",
        "Waktu (s)",
        "Speedup",
        "Efisiensi (%)",
        "Throughput (file/s)"
    ]

    with open(
        csv_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(rows)

    return csv_path


# =========================================================
# GRAFIK
# =========================================================

def make_charts(rows):
    # -----------------------------
    # 1. Waktu vs Thread
    # fixed process = 4, data = 1560
    # -----------------------------
    thread_rows = [
        row for row in rows
        if row["Process"] == 4
        and row["Data"] == 1560
    ]

    thread_rows.sort(key=lambda x: x["Thread"])

    plt.figure(figsize=(9, 5))
    plt.plot(
        [r["Thread"] for r in thread_rows],
        [r["Waktu (s)"] for r in thread_rows],
        marker="o"
    )
    plt.xlabel("Jumlah Thread")
    plt.ylabel("Waktu Eksekusi (s)")
    plt.title(
        "Grafik Waktu vs Jumlah Thread\n"
        "[Process = 4, Data = 1.560 File]"
    )
    plt.grid(True, alpha=0.3)

    for r in thread_rows:
        plt.annotate(
            f'{r["Waktu (s)"]:.2f}s',
            (r["Thread"], r["Waktu (s)"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center"
        )

    plt.tight_layout()

    thread_chart = os.path.join(
        RESULT_DIR,
        "grafik_waktu_vs_thread.png"
    )

    plt.savefig(thread_chart, dpi=200)
    plt.close()

    # -----------------------------
    # 2. Waktu vs Process
    # fixed thread = 2, data = 1560
    # -----------------------------
    process_rows = [
        row for row in rows
        if row["Thread"] == 2
        and row["Data"] == 1560
    ]

    process_rows.sort(key=lambda x: x["Process"])

    plt.figure(figsize=(9, 5))
    plt.plot(
        [r["Process"] for r in process_rows],
        [r["Waktu (s)"] for r in process_rows],
        marker="o"
    )
    plt.xlabel("Jumlah Process")
    plt.ylabel("Waktu Eksekusi (s)")
    plt.title(
        "Grafik Waktu vs Jumlah Process\n"
        "[Thread = 2, Data = 1.560 File]"
    )
    plt.grid(True, alpha=0.3)

    for r in process_rows:
        plt.annotate(
            f'{r["Waktu (s)"]:.2f}s',
            (r["Process"], r["Waktu (s)"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center"
        )

    plt.tight_layout()

    process_chart = os.path.join(
        RESULT_DIR,
        "grafik_waktu_vs_process.png"
    )

    plt.savefig(process_chart, dpi=200)
    plt.close()

    # -----------------------------
    # 3. Speedup vs Konfigurasi
    # -----------------------------
    plt.figure(figsize=(11, 5))

    labels = [
        f'C{r["No"]}\n(T{r["Thread"]} P{r["Process"]})'
        for r in rows
    ]

    speedups = [r["Speedup"] for r in rows]

    plt.bar(range(len(rows)), speedups)
    plt.axhline(
        y=1.0,
        linestyle="--",
        alpha=0.6,
        label="Baseline C1 (1.0x)"
    )

    plt.xticks(
        range(len(rows)),
        labels
    )

    plt.ylabel("Speedup (x)")
    plt.xlabel("Konfigurasi")
    plt.title("Grafik Speedup vs Konfigurasi")
    plt.legend()
    plt.grid(
        axis="y",
        alpha=0.3
    )

    for i, value in enumerate(speedups):
        plt.text(
            i,
            value + 0.03,
            f"{value:.2f}x",
            ha="center"
        )

    plt.tight_layout()

    speedup_chart = os.path.join(
        RESULT_DIR,
        "grafik_speedup_vs_konfigurasi.png"
    )

    plt.savefig(speedup_chart, dpi=200)
    plt.close()

    return (
        thread_chart,
        process_chart,
        speedup_chart
    )


# =========================================================
# MAIN EXPERIMENT
# =========================================================

def main():
    print("=" * 72)
    print("BAGIAN C - HASIL EKSPERIMEN")
    print("HYBRID PARALLEL FILE ANALYZER")
    print("=" * 72)

    print(f"Nama          : {NAMA}")
    print(f"NIM           : {NIM}")
    print(f"Random Seed   : {RANDOM_SEED}")
    print(f"Target Data   : {TARGET_DATA}")
    print("=" * 72)

    generate_dataset()

    all_files = sorted([
        os.path.join(DATASET_DIR, filename)
        for filename in os.listdir(DATASET_DIR)
        if filename.endswith(".txt")
    ])

    if len(all_files) < TARGET_DATA:
        raise RuntimeError(
            f"Dataset hanya memiliki {len(all_files)} file."
        )

    rows = []

    baseline_time = None
    baseline_results = None

    print("\nMemulai 10 konfigurasi...\n")

    for config in CONFIGS:
        no = config["no"]
        threads = config["threads"]
        processes = config["processes"]
        data_count = config["data"]

        filepaths = all_files[:data_count]

        print(
            f"[C{no:02d}] "
            f"Thread={threads} | "
            f"Process={processes} | "
            f"Data={data_count}"
        )

        elapsed, results = run_hybrid(
            filepaths,
            threads,
            processes
        )

        # C1 dijadikan baseline eksperimen
        if no == 1:
            baseline_time = elapsed
            baseline_results = results

        if baseline_time is None:
            raise RuntimeError("Baseline C1 belum tersedia.")

        speedup = baseline_time / elapsed

        # Mengikuti formulasi eksperimen pada contoh laporan:
        # Efficiency = Speedup / Process x 100%
        efficiency = (
            speedup / processes
        ) * 100

        throughput = data_count / elapsed

        rows.append({
            "No": no,
            "Thread": threads,
            "Process": processes,
            "Data": data_count,
            "Waktu (s)": round(elapsed, 4),
            "Speedup": round(speedup, 2),
            "Efisiensi (%)": round(efficiency, 2),
            "Throughput (file/s)": round(throughput, 2)
        })

        print(
            f"      Waktu={elapsed:.4f}s | "
            f"Speedup={speedup:.2f}x | "
            f"Efisiensi={efficiency:.2f}% | "
            f"Throughput={throughput:.2f} file/s"
        )

    # Simpan CSV
    csv_path = save_csv(rows)

    # Buat grafik
    charts = make_charts(rows)

    # Hasil konfigurasi utama C5
    main_config = next(
        row for row in rows
        if row["No"] == 5
    )

    # Agregasi statistik dari C5
    main_files = all_files[:1560]
    _, main_results = run_hybrid(
        main_files,
        2,
        4
    )

    summary = aggregate_results(main_results)

    print("\n" + "=" * 72)
    print("TABEL HASIL EKSPERIMEN")
    print("=" * 72)

    print(
        f'{"No":<4}'
        f'{"Thread":<9}'
        f'{"Process":<10}'
        f'{"Data":<9}'
        f'{"Waktu(s)":<12}'
        f'{"Speedup":<10}'
        f'{"Efisiensi":<13}'
        f'{"Throughput":<12}'
    )

    print("-" * 72)

    for row in rows:
        print(
            f'{row["No"]:<4}'
            f'{row["Thread"]:<9}'
            f'{row["Process"]:<10}'
            f'{row["Data"]:<9}'
            f'{row["Waktu (s)"]:<12.4f}'
            f'{row["Speedup"]:<10.2f}x'
            f'{row["Efisiensi (%)"]:<13.2f}%'
            f'{row["Throughput (file/s)"]:<12.2f}'
        )

    print("\n" + "=" * 72)
    print("KONFIGURASI UTAMA NIM (C5)")
    print("=" * 72)

    print(f'Thread       : {main_config["Thread"]}')
    print(f'Process      : {main_config["Process"]}')
    print(f'Data         : {main_config["Data"]}')
    print(f'Waktu        : {main_config["Waktu (s)"]:.4f} s')
    print(f'Speedup      : {main_config["Speedup"]:.2f}x')
    print(f'Efisiensi    : {main_config["Efisiensi (%)"]:.2f}%')
    print(f'Throughput   : {main_config["Throughput (file/s)"]:.2f} file/s')

    print("\nHASIL ANALISIS DATA C5")
    print("-" * 72)
    print(f'Total File        : {summary["files"]}')
    print(f'Total Karakter    : {summary["characters"]:,}')
    print(f'Total Kata        : {summary["words"]:,}')
    print(f'Unique Words      : {summary["unique_words"]:,}')
    print(f'Total Vokal       : {summary["vowels"]:,}')
    print(f'Total Konsonan    : {summary["consonants"]:,}')
    print(f'Total Digit       : {summary["digits"]:,}')
    print(f'Total Simbol      : {summary["symbols"]:,}')
    print(f'Average Entropy   : {summary["average_entropy"]:.4f}')

    print("\nFile hasil:")
    print(f"- {csv_path}")
    for chart in charts:
        print(f"- {chart}")

    print("=" * 72)
    print("Eksperimen selesai.")
    print("=" * 72)


if __name__ == "__main__":
    main()
