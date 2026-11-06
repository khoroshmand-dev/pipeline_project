import hashlib
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Lock, Process, Queue, Value


def analyzer(post):
    complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(post["body"])
    text = post["title"] + post["body"]
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    post["complexity_score"] = complexity_score
    post["text_hash"] = text_hash

    with worker_lock:
        worker_counter.value += 1
    return post


def process_posts(posts):
    monitor_queue = Queue()
    success_counter = Value("i", 0)
    counter_lock = Lock()
    monitor_process = Process(target=monitor_posts, args=(monitor_queue,))
    monitor_process.start()
    processed_posts = []

    try:
        with ProcessPoolExecutor(
            initializer=workers,
            initargs=(success_counter, counter_lock),
        ) as executor:
            futures = {
                executor.submit(analyzer, post): post["id"]
                for post in posts
            }
            for future in as_completed(futures):
                post_id = futures[future]
                try:
                    processed_post = future.result()
                except Exception as exc:
                    print(f"[Analyzer] Ошибка обработки поста {post_id}: {exc}")
                    continue
                processed_posts.append(processed_post)
                monitor_queue.put(processed_post["id"])
    finally:
        monitor_queue.put(None)
        monitor_process.join()
        monitor_queue.close()
        monitor_queue.join_thread()

    return processed_posts, success_counter.value


def monitor_posts(monitor_queue):
    while True:
        post_id = monitor_queue.get()
        if post_id is None:
            break
        print(f"[Monitor] Пост {post_id} обработан.", flush=True)


def workers(counter, lock):
    global worker_counter, worker_lock
    worker_counter = counter
    worker_lock = lock
