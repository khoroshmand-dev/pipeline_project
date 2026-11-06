import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def save_post(post_data, lock_file):
    time.sleep(0.05)
    with lock_file:
        with open("results.log", "a", encoding="utf-8") as file:
            file.write(f"Записан пост {post_data['id']}\n")


def save_workers(task_queue, lock_file, saved_counter):
    while True:
        post_data = task_queue.get()
        if post_data is None:
            break
        try:
            save_post(post_data, lock_file)
        except Exception as exc:
            print(f"[Saver] Ошибка сохранения поста {post_data.get('id', '?')}: {exc}")
            continue
        with lock_file:
            saved_counter[0] += 1


def progress_monitor(stop_event, saved_counter, total_posts):
    while not stop_event.is_set():
        if stop_event.wait(2):
            break
        print(f">>> Прогресс сохранения: {saved_counter[0]} / {total_posts}")


def save_posts(posts):
    task_queue = queue.Queue()
    lock_file = threading.Lock()
    stop_event = threading.Event()
    saved_counter = [0]

    for post in posts:
        task_queue.put(post)

    monitor_thread = threading.Thread(
        target=progress_monitor,
        args=(stop_event, saved_counter, len(posts)),
    )
    monitor_thread.start()
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(save_workers, task_queue, lock_file, saved_counter)
                for _ in range(5)
            ]
            for _ in range(5):
                task_queue.put(None)
            for future in futures:
                future.result()
    finally:
        stop_event.set()
        monitor_thread.join()

    print(f">>> Прогресс сохранения: {saved_counter[0]} / {len(posts)}")
    return saved_counter[0]
