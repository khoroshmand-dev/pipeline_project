import asyncio
import time

from analyzer import process_posts
from fetcher import fetch_all_posts
from saver import save_posts


def run_pipeline():
    start_time = time.perf_counter()
    posts = asyncio.run(fetch_all_posts())
    valid_posts = [post for post in posts if post is not None]
    processed_posts, success_count = process_posts(valid_posts)
    saved_count = save_posts(processed_posts)
    total_time = time.perf_counter() - start_time

    print("=== Отчёт о работе пайплайна ===")
    print("Запрошено постов: 100")
    print(f"Успешно загружено: {len(valid_posts)}")
    print(f"Успешно обработано (CPU): {success_count}")
    print(f"Успешно сохранено (I/O): {saved_count}")
    print(f"Общее время: {total_time:.2f} сек.")


if __name__ == "__main__":
    run_pipeline()
