import asyncio
import aiohttp


async def fetch_post(
    session: aiohttp.ClientSession,
    post_id: int,
    semaphore: asyncio.Semaphore,
) -> dict[str, str | int] | None:
    async with semaphore:
        asyncio.create_task(log_post(post_id))
        try:
            async with session.get(
                f"https://jsonplaceholder.typicode.com/posts/{post_id}"
            ) as response:
                if response.status != 200:
                    return None
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return None


async def fetch_all_posts() -> list[dict[str, str | int] | None]:
    semaphore = asyncio.Semaphore(5)
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            fetch_post(session, post_id, semaphore)
            for post_id in range(1, 101)
        ]
        return await asyncio.gather(*tasks)


async def log_post(post_id: int) -> None:
    print(f"Начало загрузки поста {post_id}")
