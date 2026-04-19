from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypeVar, Any

from tqdm import tqdm

T = TypeVar("T")
R = TypeVar("R")


def run_parallel(
    items: list[T],
    worker: Callable[[T, int], R],
    *,
    max_workers: int = 4,
    desc: str = "tasks",
) -> list[R]:
    results: list[R] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(worker, item, idx + 1): (item, idx + 1)
            for idx, item in enumerate(items)
        }

        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc):
            item, position = futures[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                results.append(
                    {
                        "status": "failed",
                        "item": item,
                        "position": position,
                        "error": str(e),
                    }
                )

    return results