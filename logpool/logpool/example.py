import logging
import random
from multiprocessing import current_process
from logpool.logger import LoggingPool


def worker_task(args):
    seed, count = args

    process = current_process()
    worker_logger = logging.getLogger(name=process.name)
    worker_logger.debug(f"Worker task started as process \"{process.name}\"; seed={seed}; count={count}")

    random.seed(seed)
    result = 0.0
    for k in range(count):
        result += random.randint(0, 1000) / count

    worker_logger.info(f"Worker process \"{process.name}\" finished calculations, result={result:.3f}")
    return result


if __name__ == "__main__":
    task_size = 10000
    tasks = ((3264328, task_size), (87529, task_size), (64209, task_size), (87529, task_size))
    with LoggingPool(processes=2,
                     filename="multiprocessing_pool.log") as pool:
        for r in pool.imap(worker_task, tasks):
            print(f"{r:.3f}")




