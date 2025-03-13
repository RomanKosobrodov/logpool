import logging
import logging.handlers
from multiprocessing import Pool, Queue, Process, current_process
import random


def worker_task(args):
    seed, count = args

    process = current_process()
    task_logger = logging.getLogger(name=process.name)
    task_logger.debug(f"Worker task started as process \"{process.name}\"; seed={seed}; count={count}")

    random.seed(seed)
    result = random.randint(0, 1000)
    for k in range(1, count):
        result = (result * (count - 1) + random.randint(0, 1000)) / count

    task_logger.info(f"Worker process \"{process.name}\" finished calculations, result={result:.3f}")
    return result


def init_logger(log_queue, level=logging.DEBUG):
    handler = logging.handlers.QueueHandler(log_queue)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


class Sink:
    class Sentinel:
        pass

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.queue = Queue()
        self.process = None

    def do_logging(self):
        root_logger = logging.getLogger()
        handler = logging.handlers.RotatingFileHandler(filename=self.kwargs["filename"],
                                                 mode=self.kwargs["mode"],
                                                 maxBytes=self.kwargs["maxBytes"],
                                                 backupCount=self.kwargs["backupCount"])
        formatter = logging.Formatter(fmt=self.kwargs["format"])
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        while True:
            record = self.queue.get()
            if isinstance(record, self.Sentinel):
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)

    def start(self):
        self.process = Process(target=self.do_logging)
        self.process.start()

    def stop(self):
        if self.process is None:
            return
        self.queue.put_nowait(self.Sentinel())
        self.process.join()


if __name__ == "__main__":
    sink = Sink(filename="multiprocessing_pool.log",
                mode="w",
                maxBytes=20000,
                backupCount=3,
                format="%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s")
    pool = Pool(processes=2, initializer=init_logger, initargs=(sink.queue,))

    init_logger(sink.queue)
    logger = logging.getLogger("main")
    logger.debug("Starting the sink")

    sink.start()

    logger.debug("Starting the tasks")
    task_size = 1000
    tasks = ((3264328, task_size), (87529, task_size), (64209, task_size), (87529, task_size))
    for r in pool.imap(worker_task, tasks):
        print(f"{r:.3f}")

    logger.debug("Stopping the sink")

    sink.stop()



