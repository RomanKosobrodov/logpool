import logging
import logging.handlers
from multiprocessing import Pool, Queue, Process, current_process
import random


def worker_task(args):
    seed, count = args

    process = current_process()
    worker_logger = logging.getLogger(name=process.name)
    worker_logger.debug(f"Worker task started as process \"{process.name}\"; seed={seed}; count={count}")

    random.seed(seed)
    result = random.randint(0, 1000) / count
    for k in range(1, count):
        result += random.randint(0, 1000) / count

    worker_logger.info(f"Worker process \"{process.name}\" finished calculations, result={result:.3f}")
    return result


class Sink:
    class Sentinel:
        pass

    def __init__(self,
                 fmt="%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s",
                 handler=logging.handlers.RotatingFileHandler,
                 *args,
                 **kwargs):
        self.handler = handler
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs
        self.queue = Queue()
        self.process = None

    def _do_logging(self):
        root_logger = logging.getLogger()
        file_handler = self.handler(*self.args, **self.kwargs)
        formatter = logging.Formatter(fmt=self.fmt)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        while True:
            record = self.queue.get()
            if isinstance(record, self.Sentinel):
                break
            sink_logger = logging.getLogger(record.name)
            sink_logger.handle(record)

    def start(self):
        self.process = Process(target=self._do_logging)
        self.process.start()

    def stop(self):
        if self.process is None:
            return
        self.queue.put_nowait(self.Sentinel())
        self.process.join()


class LoggingPool:
    def __init__(self,
                 processes=None,
                 initializer=None,
                 initargs=None,
                 maxtasksperchild=None,
                 level=logging.DEBUG,
                 *handler_args,
                 **handler_kwargs):
        self.level = level
        self.initializer=initializer
        self.sink = Sink(*handler_args, **handler_kwargs)
        pool_init_args = tuple() if initargs is None else initargs
        self.pool = Pool(processes=processes,
                         initializer=self._initialize,
                         initargs=pool_init_args,
                         maxtasksperchild=maxtasksperchild)

    def _initialize(self, *args):
        handler = logging.handlers.QueueHandler(self.sink.queue)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(self.level)
        if self.initializer is not None:
            self.initializer(*args)

    def __enter__(self):
        self.sink.start()
        return self.pool.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sink.stop()
        self.pool.__exit__(exc_type, exc_val, exc_tb)


if __name__ == "__main__":
    task_size = 10000
    tasks = ((3264328, task_size), (87529, task_size), (64209, task_size), (87529, task_size))
    with LoggingPool(processes=2,
                     filename="multiprocessing_pool.log",
                     mode="w",
                     maxBytes=20000,
                     backupCount=3) as pool:
        for r in pool.imap(worker_task, tasks):
            print(f"{r:.3f}")




