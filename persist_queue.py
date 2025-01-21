import persistqueue

from utils import QUEUE_PATH

# https://github.com/peter-wangxu/persist-queue
# BSD-3-Clause license
PersistQueue = persistqueue.SQLiteQueue(QUEUE_PATH, auto_commit=True)

if __name__ == "__main__":
    pass
