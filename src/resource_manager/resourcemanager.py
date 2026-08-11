# I/O Big Data utils.
import gc
from threading import Lock
from time import sleep

from resource_manager.singleton import Singleton


def mem_manager(len: int): return ResourceManager().lock(len=len)


class ResourceManager(metaclass=Singleton):
    class RamLocker(object):
        def __init__(self, _len: int, iobd):
            self.len = _len
            self.iobd = iobd

        def __enter__(self):
            self.iobd.lock_ram(ram_amount=self.len)
            return self

        def unlock(self, amount: int):
            self.iobd.unlock_ram(ram_amount=amount)
            self.len -= amount

        def __exit__(self, _type, value, traceback):
            self.iobd.unlock_ram(ram_amount=self.len)
            gc.collect()

    def __init__(self,
                 log=lambda message: print(message),
                 ram_pool_method=None,
                 modify_resources=None
                 ) -> None:

        self.ram_pool = ram_pool_method
        self.balance_mu: int = 0  # Instance balance in MU, as reported by the node on each resize.
        self.modify_resources = modify_resources  # {min, max} -> (sysresources, balance_mu)
        self.last_request = None

        self.log = log
        self.ram_locked = 0
        self.get_ram_available = lambda: self.ram_pool() - self.ram_locked
        self.amount_lock = Lock()

        self.wait = []
        self.wait_lock = Lock()

    # General methods.

    def set_log(self, log=lambda message: print(message)) -> None:
        self.log = log

    @staticmethod
    def convert_size(size_bytes):
        import math
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        try:
            i = int(math.floor(math.log(size_bytes, 1024)))
            p = math.pow(1024, i)
            s = round(size_bytes / p, 2)
            return "%s %s" % (s, size_name[i])
        except ValueError:
            return "%s %s" % (size_bytes, size_name[0])

    def __stats(self, message: str):
        with self.amount_lock:
            self.log('\n--------- ' + message + ' -------------')
            self.log('RAM POOL       -> ' + ResourceManager.convert_size(self.ram_pool()))
            self.log('RAM LOCKED     -> ' + ResourceManager.convert_size(self.ram_locked))
            self.log('RAM AVAILABLE  -> ' + ResourceManager.convert_size(self.get_ram_available()))
            self.log('RAM WAITING    -> ' + ResourceManager.convert_size(sum(self.wait)))
            self.log('BALANCE (MU)   -> ' + str(self.balance_mu))
            self.log('-----------------------------------------\n')

    # Resource negotiation with the node.
    def __update_resources(self, modify_formula):
        if not self.modify_resources:
            return

        request = {
            "min": int(modify_formula(min)),  # min resources.
            "max": int(modify_formula(sum))  # max resources.
        }
        # A lock/unlock cycle of the same size lands back on the range already in force.
        # The node charges pricing.MODIFY_RESOURCES_MU per call, so asking it again for
        # what it is already doing is paid churn.
        if request == self.last_request:
            return
        self.last_request = request

        resources, self.balance_mu = self.modify_resources(request)
        self.ram_pool = lambda: resources.mem_limit

    def __push_wait_list(self, l: int):
        with self.wait_lock:
            self.wait.append(l)
        if sum(self.wait) > self.get_ram_available():
            with self.amount_lock:
                self.__update_resources(
                    modify_formula=lambda m: self.ram_locked + m(self.wait)
                )

    def __pop_wait_list(self, l: int):
        with self.wait_lock:
            self.wait.remove(l)

    # Manage resources methods.

    def lock(self, len):
        return self.RamLocker(_len=len, iobd=self)

    def lock_ram(self, ram_amount: int, wait: bool = True):
        self.__stats('want lock ' + ResourceManager.convert_size(ram_amount))
        self.__push_wait_list(l=ram_amount)
        while True:
            self.__stats('go to lock ' + ResourceManager.convert_size(ram_amount))
            if wait:
                self.wait_to_prevent_kill(len=ram_amount)

            elif not self.prevent_kill(len=ram_amount):
                self.__pop_wait_list(l=ram_amount)
                raise Exception

            with self.amount_lock:
                if self.get_ram_available() >= ram_amount:
                    self.ram_locked += ram_amount
                else:
                    continue
            self.__pop_wait_list(l=ram_amount)
            break
        self.__stats('locked ' + ResourceManager.convert_size(ram_amount))

    def unlock_ram(self, ram_amount: int):
        with self.amount_lock:
            if ram_amount < self.ram_locked:
                self.ram_locked -= ram_amount
            else:
                self.ram_locked = 0

            if len(self.wait) == 0:
                self.__update_resources(
                    modify_formula=lambda m: self.ram_locked
                )
        self.__stats('unlocked ' + ResourceManager.convert_size(ram_amount))

    def prevent_kill(self, len: int) -> bool:
        with self.amount_lock:
            b = self.get_ram_available() >= len
        return b

    def wait_to_prevent_kill(self, len: int) -> None:
        while True:
            if not self.prevent_kill(len=len):
                sleep(1)
            else:
                return
