# If an instance is taken, it must be ensured that it is either added to its corresponding queue or stopped. 
# Not ensuring this causes a significant bug, as the instances would remain as zombies on the network until the service is removed.
from datetime import datetime
from time import sleep

import grpc

from node_controller.gateway.communication import stop


class ServiceInstance(object):
    def __init__(self, uri: str, token: str, check_if_is_alive: bool, debug: Callable[[str], None]=lambda s: None):
        self.uri = uri
        self.token = token
        self.creation_datetime = datetime.now()
        self.use_datetime = datetime.now()
        self.pass_timeout = 0
        self.failed_attempts = 0
        self.check_if_is_alive = check_if_is_alive
        self.debug = debug

    def error(self):
        sleep(1)  # Wait if the service is loading.
        self.failed_attempts = self.failed_attempts + 1

    def is_zombie(self,
                  pass_timeout_times,
                  timeout,
                  failed_attempts
                  ) -> bool:
        # In case it takes a long time to respond,
        #  check that the instance is still working
        return self.pass_timeout > pass_timeout_times and \
            not self.check_if_is_alive(timeout=timeout) \
            or self.failed_attempts > failed_attempts

    def timeout_passed(self):
        self.pass_timeout = self.pass_timeout + 1

    def reset_timers(self):
        self.pass_timeout = 0
        self.failed_attempts = 0

    def mark_time(self):
        self.use_datetime = datetime.now()

    def stop(self, gateway_stub):
        stop(gateway_stub=gateway_stub, token=self.token, debug=self.debug)

    def compute_exception(self, e: Exception) -> str:
        # https://github.com/avinassh/grpc-errors/blob/master/python/client.py
        if type(e) == grpc.RpcError and int(e.code().value[0]) == 4:
            self.timeout_passed()
            return 'timeout'

        else:
            self.error()
            return 'error'
