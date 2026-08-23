# If an instance is taken, it must be ensured that it is either added to its corresponding queue or stopped. 
# Not ensuring this causes a significant bug, as the instances would remain as zombies on the network until the service is removed.
from datetime import datetime
from time import sleep
from typing import Callable

import grpc

from node_controller.gateway.communication import stop, InstanceStopError


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
        """Ask the node to stop this instance.

        Raises InstanceStopError if the gateway never answered. Callers that are
        merely discarding the instance should use try_stop() instead, so a dead
        gateway does not take their thread down with it.
        """
        stop(gateway_stub=gateway_stub, token=self.token, debug=self.debug)

    def try_stop(self, gateway_stub) -> bool:
        """stop() for cleanup paths: never raises, reports whether it worked.

        A failed stop leaks a zombie instance on the node, so it is logged
        explicitly with the token rather than passing silently -- but it must not
        propagate out of a maintenance loop and kill the very thread whose job is
        to reap those zombies.
        """
        try:
            self.stop(gateway_stub)
            return True
        except InstanceStopError as e:
            self.debug(
                f"LEAKED INSTANCE: could not stop {self.token} at {self.uri}; it may still be "
                f"running and billing on the node. {e}"
            )
            return False

    def compute_exception(self, e: Exception) -> str:
        # https://github.com/avinassh/grpc-errors/blob/master/python/client.py
        if type(e) == grpc.RpcError and int(e.code().value[0]) == 4:
            self.timeout_passed()
            return 'timeout'

        else:
            self.error()
            return 'error'
