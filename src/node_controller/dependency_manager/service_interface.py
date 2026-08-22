from node_controller.dependency_manager.service_instance import ServiceInstance
from node_controller.dependency_manager.service_config import ServiceConfig


class ServiceInterface:

    def __init__(self,
                 gateway_stub,
                 service_with_config: ServiceConfig
                 ):
        self.gateway_stub = gateway_stub
        self.sc: ServiceConfig = service_with_config

    def get_instance(self, max_attempts: int=5) -> ServiceInstance:
        self.sc.lock.acquire()

        try:
            instance: ServiceInstance = self.sc.get_instance()
            self.sc.lock.release()

        except IndexError:
            self.sc.lock.release()
            instance: ServiceInstance = self.sc.launch_instance(
                gateway_stub=self.gateway_stub,
                max_attempts=max_attempts
            )

        instance.mark_time()
        return instance

    def push_instance(self, instance: ServiceInstance):

        # Si la instancia se encuentra en estado zombie
        # la detiene, en caso contrario la introduce
        #  de nuevo en su cola correspondiente.
        if instance.is_zombie(
                pass_timeout_times=self.sc.pass_timeout_times,
                timeout=self.sc.timeout,
                failed_attempts=self.sc.failed_attempts
        ):
            # try_stop: returning an instance to the pool must not raise just
            # because the gateway is unreachable. A failed stop is logged as a
            # leak instead.
            instance.try_stop(
                self.gateway_stub
            )

        else:
            self.sc.lock.acquire()
            self.sc.add_instance(
                instance=instance
            )
            self.sc.lock.release()
