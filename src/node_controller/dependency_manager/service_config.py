from threading import Lock
from typing import List, Callable, Any, Tuple, Union, Optional

from node_controller.dependency_manager.service_instance import ServiceInstance
from node_controller.gateway.communication import launch_instance
from node_controller.gateway.protos import gateway_pb2, celaut_pb2 as celaut
from node_controller.utils.get_grpc_uri import get_grpc_uri
from node_controller.utils.lambdas import SHA3_256_ID
from node_controller.utils.network import is_open


class ServiceConfig(object):
    def __init__(self,
                 service_hash: str,
                 config: celaut.Configuration,
                 timeout: int,
                 failed_attempts: int,
                 pass_timeout_times: int,
                 dynamic: bool,
                 dev_client: str,
                 static_service_directory: str,
                 static_metadata_directory: str,
                 dynamic_service_directory: str,
                 dynamic_metadata_directory: str,
                 check_if_is_alive: Optional[Callable[[], bool]]=None,
                 debug: Callable[[str], None]=lambda s: None
        ):

        self.lock: Lock = Lock()

        self.dev_client = dev_client
        self.static_service_directory = static_service_directory
        self.static_metadata_directory = static_metadata_directory
        self.dynamic_service_directory = dynamic_service_directory
        self.dynamic_metadata_directory = dynamic_metadata_directory

        self.service_hash: str = service_hash
        self.config: celaut.Configuration = config if config else celaut.Configuration()
        self.hashes = [
            celaut.Metadata.HashTag.Hash(
                type=SHA3_256_ID,
                value=bytes.fromhex(service_hash)
            )
        ]

        self.debug = debug

        # Service's instances.
        self.instances: List[ServiceInstance] = []  # A stack is used so that the 'maintainer' can detect instances that fall into disuse, as they will remain stuck at the end of the stack.


        self.check_if_is_alive = check_if_is_alive
        self.timeout = timeout
        self.failed_attempts = failed_attempts
        self.pass_timeout_times = pass_timeout_times

        self.dynamic = dynamic  # Dynamic if is acquired by the api

    def add_instance(self, instance: ServiceInstance, deep=False):
        self.debug('Add instance ' + str(instance))
        self.instances.append(instance) if not deep else self.instances.insert(0, instance)

    def get_instance(self, deep=False) -> ServiceInstance:
        self.debug('Get an instance of. deep ' + str(deep))
        self.debug('The service ' + self.hashes[0].value.hex() + ' has ' + str(len(self.instances)) + ' instances.')
        try:
            return self.instances.pop() if not deep else self.instances.pop(0)
        except IndexError:
            self.debug('    list empty --> ' + str(self.instances))
            raise IndexError

    def launch_instance(self, gateway_stub, max_attempts: int=5) -> ServiceInstance:
        instance = launch_instance(
            gateway_stub=gateway_stub,
            service_hash=self.service_hash,
            hashes=self.hashes,
            config=self.config,
            static_service_directory=self.static_service_directory,
            static_metadata_directory=self.static_metadata_directory,
            dynamic_service_directory=self.dynamic_service_directory,
            dynamic_metadata_directory=self.dynamic_metadata_directory,
            dynamic=self.dynamic,
            dev_client=self.dev_client,
            max_attempts=max_attempts,
            debug=self.debug
        )

        try:
            uri = get_grpc_uri(instance.instance)
        except Exception as e:
            self.debug(str(e))
            raise e
        self.debug('The uri for the service ' + self.service_hash + ' is--> ' + str(uri))

        return ServiceInstance(
            uri=f"{uri.ip}:{str(uri.port)}",
            token=instance.token,
            check_if_is_alive=self.check_if_is_alive
                if self.check_if_is_alive
                else lambda timeout: is_open(timeout=timeout, ip=uri.ip, port=uri.port),
            debug=self.debug
        )

    def get_service_with_config(self, mem_manager: Callable[[int], Any]) \
            -> Tuple[
                Union[str, celaut.Service],
                Union[str, celaut.Metadata],
                gateway_pb2.Configuration]:
        raise Exception("Not implemented.")

        # return get_from_registry(
        #     service_hash=self.service_hash,
        #     registry=self.dynamic_service_directory if self.dynamic else self.static_service_directory,
        #    mem_manager=mem_manager
        # ), self.config
