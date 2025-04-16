from typing import Optional, Tuple, Callable

from node_controller.dependency_manager.dependency_manager import DependencyManager
from node_controller.dependency_manager.service_interface import ServiceInterface
from node_controller.gateway.communication import modify_resources as gateway_modify_resources
from node_controller.gateway.protos import celaut_pb2
from node_controller.utils.get_grpc_uri import get_grpc_uri
from node_controller.utils.read_file import read_file

from node_controller.utils.singleton import Singleton
from resource_manager.resourcemanager import ResourceManager


class Controller(metaclass=Singleton):

    def __init__(self, debug: Callable[[str], None]=lambda s: None, default_dependency_manager=True, default_resource_manager=True):
        debug("Init celaut node controller")
        config = celaut_pb2.ConfigurationFile()
        config.ParseFromString(
            read_file('/__config__')
        )
        
        debug("Configuration file loaded.")

        gateway_uri = get_grpc_uri(config.gateway)
        self.mem_limit: int = config.initial_sysresources.mem_limit
        self.node_url = f"{gateway_uri.ip}:{str(gateway_uri.port)}"

        if default_dependency_manager:
            DependencyManager(
                node_url=self.node_url,
                maintenance_sleep_time=60,
                timeout=30,
                failed_attempts=3,
                pass_timeout_times=5,
                dev_client=None,
                static_service_directory="__services__",
                static_metadata_directory="__metadata__",
                dynamic_service_directory="",
                dynamic_metadata_directory="",
                debug=lambda message: debug(message)
            )

        if default_resource_manager:
            ResourceManager(
                log=lambda message: debug(message),
                ram_pool_method=lambda: self.mem_limit,
                modify_resources=lambda d: gateway_modify_resources(i=d, node_url=self.node_url)
            )

    def get_node_url(self) -> str:
        return self.node_url

    def get_mem_limit_at_start(self) -> int:
        return self.mem_limit

    def add_service(self,
                    service_hash: str,
                    config: Optional[celaut_pb2.Configuration] = None,
                    dynamic: bool = False,
                    timeout: int = None,
                    failed_attempts: int = None,
                    pass_timeout_times: int = None
                    ) -> ServiceInterface:
        return DependencyManager().add_service(
            service_hash=service_hash,
            config=config,
            dynamic=dynamic,
            timeout=timeout,
            failed_attempts=failed_attempts,
            pass_timeout_times=pass_timeout_times
        )

    def modify_resources(self, resources: dict) -> Tuple[celaut_pb2.Sysresources, int]:
        return gateway_modify_resources(
            i={'max': resources.get('max', 0), 'min': resources.get('min', 0)},
            node_url=self.node_url
        )
