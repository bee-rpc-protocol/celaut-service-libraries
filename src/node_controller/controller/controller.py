import os
from typing import Optional, Tuple, Callable

from node_controller.dependency_manager.dependency_manager import DependencyManager
from node_controller.dependency_manager.service_interface import ServiceInterface
from node_controller.gateway.communication import modify_resources as gateway_modify_resources
from node_controller.gateway.protos import celaut_pb2, gateway_pb2
from node_controller.utils.get_grpc_uri import get_grpc_uri
from node_controller.utils.lambdas import SHA3_256_ID
from node_controller.utils.read_file import read_file
from bee_rpc.client import read_from_file, read_multiblock_directory

from node_controller.utils.singleton import Singleton
from resource_manager.resourcemanager import ResourceManager


class Controller(metaclass=Singleton):

    def __init__(self, 
                 debug: Callable[[str], None]=lambda s: None, 
                 default_dependency_manager: bool=True, 
                 default_resource_manager: bool=True, 
                 app_dir: str="",
                 services_dir: str="__services__",
                 metadata_dir: str="__metadata__"
                ):
        debug("Init celaut node controller")
        config = celaut_pb2.ConfigurationFile()
        config.ParseFromString(
            read_file('/__config__')
        )
        
        debug("Configuration file loaded.")

        gateway_uri = get_grpc_uri(config.gateway)
        self.mem_limit: int = config.initial_sysresources.mem_limit
        self.node_url = f"{gateway_uri.ip}:{str(gateway_uri.port)}"
        self.services_dir = os.path.join(app_dir, services_dir)
        self.metadata_dir = os.path.join(app_dir, metadata_dir)
        os.makedirs(self.services_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

        if default_dependency_manager:
            DependencyManager(
                node_url=self.node_url,
                maintenance_sleep_time=60,
                timeout=30,
                failed_attempts=3,
                pass_timeout_times=5,
                dev_client=None,
                static_service_directory=self.services_dir,
                static_metadata_directory=self.metadata_dir,
                dynamic_service_directory=app_dir,
                dynamic_metadata_directory=app_dir,
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
                    config: Optional[gateway_pb2.Configuration] = None,
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
    
    def add_bee_file(self, 
                    file_path: str,
                    validate: bool=True,
                    config: Optional[gateway_pb2.Configuration]=None,
                    dynamic: bool=False,
                    timeout: int=None,
                    failed_attempts: int=None,
                    pass_timeout_times: int=None
                    ) -> ServiceInterface:
        # Read the file using bee_rpc.client
        it = read_from_file(path=file_path, indices={
            1: celaut_pb2.Metadata,
            2: celaut_pb2.Service,
        })
        
        # Extract the metadata directory and parse the metadata
        metadata_dir = next(it).dir
        service_dir = next(it).dir

        metadata = celaut_pb2.Metadata()
        metadata.ParseFromString(open(metadata_dir, "rb").read())
        
        hashtag_service_hash = ""
        for _hash in metadata.hashtag.hash:
            if _hash.type == SHA3_256_ID:
                hashtag_service_hash = _hash.value.hex()

        if validate:
            self.debug("validating ....")
            from hashlib import sha3_256
            validate_content = sha3_256()
            for i in read_multiblock_directory(directory=service_dir):
                validate_content.update(i)
            service_hash = validate_content.hexdigest()
            if hashtag_service_hash and service_hash != hashtag_service_hash:
                _msg = f"Invalid service hash {hashtag_service_hash} was validated: {service_hash}"
                self.debug(_msg)
                raise Exception(_msg)
            self.debug("validated correctly")
        else:
            service_hash = hashtag_service_hash
    
        if not service_hash:
            self.debug("Any service hash available")
            return
        
        # Move metadata to the metadata registry
        metadata_destination = os.path.join(self.metadata_dir, service_hash)
        os.system(f"mv {metadata_dir} {metadata_destination}")

        service_destination = os.path.join(self.services_dir, service_hash)
        os.system(f"mv {service_dir} {service_destination}")

        self.debug(f"Metadata directory {metadata_destination}")
        self.debug(f"Service directory {service_destination}")
        
        return self.add_service(
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
