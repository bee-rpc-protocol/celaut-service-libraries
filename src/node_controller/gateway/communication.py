from time import sleep
import os
from typing import List, Tuple, Callable, Optional

from bee_rpc.client import Dir, client_grpc
import grpc

from node_controller.gateway.protos import celaut_pb2, celaut_pb2_grpc
from node_controller.gateway.protos.gateway_bee import StartService_input_indices
from node_controller.gateway.utils import from_amount


VALIDATE_HASH_INTEGRITY = True

def generate_gateway_stub(node_url: str) -> celaut_pb2_grpc.GatewayStub:
    return celaut_pb2_grpc.GatewayStub(
        grpc.insecure_channel(node_url)
    )


def generate_instance_stub(stub_class, uri: str):
    return stub_class(grpc.insecure_channel(uri))


def __service_extended(
        hashes: List[celaut_pb2.Metadata.HashTag.Hash],
        config: Optional[celaut_pb2.Configuration],
        service_hash: str,
        service_directory: str,
        metadata_directory: str,
        dev_client: str,
        debug: Callable[[str], None]=lambda s: None
):
    if dev_client:
        yield celaut_pb2.Client(client_id=dev_client)

    # No initial_mu on purpose: with it unset the node funds the instance for
    # deposits.INITIAL_RUNTIME_HOURS of the resources it actually asked for. Any
    # constant here would be a flat amount again, and 10000 MU (the old default,
    # back when MU was gas) buys a few seconds of a real instance.
    yield config if config else celaut_pb2.Configuration()

    for _hash in hashes:
        yield _hash

    metadata_path = os.path.join(metadata_directory, service_hash)
    service_path = os.path.join(service_directory, service_hash)

    # Check metadata directory
    if not os.path.exists(metadata_path):
        debug(
            "Metadata directory missing. Components:\n"
            f"- Base metadata directory: {metadata_directory} "
            f"({'exists' if os.path.exists(metadata_directory) else 'missing'})\n"
            f"- Service hash: {service_hash}\n"
            f"- Full path: {metadata_path}"
        )
        return

    debug(f"Found metadata directory at {metadata_path}")

    if VALIDATE_HASH_INTEGRITY:
        # Validate metadata integrity
        metadata = celaut_pb2.Metadata()
        metadata.ParseFromString(open(metadata_path, "rb").read())
        integrity_verification = [h.type for h in metadata.hashtag.hash]
        if len(integrity_verification) != len(set(integrity_verification)):
            _msg = f"ALERT: Integrity problem with metadata hashes: \n {[(h.type.hex(), h.value.hex()) for h in metadata.hashtag.hash]}"
            debug(_msg)
            raise Exception(_msg)

        yield metadata
        
    else:
        yield Dir(dir=metadata_path, _type=celaut_pb2.Metadata)

    # Check service directory
    if not os.path.exists(service_path):
        debug(
            "Service directory missing. Components:\n"
            f"- Base service directory: {service_directory} "
            f"({'exists' if os.path.exists(service_directory) else 'missing'})\n"
            f"- Service hash: {service_hash}\n"
            f"- Full path: {service_path}"
        )
        return
        
    debug(f"Found service directory at {service_path}")
    yield Dir(dir=service_path, _type=celaut_pb2.Service)


def launch_instance(gateway_stub,
                    hashes, config, service_hash,
                    static_service_directory: str,
                    static_metadata_directory: str,
                    dynamic_service_directory: str,
                    dynamic_metadata_directory: str,
                    dynamic: bool,
                    dev_client,
                    max_attempts: int=5,
                    debug: Callable[[str], None]=lambda s: None
                    ) -> celaut_pb2.ServiceInstance:
    debug(f'    launching new {"dynamic" if dynamic else "static"} instance for service {service_hash}')
    attempt = 0
    while attempt < max_attempts:
        attempt +=1
        debug(f'    - attempt: {attempt}')
        try:
            instance: celaut_pb2.Instance = next(client_grpc(
                method=gateway_stub.StartService,
                input=__service_extended(
                    hashes=hashes,
                    config=config,
                    service_hash=service_hash,
                    service_directory=dynamic_service_directory if dynamic else static_service_directory,
                    metadata_directory=dynamic_metadata_directory if dynamic else static_metadata_directory,
                    dev_client=dev_client,
                    debug=debug
                ),
                indices_parser=celaut_pb2.ServiceInstance,
                partitions_message_mode_parser=True,
                indices_serializer=StartService_input_indices,
                debug=lambda s: debug(f'bee-rpc debug: {s}')
            ))
            break
        except grpc.RpcError as e:
            debug('GRPC ERROR LAUNCHING INSTANCE. ' + str(e))
            sleep(1)

    return instance


def stop(gateway_stub, token: str, debug: Callable[[str], None]=lambda s: None):
    debug('Stops this instance with token ' + str(token))
    while True:
        try:
            next(client_grpc(
                method=gateway_stub.StopService,
                input=celaut_pb2.TokenMessage(
                    token=token
                ),
                indices_serializer=celaut_pb2.TokenMessage
            ))
            break
        except grpc.RpcError as e:
            debug('GRPC ERROR STOPPING SOLVER ' + str(e))
            sleep(1)


def modify_resources(i: dict, node_url: str) -> Tuple[celaut_pb2.Sysresources, int]:
    output: celaut_pb2.ModifyServiceSystemResourcesOutput = next(
        client_grpc(
            method=celaut_pb2_grpc.GatewayStub(
                grpc.insecure_channel(node_url)
            ).ModifyServiceSystemResources,
            input=celaut_pb2.ModifyServiceSystemResourcesInput(
                min_sysreq=celaut_pb2.Sysresources(
                    mem_limit=i['min']
                ),
                max_sysreq=celaut_pb2.Sysresources(
                    mem_limit=i['max']
                ),
            ),
            partitions_message_mode_parser=True,
            indices_parser=celaut_pb2.ModifyServiceSystemResourcesOutput,
        )
    )
    return output.sysreq, from_amount(output.balance)
