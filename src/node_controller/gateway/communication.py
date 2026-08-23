from time import sleep
import os
from typing import List, Tuple, Callable, Optional

from bee_rpc.client import Dir, client_grpc
import grpc

from node_controller.gateway.protos import celaut_pb2, celaut_pb2_grpc
from node_controller.gateway.protos.gateway_bee import StartService_input_indices
from node_controller.gateway.utils import from_amount


VALIDATE_HASH_INTEGRITY = True

# How many times stop() retries before giving up. It used to loop forever, which
# turns an unreachable gateway into a hung thread instead of a reportable error.
STOP_MAX_ATTEMPTS = 5


class GatewayCallError(RuntimeError):
    """A gateway RPC failed on every attempt.

    Carries the last real exception so callers can inspect it, e.g.::

        except GatewayCallError as e:
            if isinstance(e.last_error, grpc.RpcError) and \\
                    e.last_error.code() == grpc.StatusCode.UNAVAILABLE:
                ...  # the node is unreachable, which is not the node's fault

    This distinction matters to consumers that turn observations into
    accusations (reputation systems): "the node refused" and "the node could not
    be reached" must not collapse into the same opaque failure.
    """

    def __init__(self, operation: str, attempts: int, last_error: BaseException, detail: str = ""):
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"{operation} failed after {attempts} attempt(s): "
            f"{type(last_error).__name__}: {last_error}" + (f" ({detail})" if detail else "")
        )


class InstanceLaunchError(GatewayCallError):
    """Every StartService attempt failed; no instance was obtained."""

    def __init__(self, service_hash: str, attempts: int, last_error: BaseException, detail: str = ""):
        self.service_hash = service_hash
        super().__init__(f"launching an instance of {service_hash}", attempts, last_error, detail)


class InstanceStopError(GatewayCallError):
    """Every StopService attempt failed; the instance may still be running."""

    def __init__(self, token: str, attempts: int, last_error: BaseException, detail: str = ""):
        self.token = token
        super().__init__(f"stopping instance {token}", attempts, last_error, detail)


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
    last_error: Optional[BaseException] = None
    while attempt < max_attempts:
        attempt += 1
        debug(f'    - attempt: {attempt}')
        try:
            # Returning from inside the try is deliberate: the previous `break`
            # left `instance` unbound when every attempt failed, so the function
            # ended in `return instance` -> UnboundLocalError, and the real gRPC
            # status was already lost to debug().
            return next(client_grpc(
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
        except grpc.RpcError as e:
            debug('GRPC ERROR LAUNCHING INSTANCE. ' + str(e))
            last_error = e
            # Don't sleep after the final attempt: it delayed the failure by a
            # second without ever retrying again.
            if attempt < max_attempts:
                sleep(1)
        except StopIteration as e:
            # The stream completed without yielding a ServiceInstance. That is a
            # deterministic protocol/format mismatch, not a transient fault, so
            # retrying cannot help -- fail immediately with something readable
            # instead of a bare StopIteration.
            debug('StartService stream ended without yielding a ServiceInstance.')
            raise InstanceLaunchError(
                service_hash=service_hash,
                attempts=attempt,
                last_error=e,
                detail="the StartService stream completed without yielding a ServiceInstance; "
                       "this usually means a bee-rpc format skew between node and library",
            ) from e

    raise InstanceLaunchError(
        service_hash=service_hash,
        attempts=max_attempts,
        last_error=last_error or RuntimeError("no attempt was made (max_attempts < 1)"),
    )


def stop(gateway_stub, token: str,
         max_attempts: int = STOP_MAX_ATTEMPTS,
         debug: Callable[[str], None]=lambda s: None):
    debug('Stops this instance with token ' + str(token))
    attempt = 0
    last_error: Optional[BaseException] = None
    # Bounded, unlike the previous `while True`: an unreachable gateway used to
    # park the calling thread here forever. That silently kills the
    # DependencyManager maintenance thread, which is what reaps zombie
    # instances -- so the loop meant to guarantee cleanup prevented all of it.
    while attempt < max_attempts:
        attempt += 1
        try:
            next(client_grpc(
                method=gateway_stub.StopService,
                input=celaut_pb2.TokenMessage(
                    token=token
                ),
                indices_serializer=celaut_pb2.TokenMessage
            ))
            return
        except StopIteration:
            # StopService returns nothing meaningful; an empty stream is success.
            return
        except grpc.RpcError as e:
            debug('GRPC ERROR STOPPING SOLVER ' + str(e))
            last_error = e
            if attempt < max_attempts:
                sleep(1)

    raise InstanceStopError(
        token=token,
        attempts=max_attempts,
        last_error=last_error or RuntimeError("no attempt was made (max_attempts < 1)"),
        detail="the instance may still be running on the node",
    )


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
