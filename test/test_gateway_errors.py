"""Gateway RPC failures must be reportable, not swallowed or infinite.

Run: python3 test/test_gateway_errors.py

Two defects are covered:

1. `launch_instance()` caught every `grpc.RpcError` into `debug()` and then
   `break`-ed out of the retry loop. When all attempts failed, `instance` was
   never bound, so `return instance` raised `UnboundLocalError: cannot access
   local variable 'instance'`. The real status (e.g. UNAVAILABLE, with the
   address) only ever reached `debug()`.

   Observed downstream in celaut-basics/demo-service#2: a node-honesty verifier
   read that `UnboundLocalError` as evidence of node misbehaviour and was about
   to publish it as a permanent on-chain reputation accusation. A library that
   loses the cause makes its consumers accuse the wrong party.

2. `stop()` retried in a `while True` with no attempt limit, so an unreachable
   gateway parked the caller forever. The caller is usually the
   DependencyManager maintenance thread -- the one that reaps zombie instances
   -- so the loop written to guarantee cleanup was what prevented it.

These tests use a fake stub and monkeypatch `client_grpc`, so no node, no
network and no bee-rpc install are needed.
"""
import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))


# --- stub bee_rpc so the module imports without the dependency installed -----
#
# celaut_pb2 imports buffer_pb2 from bee_rpc (not a local vendored copy --
# see the comment there), and celaut.proto's own Gateway service references
# `buffer.Buffer`/`buffer.Empty` by name, so the descriptor pool needs a real
# "buffer.proto" registered before celaut_pb2 can parse. A bare fake module
# without those message types would just move the failure from "no bee_rpc"
# to "buffer.Buffer not found in descriptor pool", so the stub builds the
# actual tiny descriptor instead of pretending the dependency doesn't matter.
if "bee_rpc" not in sys.modules:
    from google.protobuf.internal import builder as _builder
    from google.protobuf import descriptor_pb2, descriptor_pool

    bee = types.ModuleType("bee_rpc")
    bee_client = types.ModuleType("bee_rpc.client")
    bee_buffer_pb2 = types.ModuleType("bee_rpc.buffer_pb2")

    class Dir:
        def __init__(self, dir=None, _type=None):
            self.dir = dir
            self._type = _type

    bee_client.Dir = Dir
    bee_client.client_grpc = lambda **kw: iter(())
    bee.client = bee_client
    bee.buffer_pb2 = bee_buffer_pb2

    # Same construction the real generated *_pb2.py files use (see
    # node_controller/gateway/protos/celaut_pb2.py), just built from a
    # FileDescriptorProto instead of a baked-in bytes literal, so the stub
    # has no bytes blob to fall out of sync with the real buffer.proto.
    _fdp = descriptor_pb2.FileDescriptorProto()
    _fdp.name = "buffer.proto"
    _fdp.package = "buffer"
    _fdp.syntax = "proto3"
    _fdp.message_type.add(name="Empty")
    _buffer_msg = _fdp.message_type.add(name="Buffer")
    _buffer_msg.field.add(
        name="chunk", number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    )
    _descriptor = descriptor_pool.Default().AddSerializedFile(_fdp.SerializeToString())
    _builder.BuildMessageAndEnumDescriptors(_descriptor, bee_buffer_pb2.__dict__)
    _builder.BuildTopDescriptorsAndMessages(_descriptor, "bee_rpc.buffer_pb2", bee_buffer_pb2.__dict__)

    sys.modules["bee_rpc"] = bee
    sys.modules["bee_rpc.client"] = bee_client
    sys.modules["bee_rpc.buffer_pb2"] = bee_buffer_pb2

import grpc  # noqa: E402
from node_controller.gateway import communication  # noqa: E402
from node_controller.gateway.communication import (  # noqa: E402
    GatewayCallError, InstanceLaunchError, InstanceStopError, launch_instance, stop,
)


class FakeRpcError(grpc.RpcError):
    """Mimics a real UNAVAILABLE, which is what an unreachable gateway raises."""

    def __init__(self, code=grpc.StatusCode.UNAVAILABLE,
                 details="failed to connect to all addresses; last error: "
                         "UNKNOWN: ipv4:192.168.200.1:58443: Failed to connect"):
        self._code = code
        self._details = details
        super().__init__(f"{code}: {details}")

    def code(self):
        return self._code

    def details(self):
        return self._details


class FakeStub:
    StartService = object()
    StopService = object()


def _launch(**kw):
    """launch_instance with the boilerplate the tests don't care about."""
    params = dict(
        gateway_stub=FakeStub(), hashes=[], config=None, service_hash="deadbeef",
        static_service_directory="/tmp", static_metadata_directory="/tmp",
        dynamic_service_directory="/tmp", dynamic_metadata_directory="/tmp",
        dynamic=False, dev_client=None,
    )
    params.update(kw)
    return launch_instance(**params)


class LaunchInstanceTests(unittest.TestCase):

    def test_total_failure_raises_typed_error_not_unbound_local(self):
        with mock.patch.object(communication, "client_grpc", side_effect=FakeRpcError()), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceLaunchError) as ctx:
                _launch(max_attempts=2)
        # The regression itself.
        self.assertNotIsInstance(ctx.exception, UnboundLocalError)
        self.assertIsInstance(ctx.exception, GatewayCallError)

    def test_the_real_grpc_status_survives(self):
        with mock.patch.object(communication, "client_grpc", side_effect=FakeRpcError()), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceLaunchError) as ctx:
                _launch(max_attempts=2)
        e = ctx.exception
        self.assertIsInstance(e.last_error, grpc.RpcError)
        self.assertEqual(e.last_error.code(), grpc.StatusCode.UNAVAILABLE)
        # A caller can now tell "unreachable" from "refused" without parsing text.
        self.assertIn("192.168.200.1:58443", str(e))
        self.assertIn("deadbeef", str(e))
        self.assertEqual(e.attempts, 2)
        self.assertEqual(e.service_hash, "deadbeef")

    def test_all_attempts_are_used(self):
        calls = []

        def boom(**kw):
            calls.append(1)
            raise FakeRpcError()

        with mock.patch.object(communication, "client_grpc", side_effect=boom), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceLaunchError):
                _launch(max_attempts=4)
        self.assertEqual(len(calls), 4)

    def test_no_sleep_after_the_final_attempt(self):
        with mock.patch.object(communication, "client_grpc", side_effect=FakeRpcError()), \
             mock.patch.object(communication, "sleep") as slept:
            with self.assertRaises(InstanceLaunchError):
                _launch(max_attempts=3)
        # 3 attempts -> 2 inter-attempt waits, not 3.
        self.assertEqual(slept.call_count, 2)

    def test_success_on_a_retry_returns_the_instance(self):
        sentinel = object()
        seq = [FakeRpcError(), FakeRpcError()]

        def flaky(**kw):
            if seq:
                raise seq.pop(0)
            return iter([sentinel])

        with mock.patch.object(communication, "client_grpc", side_effect=flaky), \
             mock.patch.object(communication, "sleep"):
            self.assertIs(_launch(max_attempts=5), sentinel)

    def test_first_attempt_success_does_not_sleep(self):
        sentinel = object()
        with mock.patch.object(communication, "client_grpc",
                               side_effect=lambda **kw: iter([sentinel])), \
             mock.patch.object(communication, "sleep") as slept:
            self.assertIs(_launch(max_attempts=5), sentinel)
        slept.assert_not_called()

    def test_empty_stream_fails_fast_with_an_explanation(self):
        """A completed stream that yielded nothing is a format skew, not a fault
        worth retrying -- and it must not surface as a bare StopIteration."""
        calls = []

        def empty(**kw):
            calls.append(1)
            return iter(())

        with mock.patch.object(communication, "client_grpc", side_effect=empty), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceLaunchError) as ctx:
                _launch(max_attempts=5)
        self.assertEqual(len(calls), 1, "a deterministic skew must not be retried")
        self.assertIn("without yielding a ServiceInstance", str(ctx.exception))

    def test_non_grpc_exceptions_still_propagate_unchanged(self):
        """Only gRPC faults are retryable; a bug in our own code must not be
        retried five times and then relabelled as a gateway problem."""
        with mock.patch.object(communication, "client_grpc",
                               side_effect=ValueError("bug in the caller")), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(ValueError):
                _launch(max_attempts=3)


class StopTests(unittest.TestCase):

    def test_stop_is_bounded_and_raises(self):
        calls = []

        def boom(**kw):
            calls.append(1)
            raise FakeRpcError()

        with mock.patch.object(communication, "client_grpc", side_effect=boom), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceStopError) as ctx:
                stop(gateway_stub=FakeStub(), token="tok-1", max_attempts=3)
        self.assertEqual(len(calls), 3, "stop() must not loop forever")
        self.assertEqual(ctx.exception.token, "tok-1")
        self.assertIn("may still be running", str(ctx.exception))
        self.assertIsInstance(ctx.exception.last_error, grpc.RpcError)

    def test_stop_succeeds_on_an_empty_stream(self):
        with mock.patch.object(communication, "client_grpc", side_effect=lambda **kw: iter(())):
            stop(gateway_stub=FakeStub(), token="tok-2")  # must not raise

    def test_stop_succeeds_on_a_retry(self):
        seq = [FakeRpcError()]

        def flaky(**kw):
            if seq:
                raise seq.pop(0)
            return iter([object()])

        with mock.patch.object(communication, "client_grpc", side_effect=flaky), \
             mock.patch.object(communication, "sleep"):
            stop(gateway_stub=FakeStub(), token="tok-3", max_attempts=3)


class ServiceInstanceCleanupTests(unittest.TestCase):
    """A dead gateway must not take down the thread that reaps zombies."""

    def _instance(self):
        from node_controller.dependency_manager.service_instance import ServiceInstance
        return ServiceInstance(uri="1.2.3.4:5", token="tok", check_if_is_alive=lambda timeout: True,
                               debug=self.logs.append)

    def setUp(self):
        self.logs = []

    def test_try_stop_reports_failure_without_raising(self):
        inst = self._instance()
        with mock.patch.object(communication, "client_grpc", side_effect=FakeRpcError()), \
             mock.patch.object(communication, "sleep"):
            self.assertFalse(inst.try_stop(FakeStub()))
        # The leak is loud: a silent failure here means an instance keeps billing.
        self.assertTrue(any("LEAKED INSTANCE" in m and "tok" in m for m in self.logs), self.logs)

    def test_try_stop_returns_true_on_success(self):
        inst = self._instance()
        with mock.patch.object(communication, "client_grpc", side_effect=lambda **kw: iter(())):
            self.assertTrue(inst.try_stop(FakeStub()))

    def test_explicit_stop_still_raises_for_callers_that_want_to_know(self):
        inst = self._instance()
        with mock.patch.object(communication, "client_grpc", side_effect=FakeRpcError()), \
             mock.patch.object(communication, "sleep"):
            with self.assertRaises(InstanceStopError):
                inst.stop(FakeStub())


if __name__ == "__main__":
    unittest.main(verbosity=2)
