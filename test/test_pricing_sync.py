"""Wire contract with the node after the MU pricing rework (nodo#243).

Run: python3 test/test_amount.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from node_controller.gateway.protos import celaut_pb2
from node_controller.gateway.utils import to_amount, from_amount

# Field numbers did not change with the rename, so a node or service still running the
# pre-243 schema stays readable both ways.
assert celaut_pb2.Configuration.DESCRIPTOR.fields_by_name["initial_mu"].number == 3
assert celaut_pb2.ModifyServiceSystemResourcesOutput.DESCRIPTOR.fields_by_name["balance"].number == 2

assert from_amount(to_amount(10 ** 8)) == 10 ** 8
assert to_amount(1e64).n.isdigit()  # never "1e+64", which from_amount cannot parse

# An empty Configuration must leave initial_mu unset: that absence is what makes the node
# fund the instance for INITIAL_RUNTIME_HOURS of the resources it asked for, instead of a
# flat amount the library made up.
assert not celaut_pb2.Configuration().HasField("initial_mu")

# Re-asking the node for the range it is already applying is billed at
# pricing.MODIFY_RESOURCES_MU like any other resize, so it must not leave the library.
from resource_manager.resourcemanager import ResourceManager

calls = []
rm = ResourceManager(
    log=lambda m: None,
    ram_pool_method=lambda: 1000,
    modify_resources=lambda d: (calls.append(d), celaut_pb2.Sysresources(mem_limit=1000), 0)[1:],
)
update = rm._ResourceManager__update_resources
update(modify_formula=lambda m: 500)
update(modify_formula=lambda m: 500)
assert len(calls) == 1, calls
update(modify_formula=lambda m: 800)
assert len(calls) == 2, calls

print("ok")
