from node_controller.gateway.protos import celaut_pb2


def to_gas_amount(gas_amount: int) -> celaut_pb2.GasAmount:
    return celaut_pb2.GasAmount(n=str(gas_amount))


def from_gas_amount(gas_amount: celaut_pb2.GasAmount) -> int:
    return int(gas_amount.n)
