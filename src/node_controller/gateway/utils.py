from node_controller.gateway.protos import celaut_pb2


def to_amount(amount_mu: int) -> celaut_pb2.Amount:
    return celaut_pb2.Amount(n=str(int(amount_mu)))


def from_amount(amount: celaut_pb2.Amount) -> int:
    return int(amount.n)
