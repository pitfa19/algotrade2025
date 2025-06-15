from dataclasses import dataclass, field
from collections import deque

@dataclass
class MicroState:
    last_mid: int = 0
    qpos_bid: int = 0          # queue rank %
    qpos_ask: int = 0
    flow_ewma: float = 0.0
    void_flag: bool = False
    mids: deque = field(default_factory=lambda: deque(maxlen=20))  # 20-msg window
