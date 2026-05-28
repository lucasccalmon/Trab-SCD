from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set
import heapq
import itertools

Timestamp = Tuple[int, int]  # (lamport, process_id) -> desempate por ID do processo


@dataclass(frozen=True)
class Operation:
    """Operação replicada, aplicada em todas as réplicas na mesma ordem."""
    kind: str  # 'deposit' ou 'interest'
    value: float

    def apply(self, balance: float) -> float:
        if self.kind == "deposit":
            return balance + self.value
        if self.kind == "interest":
            return balance * (1 + self.value)
        raise ValueError(f"Operação desconhecida: {self.kind}")

    def describe(self) -> str:
        if self.kind == "deposit":
            return f"deposit({self.value:.2f})"
        if self.kind == "interest":
            return f"interest({self.value:.4f})"
        return f"op({self.kind}, {self.value})"


@dataclass(frozen=True)
class TransactionMessage:
    tx_id: str
    timestamp: Timestamp
    sender: int
    operation: Operation


@dataclass(frozen=True)
class AckMessage:
    tx_id: str
    ack_sender: int


@dataclass(order=True)
class QueueItem:
    sort_key: Timestamp
    tx_id: str = field(compare=False)


@dataclass
class TransactionRecord:
    tx_id: str
    timestamp: Timestamp
    sender: int
    operation: Operation
    acks: Set[int] = field(default_factory=set)
    delivered: bool = False


class Network:
    """
    Rede simulada em memória.

    Mantém uma fila FIFO global de entrega para simplificar a demonstração.
    """

    def __init__(self) -> None:
        self.processes: Dict[int, Process] = {}
        self.event_queue: List[Tuple[int, int, object]] = []
        self._seq = itertools.count()

    def register(self, process: "Process") -> None: #cadastra quem são os processos da nossa rede.
        self.processes[process.pid] = process

    def multicast(self, msg: object) -> None:  #ao invés de entregar as mensagens instantaneamente para os outros processos, ela enfileira tudo em uma fila global chamada event_queue.
        # Emula multicast FIFO e confiável: todos receberão a mensagem.
        for pid in sorted(self.processes):
            seq = next(self._seq)
            self.event_queue.append((seq, pid, msg))

    def run(self, verbose: bool = True) -> None:
        # Entrega mensagens na ordem de enfileiramento.
        while self.event_queue:
            seq, pid, msg = self.event_queue.pop(0)
            self.processes[pid].receive(msg, verbose=verbose)


class Process:
    def __init__(self, pid: int, network: Network, num_processes: int, initial_balance: float = 1000.0) -> None:
        self.pid = pid
        self.network = network
        self.num_processes = num_processes
        self.clock = 0
        self.balance = initial_balance

        # Estruturas do TOM
        self.pending_heap: List[QueueItem] = []
        self.records: Dict[str, TransactionRecord] = {}
        self.delivery_log: List[str] = []

        network.register(self)

    # ---------------- Relógio de Lamport ----------------
    def tick_local_event(self) -> int:
        self.clock += 1
        return self.clock

    def update_clock_on_receive(self, received_lamport: int) -> None:
        self.clock = max(self.clock, received_lamport) + 1

    # ---------------- API do protocolo ----------------
    def submit_transaction(self, tx_id: str, operation: Operation, verbose: bool = True) -> None:
        """Cria uma transação local e envia em multicast, inclusive para si mesmo."""
        lamport = self.tick_local_event()
        timestamp = (lamport, self.pid)
        msg = TransactionMessage(
            tx_id=tx_id,
            timestamp=timestamp,
            sender=self.pid,
            operation=operation,
        )
        if verbose:
            print(f"P{self.pid} CRIA {tx_id} com timestamp {timestamp} e faz multicast de {operation.describe()}")
        self.network.multicast(msg)

    def receive(self, msg: object, verbose: bool = True) -> None:
        if isinstance(msg, TransactionMessage):
            self._receive_transaction(msg, verbose=verbose)
        elif isinstance(msg, AckMessage):
            self._receive_ack(msg, verbose=verbose)
        else:
            raise TypeError(f"Mensagem desconhecida: {type(msg)}")

    # ---------------- Regras do TOM ----------------
    def _receive_transaction(self, msg: TransactionMessage, verbose: bool = True) -> None:
        self.update_clock_on_receive(msg.timestamp[0])

        if msg.tx_id not in self.records:
            self.records[msg.tx_id] = TransactionRecord(
                tx_id=msg.tx_id,
                timestamp=msg.timestamp,
                sender=msg.sender,
                operation=msg.operation,
            )
            heapq.heappush(self.pending_heap, QueueItem(msg.timestamp, msg.tx_id))
            if verbose:
                print(f"P{self.pid} RECEBE TX {msg.tx_id} ts={msg.timestamp}; entra na fila")
        else:
            if verbose:
                print(f"P{self.pid} IGNORA duplicata da TX {msg.tx_id}")

        # Ao receber a transação, confirma para todos (inclusive para si mesmo).
        ack = AckMessage(tx_id=msg.tx_id, ack_sender=self.pid)
        if verbose:
            print(f"P{self.pid} faz multicast do ACK de {msg.tx_id}")
        self.network.multicast(ack)

        self._try_deliver(verbose=verbose)

    def _receive_ack(self, msg: AckMessage, verbose: bool = True) -> None:
        # ACK não carrega timestamp original da transação; é apenas confirmação.
        self.tick_local_event()

        record = self.records.get(msg.tx_id)
        if record is None:
            # Em um sistema real, seria preciso bufferizar ACKs adiantados.
            # Como assumimos FIFO + transmissão da TX antes dos ACKs, isso não ocorre.
            if verbose:
                print(f"P{self.pid} recebeu ACK adiantado de {msg.tx_id}; ignorado pela simplificação FIFO")
            return

        record.acks.add(msg.ack_sender)
        if verbose:
            print(f"P{self.pid} RECEBE ACK de P{msg.ack_sender} para {msg.tx_id} -> {len(record.acks)}/{self.num_processes}")

        self._try_deliver(verbose=verbose)

    def _try_deliver(self, verbose: bool = True) -> None:
        """
        Entrega quantas transações forem possíveis.
        Regra: só entrega a da cabeça da fila e somente se houver ACK de todos.
        """
        progressed = True
        while progressed and self.pending_heap:
            progressed = False
            head = self.pending_heap[0]
            record = self.records[head.tx_id]

            if record.delivered:
                heapq.heappop(self.pending_heap)
                progressed = True
                continue

            if len(record.acks) == self.num_processes:
                heapq.heappop(self.pending_heap)
                old_balance = self.balance
                self.balance = record.operation.apply(self.balance)
                record.delivered = True
                entry = (
                    f"P{self.pid} ENTREGA {record.tx_id} ts={record.timestamp} "
                    f"op={record.operation.describe()} saldo: {old_balance:.2f} -> {self.balance:.2f}"
                )
                self.delivery_log.append(entry)
                if verbose:
                    print(entry)
                progressed = True

    # ---------------- Utilidades ----------------
    def queue_snapshot(self) -> List[str]:
        items = sorted((item.sort_key, item.tx_id) for item in self.pending_heap if not self.records[item.tx_id].delivered)
        return [f"{tx_id}@{ts}" for ts, tx_id in items]

    def debug_state(self) -> str:
        pending = self.queue_snapshot()
        return f"P{self.pid}: clock={self.clock}, saldo={self.balance:.2f}, fila={pending}"


def demo() -> None:
    """
    Demonstra o caso clássico dos slides:
    - saldo inicial = 1000
    - u1 = +100
    - u2 = +1%

    Sem ordenação total, as réplicas poderiam divergir (1111 vs 1110).
    Com TOM, todas executam na mesma ordem e chegam ao mesmo valor final.
    """
    net = Network()
    processes = [Process(pid=i, network=net, num_processes=3, initial_balance=1000.0) for i in (1, 2, 3)]

    # Duas transações concorrentes, iniciadas em processos diferentes.
    processes[0].submit_transaction("u1", Operation("deposit", 100.0))
    processes[1].submit_transaction("u2", Operation("interest", 0.01))

    print("\n--- INÍCIO DA EXECUÇÃO DA REDE ---")
    net.run(verbose=True)
    print("--- FIM DA EXECUÇÃO DA REDE ---\n")

    print("Estados finais:")
    for p in processes:
        print(p.debug_state())

    final_balances = {round(p.balance, 2) for p in processes}
    if len(final_balances) == 1:
        print(f"\nConsistência alcançada: todas as réplicas terminaram com saldo {final_balances.pop():.2f}")
    else:
        print("\nERRO: as réplicas divergiram!")

    print("\nLogs de entrega:")
    for p in processes:
        print(f"\nProcesso P{p.pid}:")
        for line in p.delivery_log:
            print("  " + line)


if __name__ == "__main__":
    demo()