import threading
import time
import random

# Configurações iniciais
N = 5  # Tamanho do buffer
NUM_PRODUTORES = 2
NUM_CONSUMIDORES = 2

# Variáveis compartilhadas
buffer = [0] * N
in_index = 0
out_index = 0

# Inicialização dos Semáforos
# threading.Semaphore(valor_inicial)
mutex = threading.Semaphore(1)  # Exclusão mútua (1 = liberado)
empty = threading.Semaphore(N)  # Conta espaços vazios (começa com N)
full = threading.Semaphore(0)   # Conta itens produzidos (começa com 0)

def producer(id):
    global in_index
    while True:
        item = random.randint(1, 100) # Produz um novo recurso
        
        empty.acquire() # wait(empty) - Espera por espaço vazio
        mutex.acquire() # wait(mutex) - Bloqueia o acesso ao buffer
        
        # --- REGIÃO CRÍTICA ---
        buffer[in_index] = item
        print(f"Produtor {id}: Produziu e inseriu {item} na posicao {in_index}")
        in_index = (in_index + 1) % N
        # ----------------------
        
        mutex.release() # signal(mutex) - Libera o acesso ao buffer
        full.release()  # signal(full)  - Sinaliza que há um novo item
        
        time.sleep(random.uniform(0.5, 1.5)) # Simula tempo de produção

def consumer(id):
    global out_index
    while True:
        full.acquire()  # wait(full)  - Espera por um item disponível
        mutex.acquire() # wait(mutex) - Bloqueia o acesso ao buffer
        
        # --- REGIÃO CRÍTICA ---
        item = buffer[out_index]
        print(f"Consumidor {id}: Removeu e consumiu {item} da posicao {out_index}")
        out_index = (out_index + 1) % N
        # ----------------------
        
        mutex.release() # signal(mutex) - Libera o acesso ao buffer
        empty.release() # signal(empty) - Sinaliza que abriu um espaço
        
        time.sleep(random.uniform(1.0, 2.0)) # Simula tempo de consumo

if __name__ == "__main__":
    produtores = []
    consumidores = []

    # Cria e inicia as threads produtoras
    for i in range(NUM_PRODUTORES):
        t = threading.Thread(target=producer, args=(i+1,))
        produtores.append(t)
        t.start()

    # Cria e inicia as threads consumidoras
    for i in range(NUM_CONSUMIDORES):
        t = threading.Thread(target=consumer, args=(i+1,))
        consumidores.append(t)
        t.start()

    # Aguarda a finalização das threads (como é um while True, roda indefinidamente)
    try:
        for t in produtores + consumidores:
            t.join()
    except KeyboardInterrupt:
        print("\nExecução interrompida pelo usuário.")