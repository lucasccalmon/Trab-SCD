import socket
import threading
import queue
import sys
import os
from datetime import datetime

F = 10  # Tamanho fixo em bytes
HOST = '127.0.0.1'
PORT = 5000

# Estruturas de dados compartilhadas
request_queue = []
counts = {}
sockets_map = {}
current_in_cs = None
lock = threading.Lock()

# Fila thread-safe para o event-loop do algoritmo
event_queue = queue.Queue()

def formatar_msg(tipo, pid):
    msg = f"{tipo}|{pid}|"
    return msg.ljust(F, '0')[:F].encode('utf-8')

def parse_msg(msg_bytes):
    try:
        partes = msg_bytes.decode('utf-8').split('|')
        return int(partes[0]), int(partes[1])
    except:
        return None, None

def escrever_log(tipo, pid, direcao):
    tipos = {1: "REQUEST", 2: "GRANT", 3: "RELEASE"}
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_line = f"[LOG] {agora} | {direcao} | {tipos[tipo]} | Processo {pid}\n"
    with open("coordenador_log.txt", "a") as f:
        f.write(log_line)

def client_handler(conn):
    """Thread que escuta mensagens de um processo específico."""
    while True:
        try:
            data = conn.recv(F)
            if not data:
                break
            tipo, pid = parse_msg(data)
            if tipo is not None:
                # Envia o evento para a Thread do Algoritmo
                event_queue.put((tipo, pid, conn))
        except:
            break

def thread_conexoes(server_socket):
    """Thread 1: Apenas recebe conexões."""
    while True:
        try:
            conn, _ = server_socket.accept()
            threading.Thread(target=client_handler, args=(conn,), daemon=True).start()
        except:
            break

def thread_algoritmo():
    """Thread 2: Executa o algoritmo centralizado de exclusão mútua."""
    global current_in_cs
    while True:
        tipo, pid, conn = event_queue.get()
        
        with lock:
            if pid not in sockets_map:
                sockets_map[pid] = conn
            if pid not in counts:
                counts[pid] = 0
                
        escrever_log(tipo, pid, "RECEBIDO de")

        with lock:
            if tipo == 1:  # REQUEST
                if current_in_cs is None:
                    current_in_cs = pid
                    sockets_map[pid].sendall(formatar_msg(2, pid))
                    escrever_log(2, pid, "ENVIADO para")
                else:
                    request_queue.append(pid)
            
            elif tipo == 3:  # RELEASE
                counts[pid] += 1
                if request_queue:
                    next_pid = request_queue.pop(0)
                    current_in_cs = next_pid
                    sockets_map[next_pid].sendall(formatar_msg(2, next_pid))
                    escrever_log(2, next_pid, "ENVIADO para")
                else:
                    current_in_cs = None

def thread_interface():
    """Thread 3: Interface do terminal bloqueada aguardando comandos."""
    print("Coordenador iniciado. Comandos: [1] Ver Fila | [2] Ver Contagens | [3] Encerrar")
    while True:
        cmd = input()
        if cmd == '1':
            with lock:
                print(f"-> Fila de pedidos atual: {request_queue}")
        elif cmd == '2':
            with lock:
                print("-> Quantidade de atendimentos por processo:")
                for p, c in counts.items():
                    print(f"   Processo {p}: {c} vezes")
        elif cmd == '3':
            print("Encerrando coordenador...")
            os._exit(0)
        else:
            print("Comando inválido.")

if __name__ == "__main__":
    # Limpa log anterior, se houver
    open("coordenador_log.txt", "w").close()
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()
    
    threading.Thread(target=thread_conexoes, args=(server,), daemon=True).start()
    threading.Thread(target=thread_algoritmo, daemon=True).start()
    
    # A interface roda na thread principal
    thread_interface()