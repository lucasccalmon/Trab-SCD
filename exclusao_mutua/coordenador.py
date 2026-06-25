import socket
import threading
import queue
import sys
import os
import time
from datetime import datetime

# CONSTANTES DO SISTEMA
F = 10         #  tamanho da mensagem (10 bytes). 
HOST = '127.0.0.1'  # localhost
PORT = 5000         # porta TCP utilizada para escutar as conexões dos processos.

# VARIÁVEIS COMPARTILHADAS (ZONA CRÍTICA)
request_queue = []   # fila convencional (FIFO) que armazena a ordem dos PIDs que aguardam a Região Crítica.
counts = {}          # dicionário pra anotar quantas vezes cada processo usou a Região Crítica.
sockets_map = {}     # mapeamento de {PID: objeto_socket} para saber por qual canal responder a cada processo.
current_in_cs = None # guarda quem está usando a Região Crítica AGORA. Se for None, tá livre.

# MECANISMO DE SINCRONIZAÇÃO INTERNA (lock)
# impede que o menu do terminal e o algoritmo mexam na fila ao mesmo tempo e quebrem o código.
lock = threading.Lock()

# FILA DE EVENTOS THREAD-SAFE
# utilizada para canalizar todas as mensagens recebidas pelas threads de escuta em um único fluxo ordenado.
event_queue = queue.Queue()


def formatar_msg(tipo, pid):
    """
    Prepara a mensagem pra enviar na rede. 
    Coloca no formato 'TIPO|PID|' e preenche com zeros no final até dar 10 caracteres certinho.
    Exemplo de saída: '1|3|000000'
    """
    msg = f"{tipo}|{pid}|"
    # ljust preenche com '0' à direita até o tamanho F.
    return msg.ljust(F, '0')[:F].encode('utf-8')


def parse_msg(msg_bytes):
    """
    Quando recebe dados da rede, essa função corta o texto no '|' 
    para descobrir de qual tipo é a mensagem e quem enviou (PID).
    """
    try:
        partes = msg_bytes.decode('utf-8').split('|')
        return int(partes[0]), int(partes[1])
    except (ValueError, IndexError):
        return None, None


def escrever_log(tipo, pid, direcao):
    """
    Grava o histórico de mensagens enviadas e recebidas em um arquivo de log local.
    Salva a hora exata e se a mensagem foi enviada ou recebida.
    """
    tipos = {1: "REQUEST", 2: "GRANT", 3: "RELEASE"}
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_line = f"[LOG] {agora} | {direcao} | {tipos[tipo]} | Processo {pid}\n"
    
    # Abertura em modo 'append' (a) para não sobrescrever os registros anteriores
    with open("coordenador_log2.txt", "a") as f:
        f.write(log_line)


def client_handler(conn):
    """
    THREAD DE ESCUTA INDIVIDUAL: para cada processo que conecta, criamos um 'escutador' desses.
    Ele fica travado aqui esperando chegar uma mensagem. Quando chega, 
    ele simplesmente joga ela na 'event_queue' pro cérebro processar.
    """
    while True:
        try:
            # Aguarda e lê exatamente F bytes do buffer do socket 
            data = conn.recv(F)
            if not data:
                # Se receber dados vazios, o cliente encerrou a conexão abruptamente
                break
            
            tipo, pid = parse_msg(data)
            if tipo is not None:
                # Posta o evento na fila thread-safe para ser processado de forma centralizada
                event_queue.put((tipo, pid, conn))
        except ConnectionResetError:
            break


def thread_conexoes(server_socket):
    """
    THREAD 1: GERENCIADORA DE CONEXÕES.
    Responsável por executar o 'accept()' e receber novos nós no sistema.  
    Quando alguém conecta, ela contrata um client_handler pra cuidar do cliente.
    """
    while True:
        try:
            # Aguarda a tentativa de conexão de um script de processo
            conn, _ = server_socket.accept()
            # Dispara uma thread de escuta dedicada para o novo cliente conectado
            threading.Thread(target=client_handler, args=(conn,), daemon=True).start()
        except socket.error:
            break


def thread_algoritmo():
    """
    THREAD 2: NÚCLEO DO ALGORITMO DE EXCLUSÃO MÚTUA.
   Lê a fila de mensagens uma por uma, de forma isolada, evitando condições de corrida.
    """
    global current_in_cs
    while True:
        # Fica bloqueado aqui até que qualquer thread de escuta insira uma mensagem recebida
        tipo, pid, conn = event_queue.get()
        
        # Sincroniza o registro inicial do processo nas tabelas do Coordenador
        with lock:
            if pid not in sockets_map:
                sockets_map[pid] = conn
            if pid not in counts:
                counts[pid] = 0
                
        # Registra no arquivo de texto que uma mensagem foi recebida 
        escrever_log(tipo, pid, "RECEBIDO de")

        # Região lógica de decisão do algoritmo centralizado
        with lock:
            if tipo == 1:  # MENSAGEM DO TIPO: REQUEST 
                if current_in_cs is None:
                    # Se a Região Crítica está livre, o processo requisitante ganha o acesso imediatamente
                    current_in_cs = pid
                    # Envia a mensagem GRANT de volta para o socket do processo correspondente 
                    sockets_map[pid].sendall(formatar_msg(2, pid))
                    escrever_log(2, pid, "ENVIADO para")
                else:
                    # Se já houver alguém na Região Crítica, coloca o PID no fim da fila de espera 
                    request_queue.append(pid)
            
            elif tipo == 3:  # MENSAGEM DO TIPO: RELEASE 
                # Incrementa o contador de atendimentos concluídos do processo que está saindo
                counts[pid] += 1
                
                # Verifica se há processos aguardando na fila
                if request_queue:
                    # Remove o primeiro processo da fila (FIFO) 
                    next_pid = request_queue.pop(0)
                    current_in_cs = next_pid
                    # Concede o acesso ao próximo da fila enviando o GRANT 
                    sockets_map[next_pid].sendall(formatar_msg(2, next_pid))
                    escrever_log(2, next_pid, "ENVIADO para")
                else:
                    # Se a fila estiver vazia, a Região Crítica fica totalmente liberada
                    current_in_cs = None


def thread_interface():
    """
    THREAD 3: INTERFACE DE TERMINAL.
    Gerencia a interação com o usuário operador através de comandos numéricos, sem congelar a rede.
    """
    print("Coordenador iniciado com sucesso.")
    print("Comandos disponíveis: [1] Imprimir Fila | [2] Ver Atendimentos | [3] Encerrar Sistema")
    while True:
        # Chamada bloqueante aguardando o input no terminal 
        cmd = input()
        if cmd == '1':
            with lock:
                # Exibe o estado instantâneo da fila de espera por recursos 
                print(f"-> Fila de pedidos atual: {request_queue}")
        elif cmd == '2':
            with lock:
                # Exibe o relatório de quantas vezes cada nó utilizou a região crítica 
                print("-> Quantidade de atendimentos por processo:")
                for p, c in counts.items():
                    print(f"   Processo {p}: {c} vezes")
        elif cmd == '3':
            print("Finalizando todas as atividades do coordenador...")
            # Força a terminação imediata do processo principal e de todas as threads da aplicação 
            os._exit(0)
        else:
            print("Comando inválido. Digite apenas 1, 2 ou 3.")


if __name__ == "__main__":
    # Inicialização: limpa ou cria o arquivo de log para uma nova execução limpa 
    open("coordenador_log2.txt", "w").close()
    
    # Criação do socket principal TCP (SOCK_STREAM) utilizando IPV4 (AF_INET)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Vincula o socket ao endereço e porta definidos
    server.bind((HOST, PORT))
    # Coloca o socket em modo de escuta para conexões de entrada
    server.listen()
    
    # Inicializa a Thread 1 (Conexões) configurada como daemon (fecha automaticamente se o script fechar) 
    threading.Thread(target=thread_conexoes, args=(server,), daemon=True).start()
    
    # Inicializa a Thread 2 (Lógica do Algoritmo Centralizado) 
    threading.Thread(target=thread_algoritmo, daemon=True).start()
    
    # Executa a Thread 3 (Interface de Usuário) na linha de execução principal do programa 
    thread_interface()