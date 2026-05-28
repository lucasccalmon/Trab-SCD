import socket
import threading
import time

HOST = '127.0.0.1'
PORT = 3456



def iniciar_servidor():
    # Criação do socket TCP 
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Para reusar a porta imediatamente após derrubar o servidor
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    servidor.bind((HOST, PORT))
    servidor.listen() # Fila de conexões 
    
    print(f"[Servidor] Iniciado. Escutando na porta {PORT}...\n")
    
    contador_threads = 1
    
    try:
        while True:
            # O servidor fica esperando alguém conectar
            conexao, endereco = servidor.accept()
            
            # Cria e inicia uma nova thread para lidar com o cliente
            t = threading.Thread(target=atender_cliente, args=(conexao, endereco, contador_threads))
            t.start()
            
            contador_threads += 1
    except KeyboardInterrupt:
        print("\n[Servidor] Encerrando...")
    finally:
        servidor.close()
        
def atender_cliente(conexao, endereco, thread_id):
    """Função executada por cada thread individualmente."""
    print(f"[Thread-{thread_id}] Atendendo nova conexão de {endereco}")
    
    try:
        # Recebe a requisição do cliente 
        mensagem_recebida = conexao.recv(1024).decode('utf-8')
        if mensagem_recebida:
            print(f"[Thread-{thread_id}] Cliente diz: '{mensagem_recebida}'")
            
            
            time.sleep(1) 
            
            # Envia a resposta com o id da thread
            resposta = f"Serviço prestado pela thread_{thread_id}"
            conexao.sendall(resposta.encode('utf-8'))
            
    except Exception as e:
        print(f"[Thread-{thread_id}] Erro: {e}")
    finally:
        # Encerra a conexão 
        conexao.close()
        print(f"[Thread-{thread_id}] Conexão encerrada.\n")

if __name__ == "__main__":
    iniciar_servidor()