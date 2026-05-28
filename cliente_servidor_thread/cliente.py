import socket
import threading


HOST = '127.0.0.1'
PORT = 3456

def realizar_requisicao(id_cliente):
    """Função que simula o comportamento de um único cliente"""
    # Cria o socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            # Conecta ao servidor
            s.connect((HOST, PORT))
            
            # Prepara a mensagem 
            mensagem = f"Por favor, poderia prestar-me um servico? (Sou o cliente {id_cliente})"
            print(f"[Cliente {id_cliente}] Conectado. Enviando requisição...")
            s.sendall(mensagem.encode('utf-8'))
            
            # Aguarda e exibe a resposta
            resposta = s.recv(1024).decode('utf-8')
            print(f"[Cliente {id_cliente}] Recebeu: '{resposta}'\n")
            
        except ConnectionRefusedError:
            print(f"[Cliente {id_cliente}] Erro no connect: O servidor está rodando?")
        except Exception as e:
            print(f"[Cliente {id_cliente}] Erro inesperado: {e}")

if __name__ == "__main__":
    print("\n[Cliente] Processo cliente comecou a rodar.")
    print(f"[Cliente] Usando o servidor {HOST} e a porta {PORT}.\n")
    
    threads_clientes = []
    
    # Cria e inicia 4 threads simultâneas para testar o servidor
    for i in range(1, 5):
        t = threading.Thread(target=realizar_requisicao, args=(i,))
        threads_clientes.append(t)
        t.start()
        
    # Aguarda todas as conexões terminarem antes de fechar o script
    for t in threads_clientes:
        t.join()
        
    print("[Cliente] Fim da execução.")