import socket
import sys
import time
from datetime import datetime

# CONSTANTES DE COMUNICAÇÃO
F = 10              # Alinhamento obrigatório de mensagens em 10 bytes.
HOST = '127.0.0.1'   # IP do coordenador centralizador.
PORT = 5000         # Porta do coordenador centralizador.


def formatar_msg(tipo, pid):
    """
    Garante que as mensagens do processo tenham o mesmo tamanho padrão do Coordenador (F bytes).
    """
    msg = f"{tipo}|{pid}|"
    return msg.ljust(F, '0')[:F].encode('utf-8')


def parse_msg(msg_bytes):
    """
    Interpreta os comandos de resposta vindos do Coordenador da rede.
    """
    partes = msg_bytes.decode('utf-8').split('|')
    return int(partes[0]), int(partes[1])


def main():
    # Validação dos argumentos passados via prompt de comando/terminal 
    if len(sys.argv) != 4:
        print("Erro de sintaxe! Uso correto: python processo.py <PID> <r_repeticoes> <k_segundos>")
        sys.exit(1)
        
    # Atribuição dos parâmetros informados pelo usuário/script de carga 
    pid = int(sys.argv[1]) # Identificador único do processo 
    r = int(sys.argv[2])   # Quantidade total de vezes que este nó tentará entrar na Região Crítica 
    k = float(sys.argv[3]) # Tempo em segundos de permanência simulada na Região Crítica 
    
    # Configuração inicial do socket cliente TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Estabelece conexão direta com o coordenador central 
        s.connect((HOST, PORT))
    except Exception as e:
        print(f"Processo {pid}: Não foi possível conectar ao coordenador. Erro: {e}")
        sys.exit(1)
        
    # LOOP PRINCIPAL DE REPETIÇÕES (Executa r vezes) 
    for _ in range(r):
        # ------------------------------------------------------------
        # PASSO 1: Enviar pedido de acesso (REQUEST) 
        # ------------------------------------------------------------
        s.sendall(formatar_msg(1, pid))
        
        # ------------------------------------------------------------
        # PASSO 2: Bloqueio de Sincronismo (Aguardando o GRANT) 
        # ------------------------------------------------------------
        # O fluxo do programa congela nesta linha até que o Coordenador decida enviar F bytes
        data = s.recv(F)
        tipo, _ = parse_msg(data)
        
        # Se o tipo da mensagem recebida for igual a 2 (GRANT), a entrada é liberada 
        if tipo == 2:
            # ------------------------------------------------------------
            # PASSO 3: ENTRADA NA REGIÃO CRÍTICA 
            # ------------------------------------------------------------
            # Coleta o horário corrente do sistema operacional
            agora = datetime.now()
            # Formata a hora incluindo estritamente os milissegundos correspondentes 
            hora_formatada = agora.strftime("%H:%M:%S.") + f"{agora.microsecond // 1000:03d}"
            
            # Operação de I/O Segura: Abre o arquivo compartilhado em modo Append ('a') 
            # Múltiplos processos rodam na mesma máquina escrevendo no mesmo arquivo físico.
            with open("resultado.txt", "a") as f:
                f.write(f"PID: {pid:02d} | Hora: {hora_formatada}\n")
            
            # Simulação de processamento pesado dentro da Região Crítica 
            # Mantém o trinco sobre o recurso exclusivo durante k segundos antes de liberar 
            time.sleep(k)
            
            # ------------------------------------------------------------
            # PASSO 4: SAÍDA DA REGIÃO CRÍTICA (RELEASE) 
            # ------------------------------------------------------------
            # Avisa formalmente ao coordenador que liberou o recurso para o próximo da fila 
            s.sendall(formatar_msg(3, pid))
            
    # Finalizadas todas as r repetições, fecha o canal de comunicação de rede de forma limpa 
    s.close()


if __name__ == "__main__":
    main()