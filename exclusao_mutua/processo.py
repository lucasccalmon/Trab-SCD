import socket
import sys
import time
import random
from datetime import datetime

# CONSTANTES DE COMUNICAÇÃO
F = 10              # Tamanho da mensagem (10 bytes)
HOST = '127.0.0.1'  # localhost
PORT = 5000         # Porta TCP


def formatar_msg(tipo, pid):
    """
    Prepara a mensagem pra enviar na rede. 
    Coloca no formato 'TIPO|PID|' e preenche com zeros no final até dar 10 caracteres certinho.
    Exemplo de saída: '1|3|000000'
    """
    msg = f"{tipo}|{pid}|"
    return msg.ljust(F, '0')[:F].encode('utf-8')


def parse_msg(msg_bytes):
    """
    Quando recebe dados da rede, essa função corta o texto no '|' 
    para descobrir de qual tipo é a mensagem e quem enviou (PID).
    """
    partes = msg_bytes.decode('utf-8').split('|')
    return int(partes[0]), int(partes[1])


def main():
    # aviso de erro
    if len(sys.argv) != 4:
        print("Erro de sintaxe! Uso correto: python processo.py <PID> <r_repeticoes> <k_segundos>")
        sys.exit(1)
        
   # Pega os números que o usuário digitou no terminal
    pid = int(sys.argv[1]) # Quem eu sou (Identificador)
    r = int(sys.argv[2])   # Quantas vezes vou tentar entrar na Região Crítica (r)
    k = float(sys.argv[3]) # Quantos segundos vou ficar lá dentro "trabalhando" (k)
    
    # Configuração inicial do socket cliente TCP pra ligar pro Coordenador
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
        # Espera Aleatória
        # ------------------------------------------------------------
        # Dá um sleep de 0.1 a 2 segundos pra simular um trabalho qualquer do processo.
        tempo_local = random.uniform(0.1, 2.0)
        time.sleep(tempo_local)
        # ------------------------------------------------------------
        # PASSO 1: Enviar pedido de acesso (REQUEST) 
        # ------------------------------------------------------------
        s.sendall(formatar_msg(1, pid))
        
        # ------------------------------------------------------------
        # PASSO 2: Bloqueio (Aguardando o GRANT) 
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
            with open("resultado2.txt", "a") as f:
                f.write(f"PID: {pid:02d} | Hora: {hora_formatada}\n")
            
            # Simulação de processamento  dentro da Região Crítica 
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