import socket
import sys
import time
from datetime import datetime

F = 10
HOST = '127.0.0.1'
PORT = 5000

def formatar_msg(tipo, pid):
    msg = f"{tipo}|{pid}|"
    return msg.ljust(F, '0')[:F].encode('utf-8')

def parse_msg(msg_bytes):
    partes = msg_bytes.decode('utf-8').split('|')
    return int(partes[0]), int(partes[1])

def main():
    if len(sys.argv) != 4:
        print("Uso: python processo.py <PID> <r_repeticoes> <k_segundos>")
        sys.exit(1)
        
    pid = int(sys.argv[1])
    r = int(sys.argv[2])
    k = float(sys.argv[3])
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((HOST, PORT))
    except Exception as e:
        print(f"Processo {pid}: Falha ao conectar. {e}")
        sys.exit(1)
        
    for _ in range(r):
        # 1. Solicita Região Crítica (REQUEST)
        s.sendall(formatar_msg(1, pid))
        
        # 2. Fica bloqueado até receber permissão (GRANT)
        data = s.recv(F)
        tipo, _ = parse_msg(data)
        
        if tipo == 2:
            # 3. Entrou na Região Crítica
            agora = datetime.now()
            hora_formatada = agora.strftime("%H:%M:%S.") + f"{agora.microsecond // 1000:03d}"
            
            with open("resultado.txt", "a") as f:
                f.write(f"PID: {pid:02d} | Hora: {hora_formatada}\n")
            
            # Simula processamento
            time.sleep(k)
            
            # 4. Sai da Região Crítica (RELEASE)
            s.sendall(formatar_msg(3, pid))
            
    s.close()

if __name__ == "__main__":
    main()