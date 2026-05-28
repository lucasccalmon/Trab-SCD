import subprocess
import sys

if len(sys.argv) != 4:
    print("Uso: python iniciar.py <n_processos> <r_repeticoes> <k_segundos>")
    sys.exit(1)

n = int(sys.argv[1])
r = int(sys.argv[2])
k = float(sys.argv[3])

# Limpa o arquivo de resultados de testes anteriores
open("resultado.txt", "w").close()

processos = []

print(f"Iniciando {n} processos (r={r}, k={k}s)...")

# Inicia todos os processos
for i in range(1, n + 1):
    p = subprocess.Popen(["python", "processo.py", str(i), str(r), str(k)])
    processos.append(p)

# Aguarda a finalização de todos
for p in processos:
    p.wait()

print(f"\nTeste finalizado. Verifique o arquivo 'resultado.txt' e 'coordenador_log.txt'.")