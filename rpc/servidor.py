from xmlrpc.server import SimpleXMLRPCServer

# Define as funções da calculadora
def add(x, y):
    print(f"[Servidor] Somando {x} + {y}")
    return x + y

def subtract(x, y):
    print(f"[Servidor] Subtraindo {x} - {y}")
    return x - y

def multiply(x, y):
    print(f"[Servidor] Multiplicando {x} * {y}")
    return x * y

def divide(x, y):
    print(f"[Servidor] Dividindo {x} / {y}")
    if y == 0:
        return "Erro: Divisão por zero não permitida!"
    return x / y

# Configura o servidor para rodar localmente na porta 8000
server = SimpleXMLRPCServer(("localhost", 8000))
print("Servidor RPC da Calculadora rodando na porta 8000...")

# Registra as funções para que o cliente possa acessá-las
server.register_function(add, "add")
server.register_function(subtract, "subtract")
server.register_function(multiply, "multiply")
server.register_function(divide, "divide")

# Mantém o servidor rodando infinitamente
try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\nServidor encerrado.")