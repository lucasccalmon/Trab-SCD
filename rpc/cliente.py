import xmlrpc.client

# Conecta ao servidor RPC
proxy = xmlrpc.client.ServerProxy("http://localhost:8000/")

print("--- Cliente RPC Calculadora Interativa ---")
print("Digite 'sair' na operação para encerrar o programa.\n")

while True:
    operacao = input("Escolha a operação (+, -, *, /): ")
    
    if operacao.lower() == 'sair':
        print("Encerrando o cliente...")
        break
        
    if operacao not in ['+', '-', '*', '/']:
        print("Operação inválida. Tente novamente.\n")
        continue

    try:
        # Lê os números digitados pelo usuário 
        x = float(input("Digite o primeiro número: "))
        y = float(input("Digite o segundo número: "))
        
        # Faz a chamada RPC baseada na escolha do usuário
        print("Enviando requisição ao servidor...")
        if operacao == '+':
            resultado = proxy.add(x, y)
        elif operacao == '-':
            resultado = proxy.subtract(x, y)
        elif operacao == '*':
            resultado = proxy.multiply(x, y)
        elif operacao == '/':
            resultado = proxy.divide(x, y)
            
        print(f">>> RESULTADO: {x} {operacao} {y} = {resultado}\n")

    except ValueError:
        print("Erro: Por favor, digite apenas números válidos.\n")
    except ConnectionRefusedError:
        print("Erro de Conexão: O servidor está rodando?\n")
        break