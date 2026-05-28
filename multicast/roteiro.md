# Roteiro de vídeo (5 minutos) — Totally Ordered Multicast em Python

## 1. Abertura (0:00–0:30)
"Neste vídeo eu vou mostrar uma implementação em Python do protocolo **Totally Ordered Multicast**, usado em sistemas distribuídos para garantir que todos os processos executem as transações exatamente na **mesma ordem**. A ideia é evitar inconsistências entre réplicas, mesmo quando eventos acontecem de forma concorrente."

## 2. Problema que o protocolo resolve (0:30–1:05)
"Imagina duas réplicas de banco com saldo inicial de 1000. Uma transação soma 100 reais, e outra aplica juros de 1%. Se uma réplica fizer primeiro o depósito e depois os juros, o saldo final fica 1111. Mas se outra fizer primeiro os juros e depois o depósito, fica 1110. O valor em si até poderia variar dependendo da ordem, mas o importante em um sistema replicado é que **todas as réplicas concordem na mesma ordem**."

## 3. Ideia central do algoritmo (1:05–1:55)
"O protocolo funciona assim: cada processo mantém um **relógio lógico de Lamport**. Quando ele cria uma transação local, ele associa a ela um timestamp no formato **(tempo_lógico, id_do_processo)**. O ID do processo entra para desempatar casos em que dois processos geram o mesmo tempo lógico.

Cada processo mantém também uma **fila local de transações**, ordenada por esse timestamp. Quando uma transação chega, ela entra na fila, e o processo envia uma **confirmação**, ou ACK, para todos os outros processos. A transação só pode ser entregue quando estiver na cabeça da fila e quando houver confirmações de todos os processos do sistema."

## 5. Simplificações adotadas (3:20–4:00)
"Como a proposta aqui é didática, eu fiz algumas simplificações. A rede foi simulada em memória, então eu não usei sockets. Também assumi exatamente o que os slides assumem: **rede FIFO e sem perda de mensagens**. Isso reduz complexidade e deixa o foco no protocolo. Em um sistema real, seria necessário tratar falhas, timeouts, retransmissões e possivelmente ACKs chegando antes da transação."


## 4. Estrutura do código (1:55–3:20)
"No código, eu separei a implementação em algumas partes.

A classe **Operation** representa a operação replicada. Para simplificar, usei duas operações: depósito e juros.

A classe **TransactionMessage** representa a mensagem da transação em si, contendo ID, timestamp, remetente e operação. Já a classe **AckMessage** representa a confirmação.

A classe **Process** é o núcleo do protocolo. Cada processo tem:
- um relógio lógico,
- uma fila de transações pendentes,
- uma tabela com os registros das transações,
- e um saldo local, só para demonstrar a replicação.

Quando o processo chama `submit_transaction`, ele incrementa o relógio, cria o timestamp e faz multicast da transação.

Quando recebe uma transação em `_receive_transaction`, ele atualiza o relógio de Lamport, coloca a transação na fila e faz multicast do ACK.

Quando recebe um ACK em `_receive_ack`, ele registra qual processo confirmou aquela transação.

Depois disso, o método `_try_deliver` verifica se a transação que está na cabeça da fila já foi confirmada por todos. Se sim, ela é executada."


## 6. Demonstração do funcionamento (4:00–4:40)
"Na função `demo`, eu crio três processos. O processo 1 gera a transação `u1`, que deposita 100. O processo 2 gera a transação `u2`, que aplica 1% de juros. Essas duas transações são concorrentes. Mesmo assim, todos os processos acabam montando a mesma fila global, aguardam todas as confirmações e entregam as operações na mesma ordem total. No final, todos os saldos ficam iguais."

## 1. O Conflito e o Desempate (O topo do log)
Onde apontar: Nas duas primeiras linhas (P1 CRIA u1 com timestamp (1, 1)... e P2 CRIA u2 com timestamp (1, 2)...).
O que explicar: Destaque que as duas transações nasceram praticamente juntas no tempo lógico 1. Aqui você mostra a regra de desempate do algoritmo na prática: como o ID do Processo 1 é menor que o do Processo 2, a transação u1 ganha prioridade (1, 1) < (1, 2) e vai assumir o topo da fila em todos os processos.

## 2. O Enfileiramento e o "Spam" de Mensagens (O meio do log)
Onde apontar: No bloco gigante de RECEBE TX e faz multicast do ACK.
O que explicar: Você não precisa ler linha por linha. Apenas ressalte o comportamento da rede: mostre que assim que as réplicas recebem as transações, elas as colocam na fila e imediatamente disparam a chuva de mensagens de confirmação (ACKs) para todo mundo, inclusive para si mesmas. Isso ilustra bem a alta complexidade de mensagens do protocolo que você menciona na conclusão.

## 3. A Entrega Segura (A Mágica Acontecendo)
Onde apontar: Nas linhas RECEBE ACK de P3 para u1 -> 3/3 seguidas imediatamente de ENTREGA u1....
O que explicar: Esse é o clímax do protocolo. Mostre que a transação u1 só foi executada e alterou o saldo (de 1000.00 para 1100.00) no exato momento em que o contador de ACKs atingiu 3/3 (ou seja, todos os processos confirmaram o recebimento).
Complemento: Logo abaixo, mostre que, com a u1 saindo da frente, a transação u2 (que estava travada esperando sua vez) assume o topo da fila, atinge 3/3 de confirmações e finalmente aplica os juros (de 1100.00 para 1111.00).

## 4. O Estado Final (A Prova da Consistência)
Onde apontar: No bloco final em "Estados finais:" e "Logs de entrega:".
O que explicar: Conclua mostrando que as filas de todos os processos voltaram a ficar vazias fila=[] e que a ordem exata de execução (primeiro depósito, depois juros) foi rigorosamente respeitada nos logs de P1, P2 e P3. Graças a isso, a consistência foi alcançada e ninguém divergiu do saldo final de 1111.00.

## 7. Fechamento (4:40–5:00)
"Então, resumindo: o Totally Ordered Multicast garante consistência entre réplicas ao impor uma ordem total distribuída, sem usar servidor central. O custo disso é um número alto de mensagens e atraso na entrega, porque cada transação só pode ser executada depois das confirmações de todos."