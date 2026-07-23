#!/usr/bin/env python3
import json
import random
import time
import datetime
import subprocess

# Configurações do simulador
TOPIC = "vendas_medallion_stream"
CONTAINER_NAME = "kafka"
DELAY_SECONDS = 2  # Intervalo entre envios (em segundos)

PRODUTOS = [
    ("Webcam Full HD", "Perifericos", 350.00),
    ("Teclado Mecanico", "Perifericos", 280.00),
    ("Mouse Sem Fio", "Perifericos", 150.00),
    ("Monitor 27 IPS", "Monitores", 1400.00),
    ("Suporte Articulado", "Acessorios", 120.00),
    ("Cadeira Ergonomica", "Moveis", 950.00),
    ("Headset Gamer 7.1", "Audio", 420.00),
    ("Cabo HDMI 2.1 2m", "Acessorios", 45.00),
]

CANAIS = ["E-commerce", "Loja_Fisica", "App_Mobile", "Marketplace"]

def gerar_evento_venda(id_seq):
    prod_nome, categoria, valor_base = random.choice(PRODUTOS)
    qtd = random.randint(1, 3)
    cliente_id = f"CLI-{random.randint(100, 999)}"
    canal = random.choice(CANAIS)
    
    # Gera variação pequena no preço
    valor_final = round(valor_base * random.uniform(0.95, 1.05), 2)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "id_venda": f"VND-{id_seq:05d}",
        "data_venda": now_str,
        "cliente_id": cliente_id,
        "produto": prod_nome,
        "categoria": categoria,
        "valor": valor_final,
        "quantidade": qtd,
        "canal_venda": canal
    }

def enviar_para_kafka(evento_json):
    # Envia via kafka-console-producer.sh dentro do container Docker
    cmd = [
        "docker", "exec", "-i", CONTAINER_NAME,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", "kafka:9092",
        "--topic", TOPIC
    ]
    
    mensagem = json.dumps(evento_json) + "\n"
    
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input=mensagem)
    
    if process.returncode != 0:
        print(f"❌ Erro ao enviar evento: {stderr}")
    else:
        print(f"✉️  [KAFKA STREAM] Evento enviado: {evento_json['id_venda']} - {evento_json['produto']} (R$ {evento_json['valor']}) via {evento_json['canal_venda']}")

def main():
    print("🚀 Iniciando Simulador Continuo do Kafka...")
    print(f"📌 Tópico: {TOPIC} | Intervalo: {DELAY_SECONDS}s")
    print("Pressione CTRL+C para parar.\n")
    
    seq = 3000
    try:
        while True:
            seq += 1
            evento = gerar_evento_venda(seq)
            enviar_para_kafka(evento)
            time.sleep(DELAY_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Simulador finalizado pelo usuario.")

if __name__ == "__main__":
    main()
