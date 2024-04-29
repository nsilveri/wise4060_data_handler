import paho.mqtt.client as mqtt
import json
from collections import deque
import time
import psycopg2
from psycopg2 import sql
import logging

# Configurazioni MQTT
mqtt_broker_url = 'SPH-SERVER-PRODUZIONE' # 'localhost' #
mqtt_broker_port = 1883
mqtt_topic_prefix = "Advantech"
mqtt_topic_target = "data"
mqtt_topic_target_filtered = "data_filtered"

mqtt_topic = f"{mqtt_topic_prefix}/+/{mqtt_topic_target}"

# Dizionario per mantenere le code di 20 elementi per ogni MAC address
queues = {}
blinking_queue = {}
second_queues = {}
last_payloads = {}

# Configurazioni del database PostgreSQL
db_config = {
    'host': 'SPH-SERVER-PRODUZIONE',
    'port': '5432',
    'user': 'postgres',
    'password': 'SphDbProduzione'
}

#docker run --name my-postgres-container -e POSTGRES_PASSWORD=sphdbpasswd -d -p 5432:5432 -v /my/own/datadir:/var/lib/postgresql/data postgres
#docker run --name my-postgres-container -e POSTGRES_PASSWORD=sphdbpasswd -d -p 5432:5432 -v /path/to/postgres_data:/var/lib/postgresql/data postgres
#docker run --name my-postgres-container -e POSTGRES_PASSWORD=sphdbpasswd -d -p 5432:5432 -v /mnt/c/Users/YourUsername/Desktop/postgres_data:/var/lib/postgresql/data postgres
#docker run --name my-postgres-container -e POSTGRES_PASSWORD=sphdbpasswd -d -p 5432:5432 -v /mnt/c/Users/SPH/Desktop/postgres_data:/var/lib/postgresql/data postgres

# Funzione per connettersi al database e creare un nuovo database per il macAddress se non esiste già
def connect_and_create_db(mac_address, device_name):
    logging.info(f"Tentativo di connessione e creazione del database per il dispositivo con MAC address '{mac_address}'")

    try:
        # Connessione al database principale
        conn_main = psycopg2.connect(**db_config)
        cur_main = conn_main.cursor()

        # Verifica se il database esiste già
        cur_main.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", (mac_address,))
        if not cur_main.fetchone():
            # Il database non esiste, quindi lo creiamo
            conn_main.set_isolation_level(0)  # Imposta il livello di isolamento su autocommit
            new_db_name = f"{device_name}_{mac_address}"
            cur_main.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
            logging.info(f"Database '{new_db_name}' creato con successo.")

        # Chiudiamo la connessione al database principale
        conn_main.close()

        # Connessione al nuovo database appena creato
        new_db_config = db_config.copy()  # Copia il dizionario delle configurazioni
        new_db_config['database'] = new_db_name  # Imposta il nome del database appena creato
        conn = psycopg2.connect(**new_db_config)
        cur = conn.cursor()

        # Creazione della tabella se non esiste già
        create_table_query = """
            CREATE TABLE IF NOT EXISTS deviceData (
                macAddress TEXT,
                nameDevice TEXT,
                timestamp TEXT,
                creation_date TIMESTAMP,
                di1 INTEGER,
                di2 INTEGER,
                di3 INTEGER,
                di4 INTEGER,
                do1 INTEGER,
                do2 INTEGER,
                do3 INTEGER,
                do4 INTEGER,
                duration INTERVAL,
                working INTEGER
            )
        """
        cur.execute(create_table_query)

        # Esegui il commit delle modifiche e chiudi la connessione
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Connessione e creazione del database per '{new_db_name}' completate con successo.")
    except Exception as e:
        logging.error(f"Errore durante la connessione e la creazione del database per '{mac_address}': {e}")


# Funzione per inserire i dati nel database del macAddress
def insert_data(mac_address, device_name, payload):
    # Remove 'database' key from db_config
    db_name = f"{device_name}_{mac_address}"
    db_config.pop('database', None)
    
    # Connessione al database del macAddress
    try:
        conn = psycopg2.connect(database=db_name, **db_config)
        cur = conn.cursor()
        
        # Formattazione della data leggibile
        readable_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # Inserimento dei dati nella tabella "deviceData"
        insert_query = """
            INSERT INTO deviceData (
                macAddress,
                nameDevice,
                timestamp,
                creation_date,
                di1, 
                di2, 
                di3, 
                di4, 
                do1, 
                do2, 
                do3, 
                do4,
                working,
                duration
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        cur.execute(insert_query, (
            mac_address, 
            device_name,
            time.time(), 
            readable_date,
            int(payload.get('di1', None)), 
            int(payload.get('di2', None)), 
            int(payload.get('di3', None)), 
            int(payload.get('di4', None)), 
            int(payload.get('do1', None)), 
            int(payload.get('do2', None)), 
            int(payload.get('do3', None)), 
            int(payload.get('do4', None)),
            int(working_state),
            '0 seconds'
        ))

        # Esegui il commit delle modifiche e chiudi la connessione
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Errore durante l'inserimento dei dati nel database per '{mac_address}': {e}")



# Leggi il file JSON dei nomi dei dispositivi
with open('./deviceNames.json') as f:
    device_names = json.load(f)
    print(f"Device data loaded ({device_names})...")
    

# Connessione e creazione del database per ogni MAC address
#for mac_address in device_names.keys():
#    connect_and_create_db(mac_address)

# Funzione di callback quando il client si connette al broker MQTT
def on_connect(client, userdata, flags, rc):
    print("Connesso al broker MQTT con risultato:", mqtt.connack_string(rc))
    # Sottoscrizione al topic
    client.subscribe(mqtt_topic)

# Funzione di callback quando il client riceve un messaggio MQTT
def on_message(client, userdata, msg):
    try:
        mac_address = msg.topic.split('/')[-2]
        device_name = device_names.get(mac_address, "Dispositivo sconosciuto")
        payload = json.loads(msg.payload)
        payload['t'] = time.time()

        # Calcola la differenza di tempo con il payload precedente
        time_difference = None
        if mac_address in last_payloads:
            last_payload = last_payloads[mac_address]
            if 't' in last_payload:
                last_timestamp = last_payload['t']
                time_difference = (payload['t'] - last_timestamp) * 1000  # Converte da secondi a millisecondi
        
        # Calcola le variabili modificate rispetto al payload precedente
        modified_vars = {}
        if mac_address in last_payloads:
            last_payload = last_payloads[mac_address]
            for key in ['di1', 'di2', 'di3', 'di4']:
                if key in payload and key in last_payload and last_payload[key] != payload[key]:
                    modified_vars[key] = payload[key]
        last_payloads[mac_address] = payload
        
        # Aggiungi le variabili modificate al nuovo payload
        payload['modded_val'] = modified_vars
        
        # Esegui il controllo per il "blinking"
        if time_difference is not None and time_difference < 600 and modified_vars == last_payloads.get(mac_address, {}).get('modded_val'):
            #print("Blinking! La differenza di tempo è inferiore a 600ms e le variabili modificate sono le stesse.")
            # Se la variabile modificata è False, impostala a True prima di aggiungere il payload a blinking_queue
            for key, value in modified_vars.items():
                if not value:
                    payload[key] = True
            blinking_queue[mac_address] = payload
        else:
            print("No blinking!")
            # Controlla se la blinking_queue contiene elementi
            if mac_address in blinking_queue:
                # Prendi il payload più recente dalla blinking_queue
                recent_payload = blinking_queue[mac_address]
                # Aggiungi il payload più recente alla seconda coda
                if mac_address not in second_queues:
                    second_queues[mac_address] = deque(maxlen=20)
                second_queues[mac_address].append(recent_payload)
                # Svuota la blinking_queue
                blinking_queue.pop(mac_address)
            # Aggiungi il payload alla seconda coda
            if mac_address not in blinking_queue:
                if mac_address not in second_queues:
                    second_queues[mac_address] = deque(maxlen=20)
                second_queues[mac_address].append(payload)
                
            # Publish del payload sul topic MQTT
            mqtt_topic_publish = f"{mqtt_topic_prefix}/{mac_address}/{mqtt_topic_target_filtered}"
            connect_and_create_db(mac_address, device_name)
            insert_data(mac_address, device_name, payload)
            client.publish(mqtt_topic_publish, json.dumps(payload))

        # Aggiungi il payload alla coda del MAC address corrispondente
        if mac_address not in queues:
            queues[mac_address] = deque(maxlen=20)  # Creazione della coda di lunghezza massima 20
        queues[mac_address].append(payload)
        
        # Utilizza la coda come desiderato
        # Ad esempio, stampa la coda
        print(f"Storico dei payload per {device_name}:")

        # Stampare solo gli ultimi due elementi della coda
        #for i, p in enumerate(list(queues[mac_address])[-2:], start=len(queues[mac_address])-1):
        #    if time_difference is not None:
        #        print(f"Differenza di tempo: {time_difference:.2f} millisecondi")
        #    print(f"Elemento {i}: {p}")
    except Exception as e:
        logging.error(f"Errore durante la gestione del messaggio MQTT: {e}")


# Creazione del client MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Assegnazione delle funzioni di callback
client.on_message = on_message
client.connect(mqtt_broker_url, mqtt_broker_port, keepalive=60)
client.subscribe(mqtt_topic)
client.loop_forever()