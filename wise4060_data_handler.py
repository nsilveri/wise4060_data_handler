import subprocess

def install_missing_dependencies():
    try:
        # Controlla se paho-mqtt è installato
        subprocess.check_output(["pip", "show", "paho-mqtt"])
    except subprocess.CalledProcessError:
        # Se non è installato, installalo
        print("Installing paho-mqtt...")
        subprocess.check_call(["pip", "install", "paho-mqtt"])

    try:
        # Controlla se psycopg2 è installato
        subprocess.check_output(["pip", "show", "psycopg2"])
    except subprocess.CalledProcessError:
        # Se non è installato, installalo
        print("Installing psycopg2...")
        subprocess.check_call(["pip", "install", "psycopg2"])

# Chiamare la funzione per installare le dipendenze mancanti
install_missing_dependencies()

import paho.mqtt.client as mqtt
import json
from collections import deque
import time
import psycopg2
from psycopg2 import sql
import logging
import threading
from datetime import timedelta

# Configurazioni MQTT
mqtt_broker_url = 'SPH-SERVER-PRODUZIONE' # 'localhost' #
mqtt_broker_port = 1883
mqtt_topic_prefix = "Advantech"
mqtt_topic_data_target = "data"
mqtt_topic_filtered_target = "data_filtered"
mqtt_topic_device_status_target = "Device_Status"

mqtt_data_topic = f"{mqtt_topic_prefix}/+/{mqtt_topic_data_target}"
mqtt_status_topic = f"{mqtt_topic_prefix}/+/{mqtt_topic_device_status_target}"

working_behaviors_file_path = 'working_behaviors.json'

# Configurazioni del database PostgreSQL
db_config = {
    'host': 'SPH-SERVER-PRODUZIONE',
    'port': '5432',
    'user': 'postgres',
    'password': 'SphDbProduzione'
}

def load_working_behaviors(file_path, device_names):
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {file_path} non trovato. Creazione del file...")
        # Crea un nuovo file JSON con i comportamenti predefiniti per ciascun dispositivo
        default_behaviors = {}
        for mac_address, device_name in device_names.items():
            default_behaviors[device_name] = {
                "working_behavior": "di1 and di3",
                "default_value": 0
            }
        with open(file_path, 'w') as f:
            json.dump(default_behaviors, f, indent=4)
        print(f"File {file_path} creato con comportamenti predefiniti per ciascun dispositivo.")
        return default_behaviors



# Funzione per connettersi al database e creare un nuovo database per il macAddress se non esiste già
def create_device_data_table(conn, cur):
    try:
        create_data_table_query = """
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
                duration TEXT,
                working INTEGER
            )
        """
        cur.execute(create_data_table_query)
        logging.info("Tabella deviceData creata con successo.")
        return True
    except Exception as e:
        logging.error(f"Errore durante la creazione della tabella deviceData: {e}")
        return False

def create_working_data_table(conn, cur):
    try:
        # Verifica se la tabella workingData esiste già
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'workingData'
                AND table_schema = 'public'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        # Se la tabella non esiste, crea la tabella
        if not table_exists:
            create_working_table_query = """
                CREATE TABLE workingData (
                    macAddress TEXT,
                    nameDevice TEXT,
                    timestamp TEXT,
                    creation_date TIMESTAMP,
                    working INTEGER,
                    duration TEXT,
                    interval INTERVAL
                )
            """
            cur.execute(create_working_table_query)
            conn.commit()  # Assicura che la creazione della tabella sia confermata
            #logging.info("Tabella workingData creata con successo.")
            return True
        else:
            #logging.info("La tabella workingData esiste già.")
            return True
        
    except Exception as e:
        conn.rollback()  # Annulla eventuali modifiche nella transazione
        #logging.error(f"Errore durante la creazione della tabella workingData: {e}")
        return False



def create_database(mac_address, device_name):
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

        return new_db_name

    except Exception as e:
        logging.error(f"Errore durante la creazione del database per '{mac_address}': {e}")
        return None


def connect_and_create_db(mac_address, device_name):
    logging.info(f"Tentativo di connessione e creazione del database per il dispositivo con MAC address '{mac_address}'")

    new_db_name = create_database(mac_address, device_name)
    
    #if new_db_name:
    try:
        # Connessione al nuovo database appena creato
        new_db_config = db_config.copy()  # Copia il dizionario delle configurazioni
        if(new_db_name == None):
            new_db_name = f"{device_name}_{mac_address}"
        print("new_db_name: " + str(new_db_name))
        new_db_config['database'] = new_db_name  # Imposta il nome del database appena creato
        conn = psycopg2.connect(**new_db_config)
        cur = conn.cursor()

        # Creazione della tabella "deviceData" se non esiste già
        device_data_succ = create_device_data_table(conn, cur)

        # Creazione della tabella "workingData" se non esiste già
        working_data_succ = create_working_data_table(conn, cur)

        print("device_data_succ: " + str(device_data_succ) + " | working_data_succ: " + str(working_data_succ))

        # Esegui il commit delle modifiche e chiudi la connessione
        conn.commit()
        print("Commit eseguito")
        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"Errore durante la creazione delle tabelle per '{new_db_name}': {e}")
    #else:
        #logging.error(f"Creazione del database non riuscita per '{mac_address}'")


# Funzione per inserire i dati nel database del macAddress
def insert_device_data(conn, cur, mac_address, device_name, payload):
    try:
        # Formattazione della data leggibile
        readable_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        working_state = calculate_working(payload, device_name)
        duration = payload.get('duration', None)

        # Inserimento dei dati nella tabella "deviceData"
        insert_data_query = """
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
        cur.execute(insert_data_query, (
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
            duration #payload.get('duration', None)
        ))

        # Esegui il commit delle modifiche
        conn.commit()
        logging.info("Dati inseriti con successo nella tabella deviceData.")
        return True
    except Exception as e:
        logging.error(f"Errore durante l'inserimento dei dati nella tabella deviceData: {e}")
        return False

# Funzione per calcolare lo stato working in base al payload e al MAC address
def calculate_working(payload, mac_address):
    behavior_info = working_behaviors.get(mac_address)
    print(behavior_info)
    if behavior_info:
        behavior = behavior_info.get('working_behavior')
        # Implementa la logica per calcolare lo stato working in base al comportamento
        return eval(behavior, {'di1': payload.get('di1', 0), 'di2': payload.get('di2', 0), 'di3': payload.get('di3', 0), 'di4': payload.get('di4', 0)})
    else:
        print(f"Comportamento dello stato working non trovato per il MAC address {mac_address}.")
        return None

def convert_to_interval(time_str):
    # Dividi la stringa in ore, minuti e secondi
    hours, minutes, seconds = map(int, time_str.split(':'))
    # Crea un oggetto timedelta
    delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return delta

def insert_working_data(conn, cur, mac_address, device_name, payload):
    try:
        working_state = calculate_working(payload, device_name)
        if(working_state == 1):
            # Formattazione della data leggibile
            readable_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            # Esempio di utilizzo
            interval = convert_to_interval(payload["duration"])
            print("interval: " + str(interval))
            # Inserimento dei dati nella tabella "workingData"
            insert_working_query = """
                INSERT INTO workingData (
                    macAddress,
                    nameDevice,
                    timestamp,
                    creation_date,
                    working,
                    duration,
                    interval
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """

            #working_state = calculate_working(payload, device_name)
            duration = payload.get('duration', None)
            print("working_state: " + str(working_state))
            cur.execute(insert_working_query, (
                mac_address, 
                device_name,
                time.time(), 
                readable_date,
                int(working_state), #int(payload.get('di1', None) and payload.get('di3', None)) #da modificare
                duration,
                interval
            ))

            # Esegui il commit delle modifiche
            conn.commit()
            logging.info("Dati inseriti con successo nella tabella workingData.")
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Errore durante l'inserimento dei dati nella tabella workingData: {e}")
        return False

def insert_data(mac_address, device_name, payload):
    # Remove 'database' key from db_config
    db_name = f"{device_name}_{mac_address}"
    db_config.pop('database', None)
    
    # Connessione al database del macAddress
    try:
        conn = psycopg2.connect(database=db_name, **db_config)
        cur = conn.cursor()

        # Inserimento dei dati nella tabella "deviceData"
        ins_data_succ = insert_device_data(conn, cur, mac_address, device_name, payload)
        # Inserimento dei dati nella tabella "workingData"
        ins_work_succ = insert_working_data(conn, cur, mac_address, device_name, payload)

        print("ins_data_succ: " + str(ins_data_succ) + " | ins_work_succ: " + str(ins_work_succ))

        # Chiudi la connessione
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Errore durante l'inserimento dei dati nel database per '{mac_address}': {e}")



# Leggi il file JSON dei nomi dei dispositivi
with open('./deviceNames.json') as f:
    device_names = json.load(f)
    print(f"Device data loaded ({device_names})...")

working_behaviors = load_working_behaviors(working_behaviors_file_path, device_names)
print("working_behaviors: " + str(working_behaviors))
    
def msg_changement_check(old_payload, new_payload, time_between_msg):
    #print(old_payload)
    #print(type(old_payload))
    if old_payload:
        old_payload_dict = old_payload[0]  # Estrai il dizionario dalla codaprint(old_payload_dict)
        di1_check = ("di1" + str(new_payload['di1'] == old_payload_dict['di1']))
        di2_check = ("di2" + str(new_payload['di2'] == old_payload_dict['di2']))
        di3_check = ("di3" + str(new_payload['di3'] == old_payload_dict['di3']))
        di4_check = ("di4" + str(new_payload['di4'] == old_payload_dict['di4']))
        #print("di_check: " + str(di1_check) + ", " + str(di2_check) + ", " + str(di3_check) + ", " + str(di4_check) + ", time: " + str(time_between_msg))
        if((di1_check and di2_check and di3_check and di4_check) and (time_between_msg > 600)):
            return True
        else:
            return False
    else:
        return True


# Funzione di callback quando il client si connette al broker MQTT
def on_connect(client, userdata, flags, rc):
    print("Connesso al broker MQTT con risultato:", mqtt.connack_string(rc))
    # Sottoscrizione al topic
    client.subscribe(mqtt_data_topic)

# Funzione di callback quando il client riceve un messaggio MQTT
def on_message(client, userdata, msg, queues, blinking_queue, sent_queues, last_payloads, blink_status, blink_timer):
    global mqtt_topic_device_status_target
    global mqtt_topic_data_target
    global mqtt_topic_publish
    print("\nTopic: " + str(msg.topic))# + "\nPayload: " + str(msg.payload))
    #print(msg.topic.split('/')[-1])
    if msg.topic.split('/')[-1] == mqtt_topic_device_status_target:
        try:
            mac_address = msg.topic.split('/')[-2]
            device_name = device_names.get(mac_address, "Dispositivo sconosciuto")
            payload = json.loads(msg.payload)

            if payload.get('status') == 'disconnect':
                payload_timestamp = time.time()
                # Generazione payload di disconnessione
                payload_off = f'{{"s":0,"t":"{payload_timestamp}","q":192,"c":0,"di1":false,"di2":false,"di3":false,"di4":false,"do1":false,"do2":false,"do3":false,"do4":false}}'
                # Aggiungi il payload alla coda generale
                if mac_address not in queues:
                    queues[mac_address] = deque(maxlen=20)  # Creazione della coda di lunghezza massima 20
                queues[mac_address].append(payload_off)
                if mac_address not in sent_queues:
                    sent_queues[mac_address] = deque(maxlen=20)  # Creazione della coda di lunghezza massima 20

                # Controlla se ci sono almeno due messaggi nella coda per poter confrontare i payloads
                if len(queues[mac_address]) >= 2:
                    # Prendi il penultimo e l'ultimo payload dalla coda
                    penultimate_payload = queues[mac_address][-2]
                    last_payload = queues[mac_address][-1]
                    
                    # Calcola la differenza di tempo tra il penultimo e l'ultimo payload
                    time_difference_2 = (last_payload['t'] - penultimate_payload['t']) * 1000

                    print(f"\n{device_name} disconnected!")
                    # Invia il penultimo payload e scrivilo nel database
                    mqtt_topic_publish = f"{mqtt_topic_prefix}/{mac_address}/{mqtt_topic_filtered_target}"

                    # Calcolare la differenza di tempo tra il penultimo e l'ultimo payload
                    time_difference = timedelta(seconds=last_payload['t'] - penultimate_payload['t'])
                    print(device_name + ") time_difference: " + str(time_difference))

                    time_difference_str = str(time_difference)
                    hours, remainder = divmod(time_difference.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    print(duration_str)

                    penultimate_payload['duration'] = str(duration_str)

                    print("penultimate_payload: " + str(penultimate_payload))
                    msg_check = msg_changement_check(sent_queues[mac_address], penultimate_payload, time_difference_2)
                    print(msg_check)
                    #if(msg_check):
                    sent_queues[mac_address].append(penultimate_payload)
                    print("sent_queues: " + str(sent_queues[mac_address][-1]))
                    insert_data(mac_address, device_name, penultimate_payload)
                    client.publish(mqtt_topic_publish, json.dumps(penultimate_payload))
                else:
                    msg_check = msg_changement_check(sent_queues[mac_address], penultimate_payload, time_difference_2)
                    print(msg_check)
                    #if(msg_check):
                    sent_queues[mac_address].append(penultimate_payload)
                    print("sent_queues: " + str(sent_queues[mac_address][-1]))
                    insert_data(mac_address, device_name, payload_off)
                    client.publish(mqtt_topic_publish, json.dumps(payload_off))
        except Exception as e:
            print("Errore durante la gestione del messaggio MQTT per client disconnesso:", e)

    elif msg.topic.split('/')[-1] == mqtt_topic_data_target:
        #try:
            mac_address = msg.topic.split('/')[-2]
            device_name = device_names.get(mac_address, "Dispositivo sconosciuto")
            payload = json.loads(msg.payload)
            payload['t'] = time.time()
            payload["modified_vars"] = {}

            # Aggiungi il payload alla coda generale
            if mac_address not in queues:
                queues[mac_address] = deque(maxlen=20)  # Creazione della coda di lunghezza massima 20
            queues[mac_address].append(payload)

            if mac_address not in sent_queues:
                sent_queues[mac_address] = deque(maxlen=20)  # Creazione della coda di lunghezza massima 20

            #print("last payload: " + str(queues[mac_address][-1]))

            # Controlla se ci sono almeno tre messaggi nella coda per poter confrontare i payloads
            if len(queues[mac_address]) >= 3:
                # Prendi il terzultimo, il penultimo e l'ultimo payload dalla coda
                third_last_payload = queues[mac_address][-3]
                penultimate_payload = queues[mac_address][-2]
                last_payload = queues[mac_address][-1]
                
                # Calcola la differenza di tempo tra il terzultimo e il penultimo payload
                time_difference_1 = (penultimate_payload['t'] - third_last_payload['t']) * 1000
                
                # Calcola la differenza di tempo tra il penultimo e l'ultimo payload
                time_difference_2 = (last_payload['t'] - penultimate_payload['t']) * 1000

                # Calcola le variabili modificate rispetto al payload precedente
                modified_vars = {}
                for key in ['di1', 'di2', 'di3', 'di4']:
                    if key in last_payload and key in penultimate_payload and penultimate_payload[key] != last_payload[key]:
                        modified_vars[key] = payload[key]
                        
                print("modified_vars: " + str(modified_vars))
                queues[mac_address][-1]['modified_vars'].update(modified_vars)
                
                comparison_modified_vars = penultimate_payload['modified_vars'].keys() == last_payload['modified_vars'].keys() == third_last_payload['modified_vars'].keys()

                
                # Verifica se entrambe le differenze di tempo sono inferiori a 600 e se hanno le stesse chiavi nei modified_vars
                if time_difference_1 < 600 and time_difference_2 < 600 and comparison_modified_vars:
                    print("Blinking detected!")
                    modified_var_key = next(iter(modified_vars.keys())) #ottiene la Key modificata (di1, di2, di3 o di4)
                    penultimate_payload[modified_var_key] = True

                    # Calcolare la differenza di tempo tra il penultimo e l'ultimo payload
                    time_difference = timedelta(seconds=last_payload['t'] - penultimate_payload['t'])
                    print(device_name + ") time_difference: " + str(time_difference))
                    
                    hours, remainder = divmod(time_difference.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    print(duration_str)

                    penultimate_payload['duration'] = str(duration_str)

                    #print("msg_check: " + str(msg_check))

                    #print("penultimate_payload:\n" + str(penultimate_payload))
                    #if(penultimate_payload["di1"] != sent_queues["di1"] or penultimate_payload["di2"] != sent_queues["di2"] or penultimate_payload["di3"] != sent_queues["di3"] or penultimate_payload["di4"] != sent_queues["di4"]):
                    msg_check = msg_changement_check(sent_queues[mac_address], penultimate_payload, time_difference_2)
                    print(msg_check)
                    if(msg_check):
                        insert_data(mac_address, device_name, penultimate_payload)
                        sent_queues[mac_address].append(penultimate_payload)
                    client.publish(mqtt_topic_publish, json.dumps(penultimate_payload))
                    pass
                else:
                    print("\nNo blinking detected!")
                    # Invia il penultimo payload e scrivilo nel database
                    mqtt_topic_publish = f"{mqtt_topic_prefix}/{mac_address}/{mqtt_topic_filtered_target}"

                    # Calcolare la differenza di tempo tra il penultimo e l'ultimo payload
                    time_difference = timedelta(seconds=last_payload['t'] - penultimate_payload['t'])
                    print(device_name + ") time_difference: " + str(time_difference))
                    
                    hours, remainder = divmod(time_difference.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    print(duration_str)

                    penultimate_payload['duration'] = str(duration_str)

                    print("penultimate_payload:\n" + str(penultimate_payload))
                    msg_check = msg_changement_check(sent_queues[mac_address], penultimate_payload, time_difference_2)
                    print(msg_check)
                    #if(msg_check):
                    sent_queues[mac_address].append(penultimate_payload)
                    print("sent_queues: " + str(sent_queues[mac_address][-1]))
                    insert_data(mac_address, device_name, penultimate_payload)
                    client.publish(mqtt_topic_publish, json.dumps(penultimate_payload))

        #except Exception as e:
            #print("Errore durante la gestione del messaggio MQTT:", e)

# Funzione per la gestione dei messaggi MQTT per un singolo MAC address
def mqtt_thread(mac_address, device_name, queues, blinking_queue, sent_queues, last_payloads, blink_status, blink_timer):
    # Creazione del client MQTT per il MAC address specifico
    thread_id = threading.get_ident()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = lambda client, userdata, msg: on_message(client, userdata, msg, queues, blinking_queue, sent_queues, last_payloads, blink_status, blink_timer)
    client.connect(mqtt_broker_url, mqtt_broker_port, keepalive=60)
    client.subscribe(f"{mqtt_topic_prefix}/{mac_address}/{mqtt_topic_data_target}")
    client.subscribe(f"{mqtt_topic_prefix}/{mac_address}/{mqtt_topic_device_status_target}")
    client.loop_forever()

# Creazione e avvio dei thread per ciascun MAC address
queues = {}
blinking_queue = {}
sent_queues = {}
last_payloads = {}
blink_status = False
blink_timer = time.time()

for mac_address in device_names.keys():
    device_name = device_names[mac_address]
    connect_and_create_db(mac_address, device_name)
    thread = threading.Thread(target=mqtt_thread, args=(mac_address, device_name, queues, blinking_queue, sent_queues, last_payloads, blink_status, blink_timer))
    thread.start()
