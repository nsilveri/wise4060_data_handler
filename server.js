const express = require('express');
const mqtt = require('mqtt');
const http = require('http');
const socketIO = require('socket.io');
const Chart = require('chart.js');
//const sqlite3 = require('sqlite3').verbose(); // Importa SQLite3
const moment = require('moment');
const { Pool } = require('pg');
//const BlinkDetector = require('./blink_detector');
const app = express();
const httpServer = http.createServer(app); // Definisci httpServer prima di utilizzarlo
const io = socketIO(httpServer);

const PORT = process.env.PORT || 8080;
const mqttBrokerUrl = 'mqtt://SPH-SERVER-PRODUZIONE:1883'; //192.168.1.101 
//const mqttBrokerUrl = 'mqtt://localhost:1883'; //192.168.1.101
const MQTT_TOPIC_PREFIX = "Advantech";
const MQTT_TOPIC_TARGET = "data_filtered";
const MQTT_TOPIC = `${MQTT_TOPIC_PREFIX}/+/${MQTT_TOPIC_TARGET}`;
MQTT_SERVICE = true;

const BLINK_TESTED = 2
const CURR_PREV_UNCHANGED = 3
const BLINKING_VALUE = 1;
const BLINKING_THRESHOLD = 1600;

// Carica i nomi dei dispositivi da un file JSON
const deviceNames = require('./deviceNames.json');
console.log("deviceNames: " + String(deviceNames))
const { strict } = require('assert');
const { time } = require('console');
const today = new Date().toISOString().slice(0, 10);
current_selected_date = today;

// Funzione per recuperare i dati dal database e popolare le code per un singolo MAC address
async function fetchAndPopulateDataForMAC(db_name, macAddress, deviceName, dateToFetch) {
    try {
        // Creazione di un pool di connessione per il database del MAC address corrente
        const dbConfig = {
            user: 'postgres',
            host: 'SPH-SERVER-PRODUZIONE',
            database: db_name,  // Utilizza l'indirizzo MAC come nome del database
            password: 'SphDbProduzione',
            port: 5432,
        };
        const pool = new Pool(dbConfig);

        // Ottieni la data odierna formattata come 'YYYY-MM-DD'
        const today = new Date().toISOString().slice(0, 10);

        // Connessione al pool di connessione al database
        const client = await pool.connect();

        // Query per selezionare i dati per la giornata odierna per il MAC address corrente
        const query = `
            SELECT DATE(creation_date) AS date_only, timestamp, di1, di2, di3, di4, do1, do2, do3, do4
            FROM deviceData
            WHERE DATE(creation_date) = $1
        `;
        
        // Esegui la query passando la data odierna come parametro
        const result = await client.query(query, [dateToFetch]);

        // Log dei record estratti dal database
        //console.log(`Record estratti per il MAC address ${db_name}:`);
        //console.log(result);

        //deviceData = {};
        //generate_device_data_structure();

        // Popola le code con i dati ottenuti dalla query
        result.rows.forEach(row => {
            const { timestamp, di1, di2, di3, di4, do1, do2, do3, do4 } = row;
            const formattedTimestamp = moment.unix(timestamp).format("hh:mm");

            // Assicurati che esista un oggetto deviceData per l'indirizzo MAC
            //if (deviceData.hasOwnProperty(macAddress)) {
                deviceData[macAddress].di1.push(di1);
                deviceData[macAddress].di2.push(di2);
                deviceData[macAddress].di3.push(di3);
                deviceData[macAddress].di4.push(di4);
                deviceData[macAddress].di_timestamps.push(formattedTimestamp);
                deviceData[macAddress].do1.push(do1);
                deviceData[macAddress].do2.push(do2);
                deviceData[macAddress].do3.push(do3);
                deviceData[macAddress].do4.push(do4);
            //}
        });

        // Rilascia la connessione al pool di connessione al database
        client.release();
    } catch (error) {
        console.error(`Errore durante il recupero e il popolamento dei dati per il MAC address ${macAddress}:`, error);
    }
}

function clearAllData() {
    return new Promise((resolve, reject) => {
        try {
            Object.keys(deviceData).forEach(macAddress => {
                deviceData[macAddress].di1 = [];
                deviceData[macAddress].di2 = [];
                deviceData[macAddress].di3 = [];
                deviceData[macAddress].di4 = [];
                deviceData[macAddress].di_timestamps = [];
                deviceData[macAddress].do1 = [];
                deviceData[macAddress].do2 = [];
                deviceData[macAddress].do3 = [];
                deviceData[macAddress].do4 = [];
            });
            resolve();
        } catch (error) {
            reject(error);
        }
    });
}

async function populate_data_on_request(date){
    console.log("Populating data for " + String(date));
    
    // Svuota tutte le code prima di popolarle con nuovi dati
    await clearAllData();

    Object.keys(deviceNames).forEach(macAddress => {
        db_name = `${deviceNames[macAddress]}_${macAddress}`;
        fetchAndPopulateDataForMAC(db_name, macAddress, deviceNames[macAddress], date).then(() => {
        }).catch(error => {
            console.error(`Errore durante il recupero e il popolamento dei dati per il MAC address ${macAddress}:`, error);
        });
    });
}

let deviceData = {};

function generate_device_data_structure(){
    Object.keys(deviceNames).forEach(macAddress => {
        deviceData[macAddress] = {
            name: deviceNames[macAddress],
            label: [],
            di1: [],
            di2: [],
            di3: [],
            di4: [],
            di_timestamps: [],
            do1: [],
            do2: [],
            do3: [],
            do4: [],
        };
    });
}

generate_device_data_structure();
populate_data_on_request(today);


function extractArray(deviceData) {
    const extractedData = {};
    for (const macAddress in deviceData) {
        extractedData[macAddress] = {
            name: deviceData[macAddress].name,
            di1: deviceData[macAddress].di1,
            di2: deviceData[macAddress].di2,
            di3: deviceData[macAddress].di3,
            di4: deviceData[macAddress].di4,
            di_ts: deviceData[macAddress].di_timestamps,
            do1: deviceData[macAddress].do1,
            do2: deviceData[macAddress].do2,
            do3: deviceData[macAddress].do3,
            do4: deviceData[macAddress].do4,
        };
    }
    console.log(extractedData);
    return extractedData;
}

// MQTT Connection
const mqttClient = mqtt.connect(mqttBrokerUrl);

io.on('connection', (socket) => {
    console.log('A client connected');
    
    socket.on('requestDataForDate', async (requestedDate) => {
        console.log("requestedDate === today: " + String(requestedDate === today) + " - MQTT_SERVICE: " + String(MQTT_SERVICE));
        if(requestedDate !== current_selected_date){
            console.log("Selected date different from current!")
            current_selected_date = requestedDate;
        }
        if(requestedDate === today && MQTT_SERVICE == false){
            mqttClient.connect();
            MQTT_SERVICE = true;
            console.log("--------1) sel_data == today, MQTT enabled!");
            populate_data_on_request(requestedDate);
            io.emit('deviceDataFiltered', extractArray(deviceData));
        }else {
            MQTT_SERVICE = false;
            mqttClient.end();
            populate_data_on_request(requestedDate);
            io.emit('deviceDataFiltered', extractArray(deviceData));
        }
        return;
    });
    io.emit('deviceDataFiltered', extractArray(deviceData));
});

mqttClient.on('connect', () => {
    console.log('Connected to MQTT broker');
    mqttClient.subscribe(MQTT_TOPIC);
});

mqttClient.on('message', (topic, message) => {
    if(MQTT_SERVICE == true)
    {    
        let data = JSON.parse(message.toString());
        let { t: timestamp, s: status, c: count, di1, di2, di3, di4, do1, do2, do3, do4 } = data;

        const macAddress = topic.split('/')[1]; // Estrarre il MAC Address dal topic MQTT
        console.log("message (" + topic + "): "  + String(message));

        const options = { timeZone: 'Europe/Rome', hour12: false };
        //const formattedTimestamp = new Date(timestamp).toLocaleString('it-IT', options);
        //console.log("formattedTimestamp: " + String(formattedTimestamp));

        const formattedTimestamp = moment.unix(timestamp).format("HH:mm");//:ss.SSS");
        console.log(formattedTimestamp);

        // Gestisci i dati in base al topic MQTT
        if (topic.startsWith("Advantech") && topic.endsWith("data_filtered")) {

            deviceData[macAddress].di1.push(di1);
            deviceData[macAddress].di2.push(di2);
            deviceData[macAddress].di3.push(di3);
            deviceData[macAddress].di4.push(di4);
            deviceData[macAddress].di_timestamps.push(formattedTimestamp);
            deviceData[macAddress].do1.push(do1);
            deviceData[macAddress].do2.push(do2);
            deviceData[macAddress].do3.push(do3);
            deviceData[macAddress].do4.push(do4);

            io.emit('deviceDataFiltered', extractArray(deviceData)); 
        }
    }else console.log("MQTT_SERVICE disabled.");
});

// Express Route
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// Start Server
httpServer.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});

// Static Files
app.use(express.static('public'));