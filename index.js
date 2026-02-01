/*
Actuele reisinformatie
======================

Punctualiteit, voertuigposities en rituitval zijn ruwe brongegevens die via ndovloket.nl kunnen worden verkregen. We maken hierbij onderscheid tussen een hoge en een lage drempel en de beschikbaarheid die we daarop kunnen garanderen. Onze premium netwerk leverancier ACC ICT B.V. biedt een redundante netwerk architectuur. Toegang tot deze infrastructuur kan worden verkregen na ondertekening van de NDOV gebruikersovereenkomst. We beseffen dat een printer, pen, handtekening en scanner als een belemmering kunnen worden ervaren.

Met het beschikbaar komen van ND-OV gegevens via CC0 biedt Stichting OpenGeo naast de geplande reisinformatie ook actuele datastromen aan via een best-effort infrastructuur. Het gebruik is gelimiteerd tot fair-use, maximaal 1 verbinding wordt per afnemer per datastroom gebruikt. Gedoe met misbruik leidt tot een blokkade.

De gegevens worden ontsloten via ZeroMQ;

BISON KV6, KV15, KV17:                 tcp://pubsub.besteffort.ndovloket.nl:7658
KV78Turbo:                             tcp://pubsub.besteffort.ndovloket.nl:7817
NS InfoPlus:                           tcp://pubsub.besteffort.ndovloket.nl:7664
SIRI                                   tcp://pubsub.besteffort.ndovloket.nl:7666

Hobbyen met GTFS en GTFS-RT kan overigens via openOV: http://gtfs.openov.nl/

Voorbeelden nodig? http://htmwiki.nl/#!hackathon/realtime.md


Envelopes
=========

Binnen ZeroMQ bestaan berichten uit meerdere delen (parts). Het eerste berichtdeel is een envelope. Op basis van de envelope kan een afnemer een abonnement (subscription) nemen op (een deel van) de actuele datastroom.

Om alleen gegevens van Arriva te ontvangen voert de afnemer na het verbinden een subscribe commando uit voor de prefix "/ARR/". Het is mogelijk om meerdere abonnementen te maken, maar het is (nog) niet mogelijk om met een enkele subscription alleen KV6posinfo te ontvangen. Hiervoor zullen dus "/ARR/KV6posinfo", "/CXX/KV6posinfo", etc. moeten worden toegevoegd.

De data van NDOVloket.nl is verdeeld in een aantal ZeroMQ publish-subscribe systemen.


BISON (KV6, KV15, KV17)
=======================

URI: tcp://pubsub.besteffort.ndovloket.nl:7658/

Bekende envelopes:
/ARR/KV15messages    (Arriva)
/ARR/KV17cvlinfo
/ARR/KV6posinfo
/CXX/KV15messages    (Connexxion, Breng, OV Regio IJsselmond, Transdev)
/CXX/KV17cvlinfo
/CXX/KV6posinfo
/IVU/KV15messages   (U-OV Sneltram)
/IVU/KV17cvlinfo
/IVU/KV6posinfo
/EBS/KV15messages    (EBS)
/EBS/KV17cvlinfo
/EBS/KV6posinfo
/GVB/KV15messages    (GVB)
/GVB/KV17cvlinfo
/GVB/KV6posinfo
/OPENOV/KV6posinfo   (De Lijn, TEC)
/OPENOV/KV15messages (De Lijn, TEC, OpenEBS)
/OPENOV/KV17cvlinfo  (De Lijn, TEC, OpenEBS)
/QBUZZ/KV15messages  (QBuzz)
/QBUZZ/KV17cvlinfo
/QBUZZ/KV6posinfo
/RIG/KV15messages    (HTM, RET)
/RIG/KV17cvlinfo
/RIG/KV6posinfo
/KEOLIS/KV6posinfo   (Keolis, Syntus)
/KEOLIS/KV15message
/KEOLIS/KV17cvlinfo


InfoPlus DVS
============

URI: tcp://pubsub.besteffort.ndovloket.nl:7664/

Bekende envelopes:
/RIG/InfoPlusDASInterface4
/RIG/InfoPlusDVSInterface4
/RIG/InfoPlusLABInterface3
/RIG/InfoPlusRITInterface5
/RIG/InfoPlusSTBInterface3
/RIG/InfoPlusTRBInterface3
/RIG/InfoPlusVTLInterface3
/RIG/InfoPlusVTSInterface3
/RIG/InfoPlusVTTInterface3
/RIG/NStreinpositiesInterface5 (posities)
*/
const zmq = require("zeromq");
const zlib = require("zlib");
const { XMLParser } = require("fast-xml-parser");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const proj4 = require("proj4");
const fs = require("fs");
const { processNSTrains } = require("./ns-handler");


const PORT = 3000;

const ZMQ_SOURCES = {
    "tcp://pubsub.besteffort.ndovloket.nl:7658": ["/RIG/KV6posinfo", "/RIG/KV17cvlinfo", "/QBUZZ/KV6posinfo"],
    "tcp://pubsub.besteffort.ndovloket.nl:7664": ["/RIG/NStreinpositiesInterface5"]
};

// --- Projection ---
proj4.defs("EPSG:28992", "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.417,50.3319,465.552,-0.398957,0.343988,-1.8774,4.0725 +units=m +no_defs");

function convertRdToLatLon(x, y) {
  if (x == -1 || y == -1 || !x || !y) return null;
  const [lon, lat] = proj4("EPSG:28992", "WGS84", [parseInt(x), parseInt(y)]);
  return { lat: lat.toFixed(5), lon: lon.toFixed(5) };
}

// --- STATIC DATA LOADING ---
const stopLookup = new Map(); // UserStopCode -> "Town, Name"

function loadStaticData() {
  console.log("Loading static stop data... (This may take a moment)");
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", removeNSPrefix: true });

  try {
    // 1. Load the Quay Names (The big DB)
    // Structure: StopPlace -> Name/Town AND StopPlace -> Quays -> QuayCode
    console.log("Reading stops-db.xml...");
    // const dbData = fs.readFileSync("stops-db.xml");
    const dbData = fs.readFileSync("ExportCHB_2026-02-01.xml");
    const dbJson = parser.parse(dbData);
    
    // --- DEBUG START ---
    console.log("DEBUG: Raw DB structure keys:", Object.keys(dbJson));
    if (dbJson.export) {
        console.log("DEBUG: Export keys:", Object.keys(dbJson.export));
        if (dbJson.export.stopplaces) {
            // Log the first stop place to check nesting
            const first = Array.isArray(dbJson.export.stopplaces.stopplace) 
                ? dbJson.export.stopplaces.stopplace[0] 
                : dbJson.export.stopplaces.stopplace;
            console.log("DEBUG: First StopPlace:", JSON.stringify(first, null, 2).substring(0, 300));
        }
    }
    // --- DEBUG END ---


    // Create a Map of QuayCode -> { Name, Town }
    const quayToName = new Map();
    
    // Helper to extract array safely
    const getArr = (x) => Array.isArray(x) ? x : (x ? [x] : []);

    const stopPlaces = getArr(dbJson?.export?.stopplaces?.stopplace);
    
    stopPlaces.forEach(sp => {
        const town = sp.stopplacename?.town || "";
        const name = sp.stopplacename?.publicname || "";
        
        // Loop through Quays inside this StopPlace
        const quays = getArr(sp.quays?.quay);
        quays.forEach(q => {
             const qCode = q.quaycode;
             const platform = q.quaynamedata?.quayname || "";
             // Store: "Delft, Station (Perron A)"
             const fullName = `${town}, ${name} ${platform ? '('+platform+')' : ''}`.trim();
             if(qCode) quayToName.set(qCode, fullName);
        });
    });

    console.log(`Loaded names for ${quayToName.size} quays.`);

    // 2. Load the Linker (UserCode -> QuayCode)
    console.log("Reading stops-lookup.xml...");
    // const linkData = fs.readFileSync("stops-lookup.xml");
    const linkData = fs.readFileSync("PassengerStopAssignmentExportCHB_2026-02-01.xml");
    const linkJson = parser.parse(linkData);
    
    const exportQuays = getArr(linkJson?.export?.quays?.quay);
    
    exportQuays.forEach(eq => {
        const qCode = eq.quaycode;
        const userCodes = getArr(eq.userstopcodes?.userstopcodedata);
        
        userCodes.forEach(uc => {
            const userCode = uc.userstopcode;
            if (userCode && quayToName.has(qCode)) {
                stopLookup.set(String(userCode), quayToName.get(qCode));
            }
        });
    });
    
    console.log(`Mapped ${stopLookup.size} user stop codes to names.`);

  } catch (e) {
    console.error("Error loading static files (make sure stops-db.xml and stops-lookup.xml exist):", e.message);
  }
}

// Load data immediately
loadStaticData();

// --- Validation Whitelist (Same as before) ---
const KNOWN_FIELDS = new Set([
  "vv_tm_push", "subscriberid", "version", "dossiername", "timestamp", 
  "kv6posinfo", "kv17cvlinfo", "dataownercode",
  "arrival", "departure", "onroute", "onstop", "init", "end",
  "lineplanningnumber", "operatingday", "journeynumber", "reinforcementnumber",
  "userstopcode", "passagesequencenumber", "source", "vehiclenumber",
  "punctuality", "distancesincelastuserstop", "rd-x", "rd-y", "delimiter",
  "blockcode", "wheelchairaccessible", "numberofcoaches",
  "lat", "lon", "_type", "?xml", "stopname" // Added stopname
]);

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.get("/", (req, res) => res.sendFile(__dirname + "/index.html"));

const msgParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", removeNSPrefix: true });

function processMessage(obj, warnings) {
  let flattened = [];
  
  function traverse(node, parentType = null) {
    if (typeof node === 'object' && node !== null) {
      for (const key in node) {
        if (!key.startsWith("@_") && !KNOWN_FIELDS.has(key.toLowerCase())) {
          // warnings.push(`UNKNOWN FIELD: [${key}]`); 
        }

        if (["ARRIVAL", "DEPARTURE", "ONROUTE", "ONSTOP", "INIT", "END"].includes(key)) {
          const entries = Array.isArray(node[key]) ? node[key] : [node[key]];
          entries.forEach(entry => {
            entry._type = key;
            
            // Add Lat/Lon
            if (entry['rd-x'] && entry['rd-y']) {
                const gps = convertRdToLatLon(entry['rd-x'], entry['rd-y']);
                if (gps) { entry.lat = gps.lat; entry.lon = gps.lon; }
            }

            // --- LOOKUP STOP NAME ---
            if (entry.userstopcode) {
                const name = stopLookup.get(String(entry.userstopcode));
                if (name) entry.stopname = name;
            }

            flattened.push(entry);
            traverse(entry, key);
          });
        } else {
          traverse(node[key], parentType);
        }
      }
    }
  }
  traverse(obj);
  return flattened;
}

async function runZmq() {
  const sock = new zmq.Subscriber();
  for (const [url, topics] of Object.entries(ZMQ_SOURCES)) {
      sock.connect(url);
      console.log(`Connected to ${url} -> [${topics.join(", ")}]`);
      topics.forEach(t => sock.subscribe(t));
  }

  for await (const [topic, ...frames] of sock) {
    try {
      const topicStr = topic.toString(); // <--- Convert topic to string
      const xmlStr = zlib.gunzipSync(Buffer.concat(frames)).toString();
      const jsonObj = msgParser.parse(xmlStr);
      
      let messages = [];

      // --- ROUTING LOGIC ---
      if (topicStr.includes("NStreinpositiesInterface5")) {
          messages = processNSTrains(jsonObj);
      } else {
          // Existing logic for KV6/KV17
          let warnings = [];
          messages = processMessage(jsonObj, warnings);
      }

      if (messages.length) io.emit("update", messages);
      
    } catch (err) {
      console.error("Error processing:", err.message);
    }
  }
}

server.listen(PORT, () => {
  console.log(`Monitor running at http://localhost:${PORT}`);
  runZmq();
});