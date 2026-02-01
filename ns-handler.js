// ns-handler.js

/**
 * Processes NS Train Position XML (Interface 5)
 * Maps it to the standard format used by the frontend.
 */
function processNSTrains(jsonObj) {
    const updates = [];
    
    // Safety check for root element
    // fast-xml-parser with removeNSPrefix:true converts <tns3:ArrayOfTreinLocation> to ArrayOfTreinLocation
    const root = jsonObj.ArrayOfTreinLocation || jsonObj;
    
    if (!root || !root.TreinLocation) {
        return [];
    }

    // Ensure array (single train vs multiple trains)
    const trains = Array.isArray(root.TreinLocation) ? root.TreinLocation : [root.TreinLocation];

    trains.forEach(train => {
        const trainNumber = train.TreinNummer;
        
        // A train consists of 1 or more parts (MaterieelDelen)
        // We handle each part as a separate vehicle on the map
        const parts = train.TreinMaterieelDelen 
            ? (Array.isArray(train.TreinMaterieelDelen) ? train.TreinMaterieelDelen : [train.TreinMaterieelDelen])
            : [];

        parts.forEach(part => {
            // Map NS fields to our Generic Schema
            updates.push({
                dataownercode: "NS",
                
                // Use Train Number as the "Line"
                lineplanningnumber: "Trn " + trainNumber, 
                
                // The physical unit number (e.g., 9572)
                vehiclenumber: part.MaterieelDeelNummer, 
                
                // Also store train number here
                journeynumber: trainNumber, 
                
                // Sequence in the train (1st wagon, 2nd wagon...)
                passagesequencenumber: part.Materieelvolgnummer, 
                
                // NS sends WGS84 directly. No projection needed!
                lat: part.Latitude,
                lon: part.Longitude,
                
                // Speed is in km/h usually in this feed, we convert to string
                punctuality: 0, // NS GPS feed doesn't have punctuality relative to schedule
                speed: part.Snelheid, 
                
                // Use GPS time
                timestamp: part.GpsDatumTijd, 
                
                source: "GPS",
                userstopcode: `v. ${part.Snelheid} km/h`, // Hack: Show speed in Stop column
                _type: "TRAIN" // Custom status for color coding
            });
        });
    });

    return updates;
}

module.exports = { processNSTrains };

