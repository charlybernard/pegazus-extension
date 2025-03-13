// Description: Script principal de l'application web

var graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
var factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;
var landmarkValidTimeDivId = "landmark_valid_time" ;

var lat = 48.8566;
var lon = 2.3522;
var zoom = 13;

//////////////////////////////////////////////////////////////////

var layersToRemove = [];

var mapDiv = document.getElementById("map");
mapDiv.style.height = "600px";
mapDiv.style.width = "1200px";

// Appel aux fonctions d'initialisation
var map = initLeaflet(lat, lon, zoom);

function manageTimeSlider(dateSliderDivId, dateInputDivId, startTimeStamp, endTimeStamp) {
    var dateSlider = document.getElementById(dateSliderDivId);
    var dateInput = document.getElementById(dateInputDivId);

    // Dates de référence (point de départ et d'arrivée)
    var startDate = new Date(startTimeStamp);
    var endDate = new Date(endTimeStamp);

    // Définir les bornes du slider
    var totalDays = Math.floor((endDate - startDate) / (1000 * 60 * 60 * 24));
    dateSlider.min = 0;
    dateSlider.max = totalDays;

    // Synchronisation des événements
    dateSlider.addEventListener("input", () => updateDateFromSlider(startDate, dateSlider, dateInput));
    dateInput.addEventListener("input", () => updateSliderFromInput(startDate, endDate, dateSlider, dateInput));

    // Initialiser la date affichée
    updateDateFromSlider(startDate, dateSlider, dateInput);
}

// Fonction pour mettre à jour l'affichage de la date depuis le slider
function updateDateFromSlider(startDate, dateSlider, dateInput) {
    var newDate = new Date(startDate);
    newDate.setDate(startDate.getDate() + parseInt(dateSlider.value));

    // Mettre à jour l'input date
    dateInput.value = newDate.toISOString().split('T')[0];
}

// Fonction pour mettre à jour le slider depuis l'input date
function updateSliderFromInput(startDate, endDate, dateSlider, dateInput) {
    var selectedDate = new Date(dateInput.value);

    // Vérifier si la date est dans la plage autorisée
    if (selectedDate < startDate) {
        selectedDate = startDate;
        dateInput.value = startDate.toISOString().split('T')[0];
    }
    if (selectedDate > endDate) {
        selectedDate = endDate;
        dateInput.value = endDate.toISOString().split('T')[0];
    }

    var diffDays = Math.floor((selectedDate - startDate) / (1000 * 60 * 60 * 24));

    // Mettre à jour le slider
    dateSlider.value = diffDays;
}

// Initialiser la gestion du slider avec les IDs des éléments HTML
manageTimeSlider("date-slider", "date-input", "1790-01-01", "2026-01-01");

document.getElementById("date-validaton-button").addEventListener("click", function() {
    var timeStamp = document.getElementById("date-input").value;
    console.log(timeStamp);
    var timeCalendarURI = gregorianCalendarURI ;
    getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, factsNamedGraphURI, map, layersToRemove) ;
});

