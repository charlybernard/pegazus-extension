// Description: Script principal de l'application web

var graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
var factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;
var landmarkValidTimeDivId = "landmark_valid_time" ;
var dateSliderDivId = "date-slider" ;
var dateInputDivId = "date-input" ;
var dateValidatonButtonId = "date-validation-button" ;

var lat = 48.8566;
var lon = 2.3522;
var zoom = 13;
var startTimeStampSlider = "1790-01-01";
var endTimeStampSlider = "2026-01-01";

var certainLayerGroupName = "Certains";
var uncertainLayerGroupName = "Incertains";
var layerGroupNames = [certainLayerGroupName, uncertainLayerGroupName] ;

var mapDiv = document.getElementById("map");
mapDiv.style.height = "800px";
mapDiv.style.width = "1200px";

//////////////////////////////////////////////////////////////////

// Appel aux fonctions d'initialisation
var map = initLeaflet(lat, lon, zoom);
var overlayLayerGroups = initOverlayLayerGroups(map, layerGroupNames);

// Initialiser la gestion du slider avec les IDs des éléments HTML
manageTimeSlider(dateSliderDivId, dateInputDivId, startTimeStampSlider, endTimeStampSlider);

// Après avoir sélectionné une date, afficher le snapshot correspondant
document.getElementById(dateValidatonButtonId).addEventListener("click", function() {
    displaySnapshotFromSelectedTime(graphDBRepositoryURI, dateInputDivId, gregorianCalendarURI, factsNamedGraphURI,
        map, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName);
});

