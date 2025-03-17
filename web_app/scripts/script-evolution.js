// Description: Script principal de l'application web

document.getElementsByClassName("map_timeline")[0].style.height = window.screen.height;

var graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
var factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;
var landmarkValidTimeDivId = "landmark_valid_time" ;
var landmarkNamesDivId = "landmark_names" ;
var landmarkNamesLabelDivId = "landmark_names_label" ;

var landmarkNamesLabel = "Entité à sélectionner :" ;


var lat = 48.8566;
var lon = 2.3522;
var zoom = 13;

//////////////////////////////////////////////////////////////////

setInnerHTMLToDivFromId(landmarkNamesLabelDivId, landmarkNamesLabel) ;

var layersToRemove = [];

// Appel aux fonctions d'initialisation
var map = initLeaflet(lat, lon, zoom);
allowMapTimelineResize("resizer", map) ;
//initTimeline(graphDBRepositoryURI, lmLabel, lmLabelLang, map=map);

// var button = document.getElementById("enterName") ;
var dropDownMenu = document.getElementById(landmarkNamesDivId);

// Afficher la timeline quand on clique sur un bouton (ou entrée dans le drop menu)
dropDownMenu.addEventListener("change", function(event) { changeSelectedLandmark(event, map, layersToRemove) ;});

// Afficher les landmarks dans un menu déroulant
console.log(factsNamedGraphURI);
getLandmarks(graphDBRepositoryURI, factsNamedGraphURI, dropDownMenu) ;
