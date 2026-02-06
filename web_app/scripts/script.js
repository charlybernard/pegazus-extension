//////////////////////////////// Variables //////////////////////////////////

const graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;

const contentDivId = "content";
const selectionDivId = "selection";

const radioInputName = "radioInput";
const radioInputDivId = radioInputName ;
const radioInputLabel = "Type de visualisation";

const validationButtonLabel = "Valider";
const landmarkSelectionLabel = "Entité à sélectionner : " ;
const dateSelectionLabel = "Sélectionnez une date : ";

const graphDivId = "graph";
const graphSelectionDivId = "graph-selection";
const graphSelectionLabel = "Graphe à sélectionner : ";

const landmarkSelectionDivId = "landmark-selection"; ;
const landmarkValidTimeDivId = "landmark-valid-time" ;
const landmarkNamesDivId = "landmark-names" ;
const landmarkNamesLabelDivId = "landmark-names-label" ;
const mapTimelineDivId = "map-timeline";
const timelineDivId = "timeline";
const mapDivId = "leaflet-map";
const mapTimelineResizerDivId = "map-timeline-resizer";
const resizerClassName = "resizer";

const dateSelectionDivId = "date-selection";
const dateSliderDivId = "date-slider";
const dateSliderSettings = {"min":0, "max":100, "value":0};
const dateInputDivId = "date-input";
const dateValidationButtonId = "date-validation-button";

const mapLat = 48.8566;
const mapLon = 2.3522;
const mapZoom = 13;
const mapMessages = {
    noLandmarkToDisplay: "Aucun repère à afficher à cette date.",
    nameTitle : "Nom",
    flyOverLandmark : "Survolez un lieu",
    landmarkSelectValue : "Sélectionnez une valeur",
    graphSelectValue : "Sélectionnez un graphe",
}

const tileLayerSettings = [
    {type:"xyz", url:"https://tile.openstreetmap.org/{z}/{x}/{y}.png", name:"OpenStreetMap"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:verniquet_1789", name:"Atlas de Verniquet (1789)"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:jacoubet_1836", name:"Plan de Jacoubet (1836)"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:andriveau_1849", name:"Plan d'Andriveau (1849)"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:poubelle_1888", name:"Plan Poubelle (1888)"},
    {type:"xyz", url:"https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png", name:"OpenStreetMap Hot"},
];

const startTimeStampSlider = "1790-01-01";
const endTimeStampSlider = "2027-01-01";
const timeDelay = 20 ; // Delay in years, not delay if null
// const timeDelay = null ; // Delay in years, not delay if null

const calendarURI = gregorianCalendarURI;
const certainLayerGroupName = "Certains";
const uncertainLayerGroupName = "Incertains";
const layerGroupNames = [certainLayerGroupName, uncertainLayerGroupName] ;

const landmarkEvolutionName = "Évolution des repères"
const snapshotName = "Snapshot"

// Object of LeafletObjects class which contains all markers and dots
const lo = new LeafletObjects(L);

const radioInputs = {"name":radioInputName, "label":radioInputLabel, "id":radioInputName,
                "values":{"snapshot":{"label":snapshotName, "id":"snapshot-selection"}, "timeline":{"label":landmarkEvolutionName, "id":"timeline-selection"}}};

const graphSettings = {"divId":graphDivId, "selectionDivId":graphSelectionDivId, "selectionLabel":graphSelectionLabel} ;

//////////////////////////////// Actions on the page //////////////////////////////////

createHTML(L, radioInputs, contentDivId, selectionDivId, graphSettings, mapMessages);
const contentDiv = document.getElementById(contentDivId);
const selectDiv = document.getElementById(selectionDivId);
const inputRadioDiv = document.getElementById(radioInputDivId);
const graphSelectionDiv = document.getElementById(graphSettings.selectionDivId);

var namedGraphURI = graphSelectionDiv.value;

inputRadioDiv.addEventListener('change', function(){
    var querySelectorSetting = `input[name="${radioInputName}"]:checked`;
    var selectedValue = document.querySelector(querySelectorSetting).value;

    // Supprimer les éléments de sélection de landmark et de date s'ils existent (car pas nécessaires pour les deux types de visualisation)
    clearDiv(contentDiv);
    removeElementsByIds([landmarkSelectionDivId, landmarkValidTimeDivId, dateSelectionDivId]) ;

    if (selectedValue == landmarkEvolutionName){
        createHTMLEvolution(L, landmarkSelectionDivId, contentDiv, selectDiv, landmarkNamesDivId, landmarkSelectionLabel, landmarkValidTimeDivId,
            mapTimelineDivId, timelineDivId, mapDivId, mapTimelineResizerDivId, resizerClassName, tileLayerSettings);
        setActionsForEvolution(graphDBRepositoryURI, namedGraphURI, mapLat, mapLon, mapZoom, mapMessages, landmarkNamesDivId, timelineDivId, landmarkValidTimeDivId, resizerClassName, tileLayerSettings);
    } else if (selectedValue == snapshotName){
        createHTMLSnapshot(L, dateSelectionDivId, contentDiv, selectDiv, dateSliderDivId, dateSelectionLabel, dateSliderSettings, dateInputDivId, dateValidationButtonId, validationButtonLabel, mapDivId);
        setActionsForSnapshot(graphDBRepositoryURI, namedGraphURI, mapDivId, mapLat, mapLon, mapZoom, mapMessages, certainLayerGroupName, uncertainLayerGroupName,
            dateSliderDivId, dateInputDivId, dateValidationButtonId, startTimeStampSlider, endTimeStampSlider, timeDelay, calendarURI, tileLayerSettings) ;
    }
});


graphSelectionDiv.addEventListener('change', function(){
    namedGraphURI = graphSelectionDiv.value;
    console.log("Selected named graph:", namedGraphURI);

});