var contentDivId = "content";

var radioInputName = "visu_selection";
var radioInputDivId = radioInputName ;
var radioInputLabel = "Type de visualisation";

//////////////////////////////// Variables //////////////////////////////////

var graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
var factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;

var validationButtonLabel = "Valider";
var landmarkSelectionLabel = "Entité à sélectionner : " ;
var dateSelectionLabel = "Sélectionner une date : ";

var landmarkValidTimeDivId = "landmark-valid-time" ;
var landmarkNamesDivId = "landmark-names" ;
var landmarkNamesLabelDivId = "landmark-names-label" ;
var mapTimelineDivId = "map-timeline";
var timelineDivId = "timeline";
var mapDivId = "leaflet-map";
var mapTimelineResizerDivId = "map-timeline-resizer";
var resizerClassName = "resizer";

var dateSliderDivId = "date-slider";
var dateSliderSettings = {"min":0, "max":100, "value":0};
var dateInputDivId = "date-input";
var dateValidationButtonId = "date-validation-button";

var mapLat = 48.8566;
var mapLon = 2.3522;
var mapZoom = 13;

var tileLayerSettings = [
    {type:"xyz", url:"https://tile.openstreetmap.org/{z}/{x}/{y}.png", name:"OpenStreetMap"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:verniquet_1789", name:"Atlas de Verniquet"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:andriveau_1849", name:"Plan d'Andriveau"},
    {type:"xyz", url:"https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png", name:"OpenStreetMap Hot"},
];

var startTimeStampSlider = "1790-01-01";
var endTimeStampSlider = "2026-01-01";
var calendarURI = gregorianCalendarURI;
var certainLayerGroupName = "Certains";
var uncertainLayerGroupName = "Incertains";
var layerGroupNames = [certainLayerGroupName, uncertainLayerGroupName] ;

var landmarkEvolutionName = "Évolution des repères"
var snapshotName = "Snapshot"

// Object of LeafletObjects class which contains all markers and dots
var lo = new LeafletObjects(L);

var radioInputs = {"name":radioInputName, "label":radioInputLabel, "id":radioInputName,
                "values":{"snapshot":{"label":snapshotName, "id":"snapshot-selection"}, "timeline":{"label":landmarkEvolutionName, "id":"timeline-selection"}}};

createHTML(L, radioInputs, contentDivId);

var inputRadioDiv = document.getElementById(radioInputDivId);
var contentDiv = document.getElementById(contentDivId);

inputRadioDiv.addEventListener('change', function(){
    var querySelectorSetting = `input[name="${radioInputName}"]:checked`;
    var selectedValue = document.querySelector(querySelectorSetting).value;
    clearDiv(contentDiv);
    if (selectedValue == landmarkEvolutionName){
        createHTMLEvolution(L, contentDiv, landmarkNamesDivId, landmarkSelectionLabel, landmarkValidTimeDivId,
            mapTimelineDivId, timelineDivId, mapDivId, mapTimelineResizerDivId, resizerClassName, tileLayerSettings);
        setActionsForEvolution(graphDBRepositoryURI, factsNamedGraphURI, mapLat, mapLon, mapZoom, landmarkNamesDivId, resizerClassName);
    } else if (selectedValue == snapshotName){
        createHTMLSnapshot(L, contentDiv, dateSliderDivId, dateSelectionLabel, dateSliderSettings, dateInputDivId, dateValidationButtonId, validationButtonLabel, mapDivId);
        setActionsForSnapshot(graphDBRepositoryURI, factsNamedGraphURI, mapDivId, mapLat, mapLon, mapZoom, certainLayerGroupName, uncertainLayerGroupName,
            dateSliderDivId, dateInputDivId, dateValidationButtonId, startTimeStampSlider, endTimeStampSlider, calendarURI, tileLayerSettings);
    }
});
