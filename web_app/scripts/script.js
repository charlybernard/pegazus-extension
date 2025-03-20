const contentDivId = "content";

const radioInputName = "visu_selection";
const radioInputDivId = radioInputName ;
const radioInputLabel = "Type de visualisation";

//////////////////////////////// Variables //////////////////////////////////

const graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
const factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;

const validationButtonLabel = "Valider";
const landmarkSelectionLabel = "Entité à sélectionner : " ;
const dateSelectionLabel = "Sélectionnez une date : ";

const landmarkValidTimeDivId = "landmark-valid-time" ;
const landmarkNamesDivId = "landmark-names" ;
const landmarkNamesLabelDivId = "landmark-names-label" ;
const mapTimelineDivId = "map-timeline";
const timelineDivId = "timeline";
const mapDivId = "leaflet-map";
const mapTimelineResizerDivId = "map-timeline-resizer";
const resizerClassName = "resizer";

const dateSliderDivId = "date-slider";
const dateSliderSettings = {"min":0, "max":100, "value":0};
const dateInputDivId = "date-input";
const dateValidationButtonId = "date-validation-button";

const mapLat = 48.8566;
const mapLon = 2.3522;
const mapZoom = 13;

const tileLayerSettings = [
    {type:"xyz", url:"https://tile.openstreetmap.org/{z}/{x}/{y}.png", name:"OpenStreetMap"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:verniquet_1789", name:"Atlas de Verniquet"},
    {type:"wms", url:"http://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:andriveau_1849", name:"Plan d'Andriveau"},
    {type:"xyz", url:"https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png", name:"OpenStreetMap Hot"},
];

const startTimeStampSlider = "1790-01-01";
const endTimeStampSlider = "2026-01-01";
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

createHTML(L, radioInputs, contentDivId);

const inputRadioDiv = document.getElementById(radioInputDivId);
const contentDiv = document.getElementById(contentDivId);

inputRadioDiv.addEventListener('change', function(){
    var querySelectorSetting = `input[name="${radioInputName}"]:checked`;
    var selectedValue = document.querySelector(querySelectorSetting).value;
    clearDiv(contentDiv);
    if (selectedValue == landmarkEvolutionName){
        createHTMLEvolution(L, contentDiv, landmarkNamesDivId, landmarkSelectionLabel, landmarkValidTimeDivId,
            mapTimelineDivId, timelineDivId, mapDivId, mapTimelineResizerDivId, resizerClassName, tileLayerSettings);
        setActionsForEvolution(graphDBRepositoryURI, factsNamedGraphURI, mapLat, mapLon, mapZoom, landmarkNamesDivId, timelineDivId, landmarkValidTimeDivId, resizerClassName, tileLayerSettings);
    } else if (selectedValue == snapshotName){
        createHTMLSnapshot(L, contentDiv, dateSliderDivId, dateSelectionLabel, dateSliderSettings, dateInputDivId, dateValidationButtonId, validationButtonLabel, mapDivId);
        setActionsForSnapshot(graphDBRepositoryURI, factsNamedGraphURI, mapDivId, mapLat, mapLon, mapZoom, certainLayerGroupName, uncertainLayerGroupName,
            dateSliderDivId, dateInputDivId, dateValidationButtonId, startTimeStampSlider, endTimeStampSlider, calendarURI, tileLayerSettings);
    }
});