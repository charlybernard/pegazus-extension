function createHTML(L, radioInputs, contentDivId){
    document.body.style.height = "100vh";
    document.body.style.width = "100vw";

    var inputRadioDiv = createInputRadioDiv(L, radioInputs);
    var contentDiv = createDiv(L, "div", {"id":contentDivId}, null, null);
    
    document.body.appendChild(inputRadioDiv);
    document.body.appendChild(contentDiv);

    var contentDivHeightInt = 100*(document.body.clientHeight - inputRadioDiv.clientHeight)/document.body.clientHeight;
    contentDiv.style.height = `${contentDivHeightInt}em`

    createStyleInputRadioDiv(inputRadioDiv);
}

function createStyleInputRadioDiv(inputRadioDiv){
    inputRadioDiv.style.display = "flex";
    inputRadioDiv.style.flexDirection = "row";
}

function createHTMLEvolution(L, contentDiv, landmarkNamesDivId, landmarkNamesLabel, landmarkValidTimeDivId,
    mapTimelineDivId, timelineDivId, mapDivId, mapTimelineResizerDivId, resizerClassName){
    var landmarkNamesDiv = createDiv(L, "div", {}, null, null);
    var landmarkNamesLabelDiv = createLabel(L, landmarkNamesDivId, landmarkNamesLabel, null, labelContentIsBold = true);
    var landmarkNamesSelectDiv = createDiv(L, "select", {"name":landmarkNamesDivId, "id":landmarkNamesDivId}, null, null);
    landmarkNamesDiv.appendChild(landmarkNamesLabelDiv);
    landmarkNamesDiv.appendChild(landmarkNamesSelectDiv);

    var landmarkValidTimeDiv = createDiv(L, "div", {"id":landmarkValidTimeDivId}, null, null);

    var mapTimelineDiv = createDiv(L, "div", {"id":mapTimelineDivId}, null, null);
    var timelineDiv = createDiv(L, "div", {"id":timelineDivId}, null, null);
    var mapTimelineResizerDiv = createDiv(L, "div", {"id":mapTimelineResizerDivId, "class":resizerClassName}, null, null);
    var mapDiv = createDiv(L, "div", {"id":mapDivId}, null, null);
    mapTimelineDiv.appendChild(timelineDiv);
    mapTimelineDiv.appendChild(mapTimelineResizerDiv);
    mapTimelineDiv.appendChild(mapDiv);

    contentDiv.appendChild(landmarkNamesDiv);
    contentDiv.appendChild(landmarkValidTimeDiv);
    contentDiv.appendChild(mapTimelineDiv);

    getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);

    window.addEventListener('resize', function(){
        getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);
    })
    
}

function getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv){
    var mapTimelineDivHeightInt = 100*(contentDiv.clientHeight - landmarkNamesDiv.clientHeight - landmarkValidTimeDiv.clientHeight)/contentDiv.clientHeight
    mapTimelineDiv.style.height = `${mapTimelineDivHeightInt}%`; // Define height of map-timeline div
    mapTimelineDiv.style.width = `100%`; // Define height of map-timeline div
    // console.log(timelineDiv.style.width)
    // timelineDiv.style.width = 0.69 * contentDiv.clientWidth  + "px"; // Define height of map-timeline div
    // mapTimelineResizerDiv.style.width = 0.005 * contentDiv.clientWidth  + "px"; // Define height of map-timeline div
    // mapDiv.style.width = 0.30 * contentDiv.clientWidth  + "px"; // Define height of map-timeline div

    if ([timelineDiv.style.width, mapTimelineResizerDiv.style.width, mapDiv.style.width].includes("")){
        timelineDiv.style.width = "69.5%"; // Define height of map-timeline div
        mapTimelineResizerDiv.style.width = "0.5%"; // Define height of map-timeline div
        mapDiv.style.width = "30%"; // Define height of map-timeline div
    }
    
}

function createHTMLSnapshot(L, contentDiv, dateSliderDivId, dateSliderLabel, dateSliderSettings, dateInputDivId, dateValidationButtonId, dateValidationButtonLabel, mapDivId){
    //  dateSliderSettings = {"min":0, "max":100, "value":0}
    var dateDiv = createDiv(L, "div", {}, null, null);
    var dateSliderLabelDiv = createLabel(L, dateSliderDivId, dateSliderLabel, null, labelContentIsBold = true);
    var dateSliderDiv = createDiv(L, "input", {"type":"range", "id":dateSliderDivId, "min":dateSliderSettings.min, "max":dateSliderSettings.max, "value":dateSliderSettings.value}, null, null);
    var dateInputDiv = createDiv(L, "input", {"type":"date", "id":dateInputDivId}, null, null);
    var dateValidationButtonDiv = createDiv(L, "button", {"id":dateValidationButtonId}, dateValidationButtonLabel, null);

    dateDiv.appendChild(dateSliderLabelDiv);
    dateDiv.appendChild(dateSliderDiv);
    dateDiv.appendChild(dateInputDiv);
    dateDiv.appendChild(dateValidationButtonDiv);

    var mapDiv = createDiv(L, "div", {"id":mapDivId}, null, null);
    contentDiv.appendChild(dateDiv);
    contentDiv.appendChild(mapDiv);
}

function setActionsForEvolution(graphDBRepositoryURI, namedGraphURI, mapLat, mapLon, mapZoom, landmarkNamesDivId, resizerClassName, tileLayerSettings){
    
    var layersToRemove = [];

    // Appel aux fonctions d'initialisation
    var [map, layerControl, tileLayers, overlayLayers] = initLeafletMap(mapDivId, mapLat, mapLon, mapZoom, tileLayerSettings);
    allowMapTimelineResize(resizerClassName, map) ;

    // Afficher la timeline quand on clique sur un bouton (ou entrée dans le drop menu)
    var dropDownMenu = document.getElementById(landmarkNamesDivId);
    dropDownMenu.addEventListener("change", function(event) { changeSelectedLandmark(graphDBRepositoryURI, factsNamedGraphURI, dropDownMenu, map, layersToRemove) ;});

    // Afficher les landmarks dans un menu déroulant
    getLandmarks(graphDBRepositoryURI, namedGraphURI, dropDownMenu) ;
}

function setActionsForSnapshot(
    graphDBRepositoryURI, namedGraphURI,
    mapDivId, mapLat, mapLon, mapZoom,
    certainLayerGroupName, uncertainLayerGroupName,
    dateSliderDivId, dateInputDivId, dateValidatonButtonId,
    startTimeStampSlider, endTimeStampSlider, calendarURI, tileLayerSettings){

    //////////////////////////////////////////////////////////////////

    var layerGroupNames = [certainLayerGroupName, uncertainLayerGroupName];

    var mapDiv = document.getElementById(mapDivId);
    mapDiv.style.height = "800px";
    mapDiv.style.width = "100%";

    // Appel aux fonctions d'initialisation
    var [map, layerControl, tileLayers, overlayLayers] = initLeafletMap(mapDivId, mapLat, mapLon, mapZoom, tileLayerSettings);
    // var overlayLayerGroups = initOverlayLayerGroups(map, layerControl, layerGroupNames);

    // Initialiser la gestion du slider avec les IDs des éléments HTML
    manageTimeSlider(dateSliderDivId, dateInputDivId, startTimeStampSlider, endTimeStampSlider);

    // Après avoir sélectionné une date, afficher le snapshot correspondant
    document.getElementById(dateValidatonButtonId).addEventListener("click", function() {
        displaySnapshotFromSelectedTime(graphDBRepositoryURI, dateInputDivId, calendarURI, namedGraphURI,
            map, layerControl, overlayLayers);
    });

}