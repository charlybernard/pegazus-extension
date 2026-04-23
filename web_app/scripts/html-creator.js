function createMainHTML(L, endpoint, uiConfig){
    document.body.style.height = "100vh";
    document.body.style.width = "100vw";

    var radioInputConfig = {
        name: uiConfig.divIds.radioInputButtons,
        label: uiConfig.labels.radioInput,
        id: uiConfig.divIds.radioInput,
        values: uiConfig.radioInputs.values
    }

    var selectionDiv = createDiv(L, "div", {"id":uiConfig.divIds.selection}, null, null);
    var inputRadioDiv = createInputRadioDiv(L, radioInputConfig);
    var contentDiv = createDiv(L, "div", {"id":uiConfig.divIds.content}, null, null);
    var graphDiv = createDiv(L, "div", {"id":uiConfig.divIds.graph}, null, null);
    
    // --- Sélection d'un graphe au lancement ---
    var graphDiv = createHTMLGraph(L, uiConfig) ;

    document.body.appendChild(selectionDiv);
    document.body.appendChild(contentDiv);

    selectionDiv.appendChild(graphDiv);
    selectionDiv.appendChild(inputRadioDiv);

    var contentDivHeightInt = 100*(document.body.clientHeight - inputRadioDiv.clientHeight)/document.body.clientHeight;
    contentDiv.style.height = `${contentDivHeightInt}em`
    
    createStyleInputRadioDiv(uiConfig);

    // --- Affichage des graphes dans un menu déroulant ---
    dropDownMenu = document.getElementById(uiConfig.divIds.graphSelection) ;
    displayGraphsInDropDownMenu(endpoint, dropDownMenu, lang=uiConfig.lang) ;
}


// function createMainHTML(L, radioInputs, contentDivId, selectionDivId, graphSettings, mapMessages, lang="fr"){
//     document.body.style.height = "100vh";
//     document.body.style.width = "100vw";

//     var selectionDiv = createDiv(L, "div", {"id":selectionDivId}, null, null);
//     var inputRadioDiv = createInputRadioDiv(L, radioInputs);
//     var contentDiv = createDiv(L, "div", {"id":contentDivId}, null, null);
//     var graphDiv = createDiv(L, "div", {"id":graphSettings.divId}, null, null);
    
//     // --- Sélection d'un graphe au lancement ---
//     var graphDiv = createHTMLGraph(L, graphSettings.divId, graphSettings.selectionDivId, graphSettings.selectionLabel) ;

//     document.body.appendChild(selectionDiv);
//     document.body.appendChild(contentDiv);

//     selectionDiv.appendChild(graphDiv);
//     selectionDiv.appendChild(inputRadioDiv);

//     var contentDivHeightInt = 100*(document.body.clientHeight - inputRadioDiv.clientHeight)/document.body.clientHeight;
//     contentDiv.style.height = `${contentDivHeightInt}em`

//     createStyleInputRadioDiv(inputRadioDiv);

//     // --- Affichage des graphes dans un menu déroulant ---
//     dropDownMenu = document.getElementById(graphSettings.selectionDivId) ;
//     // displayGraphsInDropDownMenu(endpoint, dropDownMenu, mapMessages.graphSelectValue, lang=lang) ;
// }

function createStyleInputRadioDiv(uiConfig){
  
  var inputRadioDiv = document.getElementById(uiConfig.divIds.radioInput);
  var inputRadioButtons = document.getElementById(uiConfig.divIds.radioInputButtons);

  inputRadioDiv.style.display = "flex";
  inputRadioDiv.style.flexDirection = "row";

  inputRadioButtons.style.display = "flex";
  inputRadioButtons.style.flexDirection = "row";

}

function createHTMLEvolution(L, uiConfig){
    var landmarkNamesDiv = createDiv(L, "div", {"id":uiConfig.divIds.landmarkSelection}, null, null);
    var landmarkNamesLabelDiv = createLabel(L, uiConfig.divIds.landmarkTypeNamesLabel, uiConfig.labels.landmarkSelection, null, labelContentIsBold = true);
    var landmarkNamesInputDiv = createDiv(L, "input", {"type": "text", "id":uiConfig.divIds.landmarkNames, "placeholder":uiConfig.labels.searchLandmarksPlaceholder, "autocomplete":"off"}, null, null);
    // var landmarkNamesSelectDiv = createDiv(L, "select", {"name":uiConfig.divIds.landmarkNames, "id":uiConfig.divIds.landmarkNames}, null, null);
    var landmarkTypeNamesSelectDiv = createDiv(L, "select", {"name":uiConfig.divIds.landmarkTypeNames, "id":uiConfig.divIds.landmarkTypeNames}, null, null);
    var landmarkSuggestionsDiv = createDiv(L, "select", {"id": uiConfig.divIds.landmarkSelectionSuggestions}, null, null);
    var landmarkValidationButtonDiv = createDiv(L, "button", {"id":uiConfig.divIds.landmarkValidationButton}, uiConfig.labels.validationButton, null);

    landmarkNamesDiv.appendChild(landmarkNamesLabelDiv);
    landmarkNamesDiv.appendChild(landmarkTypeNamesSelectDiv);
    // landmarkNamesDiv.appendChild(landmarkNamesSelectDiv);
    landmarkNamesDiv.appendChild(landmarkSuggestionsDiv);
    landmarkNamesDiv.appendChild(landmarkNamesInputDiv);
    landmarkNamesDiv.appendChild(landmarkValidationButtonDiv);

    var landmarkValidTimeDiv = createDiv(L, "div", {"id":uiConfig.divIds.landmarkValidTime, "style": "display: flex; flex-direction: row;"}, null, null);

    var mapTimelineDiv = createDiv(L, "div", {"id":uiConfig.divIds.mapTimeline}, null, null);
    var timelineDiv = createDiv(L, "div", {"id":uiConfig.divIds.timeline}, null, null);
    var mapTimelineResizerDiv = createDiv(L, "div", {"id":uiConfig.divIds.mapTimelineResizer, "class":uiConfig.classNames.resizer}, null, null);
    var mapDiv = createDiv(L, "div", {"id":uiConfig.divIds.map}, null, null);
    mapTimelineDiv.appendChild(timelineDiv);
    mapTimelineDiv.appendChild(mapTimelineResizerDiv);
    mapTimelineDiv.appendChild(mapDiv);

    selectDiv.appendChild(landmarkNamesDiv);
    selectDiv.appendChild(landmarkValidTimeDiv);
    contentDiv.appendChild(mapTimelineDiv);

    getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);

    window.addEventListener('resize', function(){
        getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);
    })
    
}

// function createHTMLEvolution(L, divId, contentDiv, selectDiv, landmarkTypeNamesDivId, landmarkNamesDivId, landmarkNamesLabel, landmarkValidTimeDivId,
//     mapTimelineDivId, timelineDivId, mapDivId, mapTimelineResizerDivId, resizerClassName){
//     var landmarkNamesDiv = createDiv(L, "div", {"id":divId}, null, null);
//     var landmarkNamesLabelDiv = createLabel(L, landmarkNamesDivId, landmarkNamesLabel, null, labelContentIsBold = true);
//     var landmarkNamesInputDiv = createDiv(L, "input", {"type": "text", "id":landmarkNamesDivId, "placeholder":"Rechercher un landmark...", "autocomplete":"off"}, null, null);
//     var landmarkNamesSelectDiv = createDiv(L, "select", {"name":landmarkNamesDivId, "id":landmarkNamesDivId}, null, null);
//     var landmarkTypeNamesSelectDiv = createDiv(L, "select", {"name":landmarkTypeNamesDivId, "id":landmarkTypeNamesDivId}, null, null);
//     var landmarkSuggestionsDiv = createDiv(L, "select", {"id": "landmarkSuggestions", "class": "suggestions"}, null, null);
//     var landmarkValidationButtonDiv = createDiv(L, "button", {"id":landmarkValidationButtonDivLabel}, landmarkValidationButtonLabel, null);

//     landmarkNamesDiv.appendChild(landmarkNamesLabelDiv);
//     landmarkNamesDiv.appendChild(landmarkTypeNamesSelectDiv);
//     // landmarkNamesDiv.appendChild(landmarkNamesSelectDiv);
//     landmarkNamesDiv.appendChild(landmarkNamesInputDiv);
//     landmarkNamesDiv.appendChild(landmarkSuggestionsDiv);

//     var landmarkValidTimeDiv = createDiv(L, "div", {"id":landmarkValidTimeDivId}, null, null);

//     var mapTimelineDiv = createDiv(L, "div", {"id":mapTimelineDivId}, null, null);
//     var timelineDiv = createDiv(L, "div", {"id":timelineDivId}, null, null);
//     var mapTimelineResizerDiv = createDiv(L, "div", {"id":mapTimelineResizerDivId, "class":resizerClassName}, null, null);
//     var mapDiv = createDiv(L, "div", {"id":mapDivId}, null, null);
//     mapTimelineDiv.appendChild(timelineDiv);
//     mapTimelineDiv.appendChild(mapTimelineResizerDiv);
//     mapTimelineDiv.appendChild(mapDiv);

//     selectDiv.appendChild(landmarkNamesDiv);
//     selectDiv.appendChild(landmarkValidTimeDiv);
//     contentDiv.appendChild(mapTimelineDiv);

//     getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);

//     window.addEventListener('resize', function(){
//         getStyleForHTMLEvolution(contentDiv, landmarkNamesDiv, landmarkValidTimeDiv, mapTimelineDiv, timelineDiv, mapTimelineResizerDiv, mapDiv);
//     })
    
// }

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

// function createHTMLSnapshot(
//     L, divId, contentDiv, selectDiv,
//     dateSliderDivId, dateSliderLabel, dateSliderSettings, dateInputDivId, dateValidationButtonId, dateValidationButtonLabel, mapDivId){
//     //  dateSliderSettings = {"min":0, "max":100, "value":0}
//     var dateDiv = createDiv(L, "div", {"id":divId}, null, null);
//     var dateSliderLabelDiv = createLabel(L, dateSliderDivId, dateSliderLabel, null, labelContentIsBold = true);
//     var dateSliderDiv = createDiv(L, "input", {"type":"range", "id":dateSliderDivId, "min":dateSliderSettings.min, "max":dateSliderSettings.max, "value":dateSliderSettings.value}, null, null);
//     var dateInputDiv = createDiv(L, "input", {"type":"date", "id":dateInputDivId}, null, null);
//     var dateValidationButtonDiv = createDiv(L, "button", {"id":dateValidationButtonId}, dateValidationButtonLabel, null);

//     dateDiv.appendChild(dateSliderLabelDiv);
//     dateDiv.appendChild(dateSliderDiv);
//     dateDiv.appendChild(dateInputDiv);
//     dateDiv.appendChild(dateValidationButtonDiv);

//     var mapDiv = createDiv(L, "div", {"id":mapDivId}, null, null);
//     selectDiv.appendChild(dateDiv);
//     contentDiv.appendChild(mapDiv);
// }

function createHTMLSnapshot(L, uiConfig){
    var dateDiv = createDiv(L, "div", {"id":uiConfig.divIds.dateSelection}, null, null);
    var dateSliderLabelDiv = createLabel(L, uiConfig.divIds.dateSlider, uiConfig.labels.dateSelection, null, labelContentIsBold = true);
    var dateSliderDiv = createDiv(L, "input", {"type":"range", "id":uiConfig.divIds.dateSlider, "min":uiConfig.dateSlider.min, "max":uiConfig.dateSlider.max, "value":uiConfig.dateSlider.value}, null, null);
    var dateInputDiv = createDiv(L, "input", {"type":"date", "id":uiConfig.divIds.dateInput}, null, null);
    var dateValidationButtonDiv = createDiv(L, "button", {"id":uiConfig.divIds.dateValidationButton}, uiConfig.labels.validationButton, null);

    dateDiv.appendChild(dateSliderLabelDiv);
    dateDiv.appendChild(dateSliderDiv);
    dateDiv.appendChild(dateInputDiv);
    dateDiv.appendChild(dateValidationButtonDiv);

    var mapDiv = createDiv(L, "div", {"id":uiConfig.divIds.map}, null, null);
    selectDiv.appendChild(dateDiv);
    contentDiv.appendChild(mapDiv);
}

function extractGraphs(bindings){
    return bindings.map(binding => {
            // Récupère le label ou fallback sur la dernière partie de l'URI
            var uri = binding.graph.value;
            var label = binding.label ? binding.label.value : uri.split('/').filter(Boolean).pop();
            return { uri, label };
        });
}


function createHTMLGraph(L, uiConfig){
    var graphNamesDiv = createDiv(L, "div", {"id":uiConfig.divIds.graph}, null, null);
    var graphNamesSelectDivLabel = createLabel(L, uiConfig.divIds.graphSelection, uiConfig.labels.graphSelection, null, labelContentIsBold = true);
    var graphNamesSelectDiv = createDiv(L, "select", {"name":uiConfig.divIds.graphSelection, "id":uiConfig.divIds.graphSelection}, null, null);
    graphNamesDiv.appendChild(graphNamesSelectDivLabel);
    graphNamesDiv.appendChild(graphNamesSelectDiv);

    return graphNamesDiv;   
}

function displayGraphsInDropDownMenu(endpoint, dropDownMenu, lang="fr"){
  var query = getQueryForGraphs(lang);

  $.ajax({
    url: endpoint,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":query}
  }).done((promise) => {
    insertGraphsInDropDownMenu(dropDownMenu, promise.results.bindings);
  })
}

function insertGraphsInDropDownMenu(dropDownMenu, bindings) {

  // reset
  dropDownMenu.innerHTML = "";

  var uris = [];

  bindings.forEach((binding, index) => {

    var graph = binding.graph.value;

    var gLabel = (binding.label && binding.label.value)
      ? binding.label.value
      : graph.split(/[/#]/).filter(Boolean).pop();

    var option = createOptionDiv(graph, gLabel);

    // ✅ sélectionner automatiquement le premier
    if (index === 0) {
      option.selected = true;
    }

    dropDownMenu.appendChild(option);
    uris.push(graph);

  });

  return uris;
}

function selectGraphs(L, endpoint, lang = "fr", graphSelectionDivId, graphSelectionLabel, mapMessages, selectDiv) {

    var query = getQueryForGraph(lang);

    $.ajax({
        url: endpoint,
        headers: { "Accept": "application/sparql-results+json" },
        contentType: "application/sparql-results+json",
        dataType: "json",
        data: { "query": query }
    }).done((response) => {
        var graphURIs = extractGraphs(response.results.bindings);
        var selectGraphDiv = createHTMLGraphSelection(L, graphURIs, graphSelectionDivId, graphSelectionLabel, graphSelectionSelectValue);   
        selectDiv.appendChild(selectGraphDiv); 
    }).fail((err) => {
        console.error("Erreur lors de la récupération des graphes:", err);
        alert("Impossible de récupérer les graphes.");
    });
}

// function setActionsForEvolution(
//     endpoint, namedGraphURI,
//     mapLat, mapLon, mapZoom, mapMessages,
//     landmarkTypeNamesDivId, landmarkNamesDivId, timelineDivId, landmarkValidTimeDivId,
//     resizerClassName, tileLayerSettings){
    
//     // Appel aux fonctions d'initialisation
//     var mapSettings = initLeafletMap(mapDivId, mapLat, mapLon, mapZoom, tileLayerSettings, mapMessages);
//     allowMapTimelineResize(resizerClassName, mapSettings.map) ;

//     // Afficher la timeline quand on clique sur un bouton (ou entrée dans le drop menu)
//     var landmarkMenu = document.getElementById(landmarkNamesDivId);
//     var landmarkTypeMenu = document.getElementById(landmarkTypeNamesDivId);
//     landmarkMenu.addEventListener("change", function() {
//         changeSelectedLandmark(endpoint, namedGraphURI, landmarkMenu, mapSettings, timelineDivId, landmarkValidTimeDivId) ;
//     });

//     // Afficher les landmarks dans un menu déroulant
//     displayLandmarksToSelectForEvolution(endpoint, namedGraphURI,
//         landmarkTypeMenu, landmarkMenu,
//         mapSettings.messages.landmarkTypeSelectValue, mapSettings.messages.landmarkSelectValue);
// }

function setActionsForEvolution(endpoint, namedGraphURI, uiConfig){
    // Appel aux fonctions d'initialisation
    var mapSettings = initLeafletMap(uiConfig.divIds.map, uiConfig.map.lat, uiConfig.map.lon, uiConfig.map.zoom, uiConfig.map.tileLayers, uiConfig.map.messages);
    allowMapTimelineResize(uiConfig.classNames.resizer, mapSettings.map) ;

    // Afficher la timeline quand on clique sur un bouton (ou entrée dans le drop menu)
    var landmarkMenu = document.getElementById(uiConfig.divIds.landmarkNames);
    var landmarkTypeMenu = document.getElementById(uiConfig.divIds.landmarkTypeNames);
    var landmarkSuggestionsMenu = document.getElementById(uiConfig.divIds.landmarkSelectionSuggestions);
    // landmarkMenu.addEventListener("change", function() {
    //     changeSelectedLandmark(endpoint, namedGraphURI, uiConfig, mapSettings) ;
    // });

    document.getElementById(uiConfig.divIds.landmarkValidationButton).addEventListener("click", function() {
        changeSelectedLandmark(endpoint, namedGraphURI, uiConfig, mapSettings) ;
    });

    // Afficher les landmarks dans un menu déroulant
    displayLandmarksToSelectForEvolution(
        endpoint, namedGraphURI, uiConfig,
        landmarkTypeMenu, landmarkMenu, landmarkSuggestionsMenu,
        mapSettings.messages.landmarkTypeSelectValue, mapSettings.messages.landmarkSelectValue);
}

// function setActionsForSnapshot(
//     endpoint, namedGraphURI,
//     mapDivId, mapLat, mapLon, mapZoom, mapMessages,
//     certainLayerGroupName, uncertainLayerGroupName,
//     dateSliderDivId, dateInputDivId, dateValidatonButtonId,
//     startTimeStampSlider, endTimeStampSlider, timeDelay, calendarURI, tileLayerSettings){

//     //////////////////////////////////////////////////////////////////

//     var layerGroupNames = [certainLayerGroupName, uncertainLayerGroupName];

//     var mapDiv = document.getElementById(mapDivId);
//     mapDiv.style.height = "90%";
//     mapDiv.style.width = "100%";

//     // Appel aux fonctions d'initialisation
//     var mapSettings = initLeafletMap(mapDivId, mapLat, mapLon, mapZoom, tileLayerSettings, mapMessages, undefined, undefined, true, ['polygon', 'rectangle']);
//     initInfoControl(mapSettings);

//     // Initialiser la gestion du slider avec les IDs des éléments HTML
//     manageTimeSlider(dateSliderDivId, dateInputDivId, startTimeStampSlider, endTimeStampSlider);

//     // Après avoir sélectionné une date, afficher le snapshot correspondant
//     document.getElementById(dateValidatonButtonId).addEventListener("click", function() {
//         displaySnapshotFromSelectedTime(endpoint, dateInputDivId, calendarURI, timeDelay, namedGraphURI, mapSettings);
//     });
// }

function setActionsForSnapshot(endpoint, namedGraphURI, uiConfig){
    var layerGroupNames = [uiConfig.layers.certain, uiConfig.layers.uncertain];

    var mapDiv = document.getElementById(uiConfig.divIds.map);
    mapDiv.style.height = uiConfig.map.style.height;
    mapDiv.style.width = uiConfig.map.style.width;

    // Appel aux fonctions d'initialisation
    var mapSettings = initLeafletMap(uiConfig.divIds.map, uiConfig.map.lat, uiConfig.map.lon, uiConfig.map.zoom, uiConfig.map.tileLayers, uiConfig.map.messages, undefined, undefined, true, ['polygon', 'rectangle']);
    initInfoControl(mapSettings);

    // Initialiser la gestion du slider avec les IDs des éléments HTML
    manageTimeSlider(uiConfig.divIds.dateSlider, uiConfig.divIds.dateInput, uiConfig.timeline.startTimestamp, uiConfig.timeline.endTimestamp);

    // Après avoir sélectionné une date, afficher le snapshot correspondant
    document.getElementById(uiConfig.divIds.dateValidationButton).addEventListener("click", function() {
        displaySnapshotFromSelectedTime(endpoint, uiConfig.divIds.dateInput, uiConfig.calendar.uri, uiConfig.timeline.timeDelay, namedGraphURI, mapSettings);
        mapSettings.drawnItems.clearLayers() ; // Supprimer les éventuelles zones de recherche dessinées sur la carte
    });
}

function handleRadioChange(L, uiConfig, endpoint, namedGraphURI){

  var querySelectorSetting =
    `input[name="${uiConfig.divIds.radioInputButtons}"]:checked`;

  var selectedValue =
    document.querySelector(querySelectorSetting).value;

  clearDiv(contentDiv);

  removeElementsByIds([
    uiConfig.divIds.landmarkSelection,
    uiConfig.divIds.landmarkValidTime,
    uiConfig.divIds.dateSelection
  ]);

  if (selectedValue === uiConfig.labels.landmarkEvolution){
    createHTMLEvolution(L, uiConfig);
    setActionsForEvolution(endpoint, namedGraphURI, uiConfig);
  } else if (selectedValue === uiConfig.labels.snapshot){
    createHTMLSnapshot(L, uiConfig);
    setActionsForSnapshot(endpoint, namedGraphURI, uiConfig);
  }
}