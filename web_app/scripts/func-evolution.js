function initTimelineFromLandmark(endpoint, namedGraphURI, uiConfig, mapSettings, landmarkURI){
  var query = getQueryToInitTimeline(landmarkURI, namedGraphURI);

  runSparqlQuery(endpoint, query).then(bindings => {
      var versions = {};

      bindings.forEach(binding => {
        var uri = binding.attrVers.value;

        versions[uri] = {
          ...binding,
          values: []
        };
      });

      configureTimelineFromLandmark(endpoint, uiConfig, mapSettings, versions);
    })
    .catch(err => {
      console.error("SPARQL timeline error:", err);
    });
}

function configureTimelineFromLandmark(endpoint, uiConfig, mapSettings, versions){
  var valuesForQuery = getValuesForQuery("vers", versions);
  var query = getQueryForAttributeVersionValues(valuesForQuery);

  runSparqlQuery(endpoint, query).then(bindings => {
      configureTimeline(uiConfig, versions, bindings, mapSettings);

    })
    .catch(err => {
      console.error("SPARQL timeline config error:", err);
    });
}
  
function configureTimeline(uiConfig, versions, bindings, mapSettings){
  bindings.forEach(binding => {
    var uri = binding.vers.value ;
    versions[uri].values.push(binding.val) ;
  });

  var timelineOptions = {
    scale_factor:1,
    language: uiConfig.lang,
    start_at_slide:1,
    hash_bookmark: false,
    initial_zoom: 0
    } ;

  var timelineHeadline = uiConfig.timeline.headlineLabel ;
  var timelineJson = getTimelineJson(uiConfig, versions, timelineHeadline)
  var timeline = new TL.Timeline(uiConfig.divIds.timeline, timelineJson, timelineOptions) ;
  timeline.on('change', function () { actionsOnTimelineChange(timeline, versions, mapSettings) });
}

function actionsOnTimelineChange(timeline, versions, mapSettings){
  var uri = timeline.current_id;
  var version = versions[uri];
  if (version){
    var geomStyle = {marker:lo.blueMarker, polyline:lo.blueDefaultLineStringStyle, polygon:lo.blueDefaultPolygonStyle}
    addGeometriesOfVersion(version, mapSettings, geomStyle);
  }
}

function getTimelineJson(uiConfig, versions, headline){
  var timelineJson = {"title": {"text":{"headline":headline}}, "events": []} ;

  for (uri in versions){
    var version = versions[uri];
    var feature = createTimelineFeature(uiConfig, version.attrVers, version.attrType, version.values,
      {stamp:version.tStampME, precision:version.tPrecME}, {stamp:version.tStampO, precision:version.tPrecO},
      {stamp:version.tStampMEBefore, precision:version.tPrecMEBefore}, {stamp:version.tStampMEAfter, precision:version.tPrecMEAfter},
      {stamp:version.tStampOBefore, precision:version.tPrecOBefore}, {stamp:version.tStampOAfter, precision:version.tPrecOAfter}
    ) ;
    timelineJson.events.push(feature);
  }

  return timelineJson;
}

function displayLandmarkValidTime(endpoint, namedGraphURI, landmarkURI, landmarkValidTimeDivId){
  var queryValidTimeForLandmark = getQueryValidTimeForLandmark(landmarkURI, namedGraphURI) ;

  runSparqlQuery(endpoint, queryValidTimeForLandmark).then(bindings => {
      insertLandmarkValidTime(landmarkValidTimeDivId, bindings) ;
    })
    .catch(err => {
      console.error("SPARQL timeline config error:", err);
    });
}
  
  function insertLandmarkValidTime(landmarkValidTimeDivId, bindings){
    /**
   * Displays the valid time for a landmark based on the timestamp data in the provided bindings.
   *
   * This function iterates over each binding in the given array of bindings, calculates the valid time for the landmark 
   * using the provided timestamps and precision values, and updates the inner HTML of a specific div element with the 
   * calculated valid time label.
   *
   * @param {Array} bindings - An array of objects, where each object contains timestamp data and associated precision values. 
   * Each object should have (or not) the following properties:
   *   - tStampApp: Timestamp for the application
   *   - tPrecApp: Precision of the tStampApp
   *   - tStampDis: Timestamp for the disapplication
   *   - tPrecDis: Precision of the tStampDis
   *   - tStampAppBefore: Timestamp for the application before
   *   - tPrecAppBefore: Precision of the tStampAppBefore
   *   - tStampAppAfter: Timestamp for the application after
   *   - tPrecAppAfter: Precision of the tStampAppAfter
   *   - tStampDisBefore: Timestamp for the disapplication before
   *   - tPrecDisBefore: Precision of the tStampDisBefore
   *   - tStampDisAfter: Timestamp for the disapplication after
   *   - tPrecDisAfter: Precision of the tStampDisAfter
   * 
   * @returns {void} - The function does not return any value. It updates the inner HTML of a div element with the 
   *                   calculated valid time label for each binding in the provided list.
   */
  
    bindings.forEach(binding => {
      var times = getValidTimeForLandmark(
        {stamp:binding.tStampApp, precision:binding.tPrecApp}, {stamp:binding.tStampDis, precision:binding.tPrecDis},
        {stamp:binding.tStampAppBefore, precision:binding.tPrecAppBefore}, {stamp:binding.tStampAppAfter, precision:binding.tPrecAppAfter},
        {stamp:binding.tStampDisBefore, precision:binding.tPrecDisBefore}, {stamp:binding.tStampDisAfter, precision:binding.tPrecDisAfter}
      );
      var validTimeForLandmarkLabel = getValidTimeForLandmarkLabel(times.appTime, times.disTime) ;
      var landmarkValidTimeDiv = document.getElementById(landmarkValidTimeDivId) ;
      landmarkValidTimeDiv.innerHTML = validTimeForLandmarkLabel ;
    });
  }
  
  function createTimelineText(attrVersion, attrVersionValues){
  var values = [] ;
  attrVersionValues.forEach(element => {
    values.push(element.value);
  });

  var headline = attrVersion.value.replace("http://rdf.geohistoricaldata.org/id/address/","")
  var headline = headline.replace("facts/","")
  var headline = headline.replace("factoids/","")
  var text = { "headline": headline, "text": values.join("<br>") };

  return text ;
}

function changeSelectedLandmark(graphDBRepositoryURI, namedGraphURI, uiConfig, mapSettings){
  var dropDownMenu = document.getElementById(uiConfig.divIds.landmarkSelectionSuggestions);
  var landmarkURI = dropDownMenu.value;
  displayLandmarkValidTime(graphDBRepositoryURI, namedGraphURI, landmarkURI, uiConfig.divIds.landmarkValidTime);
  initTimelineFromLandmark(graphDBRepositoryURI, namedGraphURI, uiConfig, mapSettings, landmarkURI);
}

function buildTypesDataMap(bindings, valueVar, labelVar){
  var map = new Map();

  bindings.forEach(b => {
    var type = b[valueVar].value;
    var label = b[labelVar]?.value || type;

    map.set(type, {
      label
    });
  });

  return map;
}

function buildLandmarkDataMap(typeBindings, landmarkBindings){
  var map = new Map();

  // 1. Initialisation des types
  typeBindings.forEach(b => {
    var type = b.lmType.value;
    var label = b.lmTypeLabel?.value || type;

    map.set(type, {
      label,
      landmarks: new Map()
    });
  });

  // 2. Remplissage des landmarks
  landmarkBindings.forEach(b => {
    var type = b.lmType.value;
    var lm = b.lm.value;
    var lmLabel = b.lmLabel.value;
    var relatum = b.relatumLabel?.value;

    if (!map.has(type)) return;

    var lmMap = map.get(type).landmarks;

    if (!lmMap.has(lm)) {
      // Au premier passage pour ce landmark, on initialise avec le label de base
      lmMap.set(lm, {
        label: lmLabel,
        searchLabel: lmLabel, // Valeur par défaut (si pas de relatum)
        relatums: []
      });
    }

    var currentLm = lmMap.get(lm);

    if (relatum) {
      // Si c'est le premier relatum qu'on rencontre pour ce landmark
      if (currentLm.relatums.length === 0) {
        currentLm.searchLabel = `${lmLabel}, ${relatum}`;
      }
      
      currentLm.relatums.push(relatum);
    }
  });

  return map;
}

function populateDropdown(dropDown, options, placeholder){
  dropDown.innerHTML = "";
  dropDown.appendChild(createOptionDiv("", placeholder));

  options.forEach(opt => {
    dropDown.appendChild(createOptionDiv(opt.value, opt.label));
  });
}

function populateTypeDropdown(dropDown, dataMap, placeholder){
  var options = [...dataMap.entries()].map(([value, data]) => ({
    value,
    label: data.label
  }));

  populateDropdown(dropDown, options, placeholder);
}

function populateLandmarkDropdown(dropDown, dataMap, selectedType, placeholder){
  if (!dataMap.has(selectedType)) {
    populateDropdown(dropDown, [], placeholder);
    return;
  }

  var options = [...dataMap.get(selectedType).landmarks.entries()].map(([value, data]) => {
    var label = data.searchLabel;

    // if (data.relatums.length > 0){
    //   label += " (" + data.relatums.join(", ") + ")";
    // }

    return { value, label };
  });

  populateDropdown(dropDown, options, placeholder);
}

function getDefaultLandmarks(dataMap, selectedType, limit){

  if (!dataMap.has(selectedType)) return [];

  return [...dataMap.get(selectedType).landmarks.entries()]
    .slice(0, limit)
    .map(([value, data]) => {
      let label = data.searchLabel;
      return { value, label };
    });
}

function searchLandmarks(dataMap, selectedType, query, limit = 20){
  console.log(dataMap);
  if (!dataMap.has(selectedType)) return [];

  var lmMap = dataMap.get(selectedType).landmarks;
  var q = removeDiacritics(query).toLowerCase();

  return [...lmMap.entries()]
    .filter(([_, data]) => {
      return removeDiacritics(data.searchLabel).toLowerCase().includes(q);
    })
    .slice(0, limit)
    .map(([value, data]) => {
      let label = data.searchLabel;
      return { value, label };
    });
}


function debounce(fn, delay){
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), delay);
  };
}

function setupLandmarkAutocomplete(
  uiConfig,
  dataMap,
  lmTypeDropDown,
  lmInput,
  lmSuggestionsDropDown,
){
  var handler = debounce((e) => {
    var query = lmInput.value;
    var selectedType = lmTypeDropDown.value;

    if (!selectedType){
      console.log("No type selected, not searching");
      lmSuggestionsDropDown.innerHTML = "";
      return;
    }

    var results;

    // Cas 1 : moins de 2 caractères → suggestions par défaut
    if (query.length < 2){
      results = getDefaultLandmarks(dataMap, selectedType, uiConfig.dropDownMenu.limit);

    } else {
      // Cas 2 : recherche normale
      results = searchLandmarks(dataMap, selectedType, query, uiConfig.dropDownMenu.limit);
    }

    displaySuggestions(lmSuggestionsDropDown, results);

  }, 200);

  lmInput.addEventListener("input", handler);
  lmTypeDropDown.addEventListener("change", handler);
  lmTypeDropDown.addEventListener("change", function(){
    // Clear input and suggestions when type changes
    lmInput.value = "";
    lmSuggestionsDropDown.innerHTML = "";
  });

  // BONUS : afficher suggestions au focus
  lmInput.addEventListener("focus", () => {
    var selectedType = lmTypeDropDown.value;

    if (!selectedType) return;

    var results = getDefaultLandmarks(dataMap, selectedType, uiConfig.dropDownMenu.limit);
    displaySuggestions(lmSuggestionsDropDown, results);
  });
}

function displaySuggestions(dropDown, results){
  dropDown.innerHTML = "";

  results.forEach(r => {
    var option = document.createElement("option");

    option.value = r.value;      // URI
    option.textContent = r.label; // label

    dropDown.appendChild(option);
  });
}

function displayLandmarksToSelectForEvolution(
  endpoint, namedGraphURI, uiConfig,
  lmTypeDropDown, lmDropDown, lmSuggestionsDropDown,
  selectTypeMessage, selectLmMessage
){
  var queryLandmarkTypes = getQueryForLandmarkTypes(namedGraphURI);
  var queryAttrTypes = getQueryForAttributeTypes(namedGraphURI);
  var queryLandmarks = getQueryForLandmarks(namedGraphURI);

  Promise.all([
    runSparqlQuery(endpoint, queryLandmarkTypes),
    runSparqlQuery(endpoint, queryAttrTypes),
    runSparqlQuery(endpoint, queryLandmarks)
  ]).then(([landmarkTypeBindings, attrTypeBindings, landmarkBindings]) => {

    var attrTypesDataMap = buildTypesDataMap(attrTypeBindings, "attrType", "attrTypeLabel");
    var lmTypesDataMap = buildTypesDataMap(landmarkTypeBindings, "lmType", "lmTypeLabel");
    uiConfig.types.attributes = attrTypesDataMap;
    uiConfig.types.landmarks = lmTypesDataMap;

    var lmDataMap = buildLandmarkDataMap(landmarkTypeBindings, landmarkBindings);
    populateTypeDropdown(lmTypeDropDown, lmDataMap, selectTypeMessage);
    setupLandmarkAutocomplete(uiConfig, lmDataMap, lmTypeDropDown, lmDropDown, lmSuggestionsDropDown);

    // lmTypeDropDown.addEventListener("change", (e) => {
    //   populateLandmarkDropdown(lmDropDown, dataMap, e.target.value, selectLmMessage
    //   );
    // });

  });
}
