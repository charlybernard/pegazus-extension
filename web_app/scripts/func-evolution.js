function initTimelineFromLandmark(graphDBRepositoryURI, landmarkURI, namedGraphURI, timelineDivId, mapSettings){

    var query = getQueryToInitTimeline(landmarkURI, namedGraphURI) ;
    var versions = {} ;

    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":query}
    }).done((promise) => {
      promise.results.bindings.forEach(binding => {
        var uri = binding.attrVers.value;
        binding.values = []
        versions[uri] = binding ;
      });
      configureTimelineFromLandmark(timelineDivId, versions, mapSettings) ;   
    });
  
  };

function configureTimelineFromLandmark(timelineDivId, versions, mapSettings){
  var valuesForQuery = getValuesForQuery("vers", versions) ;
  var query = getQueryForAttributeVersionValues(valuesForQuery);

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":query}
    }).done((promise) => {
      configureTimeline(timelineDivId, versions, promise.results.bindings, mapSettings);
    });
}
  
function configureTimeline(timelineDivId, versions, bindings, mapSettings){
  bindings.forEach(binding => {
    var uri = binding.vers.value ;
    versions[uri].values.push(binding.val) ;
  });

  var timelineOptions = {
    scale_factor:1,
    language:'fr',
    start_at_slide:1,
    hash_bookmark: false,
    initial_zoom: 0
    } ;

  var timelineHeadline = "Attributs de l'entité géographique" ;
  var timelineJson = getTimelineJson(versions, timelineHeadline)
  var timeline = new TL.Timeline(timelineDivId, timelineJson, timelineOptions) ;
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

function getTimelineJson(versions, headline){
  var timelineJson = {"title": {"text":{"headline":headline}}, "events": []} ;

  for (uri in versions){
    var version = versions[uri];
    var feature = createTimelineFeature(version.attrVers, version.attrType, version.values,
      {stamp:version.tStampME, precision:version.tPrecME}, {stamp:version.tStampO, precision:version.tPrecO},
      {stamp:version.tStampMEBefore, precision:version.tPrecMEBefore}, {stamp:version.tStampMEAfter, precision:version.tPrecMEAfter},
      {stamp:version.tStampOBefore, precision:version.tPrecOBefore}, {stamp:version.tStampOAfter, precision:version.tPrecOAfter}
    ) ;
    timelineJson.events.push(feature);
  }

  return timelineJson
}

function displayLandmarkValidTime(landmarkURI, namedGraphURI, landmarkValidTimeDivId){
  var queryValidTimeForLandmark = getQueryValidTimeForLandmark(landmarkURI, namedGraphURI) ;

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidTimeForLandmark}
  }).done((promise) => {
    insertLandmarkValidTime(landmarkValidTimeDivId, promise.results.bindings);
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

function changeSelectedLandmark(graphDBRepositoryURI, namedGraphURI, dropDownMenu, mapSettings, timelineDivId, landmarkValidTimeDivId){
  var landmarkURI = dropDownMenu.value;
  displayLandmarkValidTime(landmarkURI, namedGraphURI, landmarkValidTimeDivId);
  initTimelineFromLandmark(graphDBRepositoryURI, landmarkURI, namedGraphURI, timelineDivId, mapSettings);
}
  
function displayLandmarksInDropDownMenu(graphDBRepositoryURI, namedGraphURI, dropDownMenu){
  var query = getQueryForLandmarks(namedGraphURI);

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":query}
  }).done((promise) => {
    insertLandmarksInDropDownMenu(dropDownMenu, promise.results.bindings)
  })
}
  
function insertLandmarksInDropDownMenu(dropDownMenu, bindings){
  var option = createOptionDiv("", "Sélectionnez une valeur") ;
  var uris = [];
  var optGroupUris = {};
  dropDownMenu.appendChild(option) ;
  bindings.forEach(binding => {
    var uri = binding.lm.value ;
    var groupUri = binding.lmType.value ;

    if (!uris.includes(uri)){

      var option = createOptionDiv(binding.lm.value, binding.lmLabel.value) ;
      if (!Object.keys(optGroupUris).includes(groupUri)){
        var groupUri = binding.lmType.value ;
        var optgroup = createOptionGroupDiv(groupUri, binding.lmTypeLabel.value) ;
        dropDownMenu.appendChild(optgroup) ;
        optGroupUris[groupUri] = optgroup ;
      }else{
        var optgroup = optGroupUris[binding.lmType.value] ;
      }
      optgroup.appendChild(option) ;
      uris.push(uri) ;
    }

  });
}