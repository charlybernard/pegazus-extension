function getGraphDBRepositoryURI(graphDBURI, graphName){
  return graphDBURI + "/repositories/" + graphName ;
}

function getNamedGraphURI(graphDBURI, graphName, namedGraphName){
  return graphDBURI + "/repositories/" + graphName + "/rdf-graphs/" + namedGraphName ;
}

function initTimeline(graphDBRepositoryURI, landmarkURI, namedGraphURI, map, layersToRemove){

  var queryToInitTimeline = getQueryToInitTimeline(landmarkURI, namedGraphURI) ;
  var queryValidTimeForLandmark = getQueryValidTimeForLandmark(landmarkURI, namedGraphURI) ;

  var timelinejson = {"title": {"text":{"headline":'Attributs pour le landmark'}}, "events": []}

  var options = {
    scale_factor:1,
    language:'fr',
    start_at_slide:1,
    hash_bookmark: false,
    initial_zoom: 0
    }

  var versions = {} ;
  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidTimeForLandmark}
  }).done((promise) => {
    $.each(promise.results.bindings, function(i,bindings){
      var times = getValidTimeForLandmark(
        {stamp:bindings.tStampApp, precision:bindings.tPrecApp}, {stamp:bindings.tStampDis, precision:bindings.tPrecDis},
        {stamp:bindings.tStampAppBefore, precision:bindings.tPrecAppBefore}, {stamp:bindings.tStampAppAfter, precision:bindings.tPrecAppAfter},
        {stamp:bindings.tStampDisBefore, precision:bindings.tPrecDisBefore}, {stamp:bindings.tStampDisAfter, precision:bindings.tPrecDisAfter}
      );
      var validTimeForLandmarkLabel = getValidTimeForLandmarkLabel(times.appTime, times.disTime) ;
      var landmarkValidTimeDiv = document.getElementById(landmarkValidTimeDivId) ;
      landmarkValidTimeDiv.innerHTML = validTimeForLandmarkLabel ;
    });
  });

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryToInitTimeline}
  }).done((promise) => {
    //Create Timeline JS JSON
    //INIT TimelineJson END
    //Iter on features
    
    $.each(promise.results.bindings, function(i,bindings){
      var uri = bindings.attrVers.value;
      bindings.values = []
      versions[uri] = bindings ;
      });
  }).done((promise) => {
    var valuesForQuery = getValuesForQuery("vers", versions) ;
    var query = getQueryForAttributeVersionValues(valuesForQuery);

    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":query}
      }).done((promise) => {
        $.each(promise.results.bindings, function(i,bindings){
          var uri = bindings.vers.value ;
          versions[uri].values.push(bindings.val) ;
        }) ;
      }).done((promise) => {
        for (uri in versions){
          var version = versions[uri];
          var feature = createTimelineFeature(version.attrVers, version.attrType, version.values,
            {stamp:version.tStampME, precision:version.tPrecME}, {stamp:version.tStampO, precision:version.tPrecO},
            {stamp:version.tStampMEBefore, precision:version.tPrecMEBefore}, {stamp:version.tStampMEAfter, precision:version.tPrecMEAfter},
            {stamp:version.tStampOBefore, precision:version.tPrecOBefore}, {stamp:version.tStampOAfter, precision:version.tPrecOAfter}
          ) ;
          timelinejson.events.push(feature);
        }
        var timeline = new TL.Timeline('timeline', timelinejson, options) ;
        timeline.on('change', function (event) {
          var uri = timeline.current_id;
          addGeometriesOfVersion(versions[uri], map, layersToRemove);
        });
      } );

  }); // AJAX END


};//FUNCTION END


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

function changeSelectedLandmark(event){
  initTimeline(graphDBRepositoryURI, dropDownMenu.value, factsNamedGraphURI, map=map, layersToRemove=layersToRemove);
}

function getLandmarks(graphDBRepositoryURI, namedGraphURI, dropDownMenu){
  var query = getQueryForLandmarks(namedGraphURI);

$.ajax({
  url: graphDBRepositoryURI,
  Accept: "application/sparql-results+json",
  contentType:"application/sparql-results+json",
  dataType:"json",
  data:{"query":query}
}).done((promise) => {
  displayLandmarksInDropDownMenu(dropDownMenu, promise.results.bindings)
})
}

function displayLandmarksInDropDownMenu(dropDownMenu, bindings){
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

function createOptionDiv(value, label){
  var option = document.createElement("option") ;
  option.setAttribute("value", value) ;
  option.innerHTML = label ;
  return option ;
}

function createOptionGroupDiv(value, label){
  var optionGroup = document.createElement("optgroup") ;
  optionGroup.setAttribute("value", value) ;
  optionGroup.setAttribute("label", label) ;
  return optionGroup ;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Changer dynamiquement la largeur de la carte et de la timeline

function allowMapTimelineResize(resizerClassName, map) {

  var resizer = document.querySelector('.' + resizerClassName);
  const div1 = resizer.previousElementSibling;
  const div2 = resizer.nextElementSibling;
  
  let isResizing = false;
  
  resizer.addEventListener('mousedown', (e) => {
    isResizing = true;
    document.body.style.cursor = 'ew-resize';
  });
  
  document.addEventListener('mousemove', (e) => {
    if (!isResizing) return;
  
    const containerOffsetLeft = div1.parentElement.offsetLeft;
    const newWidth = e.clientX - containerOffsetLeft;
  
    div1.style.width = `${newWidth}px`;
    div2.style.width = `calc(100% - ${newWidth}px - ${resizer.offsetWidth}px)`; // Ajuste la largeur en fonction du resizer
  });
  
  document.addEventListener('mouseup', () => {
    isResizing = false;
    document.body.style.cursor = '';
    map.invalidateSize(); // Recentrer la carte
  });
}

function setInnerHTMLToDivFromId(divId, content){
  var div = document.getElementById(divId);
  div.innerHTML = content;
}


function getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, layersToRemove){

  var queryValidAttrVersFromTime = getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
  var queryValidLandmarksFromTime = getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;

  var landmarks = {} ;
  var sureLandmarks = [] ;
  var unsureLandmarks = [] ;

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidLandmarksFromTime}
  }).done((promise) => {
    $.each(promise.results.bindings, function(i,bindings){
      var landmarkDescription = {lm : bindings.lm, existsForSure : getBooleanFromXSDBoolean(bindings.existsForSure)} ;
      landmarkDescription.properties = {} ;
      landmarkDescription.geometries = [] ;
      landmarks[bindings.lm.value] = landmarkDescription ;
    });

    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":queryValidAttrVersFromTime}
    }).done((promise) => {
      $.each(promise.results.bindings, function(i,bindings){
        var versValue = bindings.versValue.value;
        var lm = bindings.lm.value;
        var attrType = bindings.attrType.value;
        var attrTypeName = attrType.replace("http://rdf.geohistoricaldata.org/id/codes/address/attributeType/", "") ;
        if (landmarks[lm] && landmarks[lm].properties[attrTypeName]){
          landmarks[lm].properties[attrTypeName].push(versValue) ;
        }else if (landmarks[lm]){
          landmarks[lm].properties[attrTypeName] = [versValue] ;
        }

        if (attrTypeName == "Geometry" && landmarks[lm]){
            landmarks[lm].geometries.push(bindings.versValue) ;
            }
        
      });

      $.each(landmarks, function(key, value){
        if (value.existsForSure){
          var style = {color: "#32a852", fillColor: "#32a852"} ;
          var layerGroup = sureLandmarks ;
        }
        else {
          var style = {color: "#b30904", fillColor: "#b30904"} ;
          var layerGroup = unsureLandmarks ;
        };

        var lmName = value.properties["Name"][0] ;
        var lmGeometries = value.geometries ;
        lmGeometries.forEach(geom => {
          var geojsonGeom = getGeoJsonGeom(geom) ;
          var leafletGeom = L.geoJSON(geojsonGeom, {style:style});
          var popupContent = "<b>" + lmName + "</b>";
          leafletGeom.bindPopup(popupContent) ;
          layerGroup.push(leafletGeom) ;
        });

      });

      var sureLandmarksLG = L.layerGroup(sureLandmarks) ;
      var unsureLandmarksLG = L.layerGroup(unsureLandmarks) ;
      
      var overlayMaps = {
        "Certain": sureLandmarksLG,
        "Incertain": unsureLandmarksLG
    };

    var layerControl = L.control.layers().addTo(map);

    $.each(overlayMaps, function(key, value){
      layerControl.addOverlay(value, key);
      value.addTo(map);
    });
    
  });

  }) ;

}


function getBooleanFromXSDBoolean(xsdBoolean){
  if (xsdBoolean.datatype == "http://www.w3.org/2001/XMLSchema#boolean" && xsdBoolean.type == "literal"){ 
    if (xsdBoolean.value == "false") { return false ; }
    else if (xsdBoolean.value == "true") { return true ; }
  }
  return null ;  
}