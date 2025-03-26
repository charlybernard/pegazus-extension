function getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, timeDelay, namedGraphURI, mapSettings){
  var [lowTimeStamp, highTimeStamp] = getLowAndHighTimeStampFromDurationDelay(timeStamp, timeDelay) ;
  var queryValidLandmarksFromTime = getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI, lowTimeStamp, highTimeStamp) ;

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidLandmarksFromTime}
  }).done((promise) => {
    var landmarksDesc = getInitLandmarksDescriptions(promise.results.bindings);
    displayLandmarksFromGivenTime(timeStamp, timeCalendarURI, namedGraphURI, landmarksDesc, mapSettings);
  }) ;  
}

function getLowAndHighTimeStampFromDurationDelay(timeStamp, timeDelay){
  var lowTimeStamp = null ;
  var highTimeStamp = null ;
  if (timeDelay){
    var lowTime = new Date(timeStamp) ;
    var highTime = new Date(timeStamp) ;
    lowTime.setFullYear(lowTime.getFullYear() - timeDelay) ;
    highTime.setFullYear(highTime.getFullYear() + timeDelay) ;
    lowTimeStamp = lowTime.toISOString() ;
    highTimeStamp = highTime.toISOString() ;
  }

  return [lowTimeStamp, highTimeStamp] ;
}

function displayLandmarksFromGivenTime(timeStamp, timeCalendarURI, namedGraphURI, landmarksDescriptions, mapSettings){
  var queryValidAttrVersFromTime = getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
  
  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidAttrVersFromTime}
  }).done((promise) => {
    displayLandmarksFromDescriptions(promise.results.bindings, landmarksDescriptions, mapSettings);
    updateMapViewForSnapshotSelection(landmarksDescriptions, mapSettings, mapSettings.messages.noLandmarkToDisplay);
});
}

function updateMapViewForSnapshotSelection(landmarksDescriptions, mapSettings, alertMessage){

  // If no landmarks have been found at this date, display an alert
  if (Object.keys(landmarksDescriptions).length === 0){
    alert(alertMessage);
  }
  // Fit bounds to layer groups if enableFitBounds is not false (fit bounds only once)
  if (mapSettings.enableFitBounds != false){
    var hasFitBounds = fitBoundsToLayerGroups(mapSettings.map, Object.values(mapSettings.overlayLayers));
    if (hasFitBounds){   
      mapSettings.enableFitBounds = false;
    }
  }
}
function displayLandmarksFromDescriptions(bindings, landmarksDescriptions, mapSettings){
  var overlayLayers = mapSettings.overlayLayers ;
  var landmarkLayers = {sure:[], unsure:[]};
  var landmarkLayersStyles = {
    sure: {
      default: {marker:lo.greenDot, polyline:lo.greenDefaultLineStringStyle, polygon:lo.greenDefaultPolygonStyle},
      selected: {marker:lo.greenMarker, polyline:lo.greenSelectedLineStringStyle, polygon:lo.greenSelectedPolygonStyle}
    },
    unsure: {
      default: {marker:lo.redDot, polyline:lo.redDefaultLineStringStyle, polygon:lo.redDefaultPolygonStyle},
      selected: {marker:lo.redMarker, polyline:lo.redSelectedLineStringStyle, polygon:lo.redSelectedPolygonStyle}
    }
  } ;

  bindings.forEach(binding => {
    updateLandmarksDescriptionsWithAttributeVersions(landmarksDescriptions, binding);
  });

  for (var key in landmarksDescriptions){
    var landmark = landmarksDescriptions[key] ;
    initGeoJsonForLandmark(landmark, landmarkLayers) ;
  }

  for (var key in landmarkLayers){
    var landmarkFeatures = landmarkLayers[key] ;
    var style = landmarkLayersStyles[key];
    var landmarkLayerGroup = displayLandmarkLayerGroup(key, landmarkFeatures, mapSettings, style);
    overlayLayers[key] = landmarkLayerGroup;
  }
}

function getInitLandmarksDescriptions(bindings){
  var landmarksDesc = {};
  bindings.forEach(binding => {
    var lm = binding.lm ;
    var lmLabel = binding.lmLabel ;
    var existsForSure = binding.existsForSure ;
    var landmarkDesc = getInitLandmarkDescription(lm, lmLabel, existsForSure);
    landmarksDesc[lm.value] = landmarkDesc;
  })
  return landmarksDesc;
}
 
function getInitLandmarkDescription(lm, lmLabel, existsForSure){
  var boolExistsForSure = getBooleanFromXSDBoolean(existsForSure) ;
  var lmName = lmLabel.value;
  var landmarkDesc = getLandmarkDescription(lm, lmName);
  landmarkDesc.existsForSure = boolExistsForSure ;
  return landmarkDesc
}

function getLandmarkDescription(lm, lmName){
  var landmarkDescription = {lm : lm} ;
  if (lmName){ landmarkDescription.name = lmName ; }
  landmarkDescription.properties = {} ;
  landmarkDescription.geometries = [] ;
  return landmarkDescription ;
}

function updateLandmarksDescriptionsWithAttributeVersions(landmarks, binding){
  // landmarks = {lm1: {lm:lm1, name:lmName1, properties:{attr1:[vers1, vers2], attr2:[vers3, vers4]}, geometries:[geom1, geom2]}, lm2: {...}} 
  var attrTypeNamespace = "http://rdf.geohistoricaldata.org/id/codes/address/attributeType/" ;
  var versValue = binding.versValue.value;
  var lm = binding.lm.value;
  var attrType = binding.attrType.value;
  var attrTypeName = attrType.replace(attrTypeNamespace, "") ;

  if (landmarks[lm] && landmarks[lm].properties[attrTypeName]){
      landmarks[lm].properties[attrTypeName].push(versValue) ;
  }else if (landmarks[lm]){
      landmarks[lm].properties[attrTypeName] = [versValue] ;
  }

  if (attrTypeName == "Geometry" && landmarks[lm]){
      landmarks[lm].geometries.push(binding.versValue) ;
      }
}

function initGeoJsonForLandmark(landmark, landmarkLayers){
  //  landmarkLayers = {sure: [...], unsure: [...]}
  if (landmark.existsForSure){
    var layer = landmarkLayers.sure;
  } else {
    var layer = landmarkLayers.unsure ;
  }

  var lmName = "" ;
  if (landmark.name){
    var lmName = landmark.name;
  }

  var lmGeometries = landmark.geometries ;
  var lmProperties = landmark.properties ;
  
  lmGeometries.forEach(lmGeom => {
    var geoJsonForLandmark = getGeoJsonForLandmark(lmName, lmGeom, lmProperties);
    layer.push(geoJsonForLandmark);
  });
}

function displaySnapshotFromSelectedTime(graphDBRepositoryURI, timeStampDivId, timeCalendarURI, timeDelay, namedGraphURI, mapSettings){
    var timeStamp = document.getElementById(timeStampDivId).value;
    removeOverlayLayers(mapSettings.overlayLayers, mapSettings.map, mapSettings.layerControl);
    getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, timeDelay, namedGraphURI, mapSettings) ;
}