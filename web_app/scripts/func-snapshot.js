function getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers){
    var queryValidLandmarksFromTime = getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;

    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":queryValidLandmarksFromTime}
    }).done((promise) => {
      var landmarksDesc = getInitLandmarksDescriptions(promise.results.bindings);
      displayLandmarksFromGivenTime(timeStamp, timeCalendarURI, namedGraphURI, landmarksDesc, map, layerControl, overlayLayers);
    }) ;  
  }

function displayLandmarksFromGivenTime(timeStamp, timeCalendarURI, namedGraphURI, landmarksDescriptions, map, layerControl, overlayLayers){
  var queryValidAttrVersFromTime = getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;

  $.ajax({
    url: graphDBRepositoryURI,
    Accept: "application/sparql-results+json",
    contentType:"application/sparql-results+json",
    dataType:"json",
    data:{"query":queryValidAttrVersFromTime}
  }).done((promise) => {
    displayLandmarksFromDescriptions(promise.results.bindings, landmarksDescriptions, map, layerControl, overlayLayers);
    fitBoundsToLayerGroups(map, Object.values(overlayLayers));
});
}

function displayLandmarksFromDescriptions(bindings, landmarksDescriptions, map, layerControl, overlayLayers){
  var landmarkLayers = {sure:[], unsure:[]};
  var landmarkLayersStyles = {
    sure: {marker:lo.greenDot, polyline:lo.greenDefaultLineStringStyle, polygon:lo.greenDefaultPolygonStyle},
    unsure: {marker:lo.redDot, polyline:lo.redDefaultLineStringStyle, polygon:lo.redDefaultPolygonStyle}
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
    var landmarkLayerGroup = displayLandmarkLayerGroup(key, landmarkFeatures, style, map, layerControl);
    overlayLayers[key] = landmarkLayerGroup;
  }
}

function getInitLandmarksDescriptions(bindings){
  var landmarksDesc = {};
  bindings.forEach(binding => {
    console.log(binding);
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
    var versValue = binding.versValue.value;
    var lm = binding.lm.value;
    var attrType = binding.attrType.value;
    var attrTypeName = attrType.replace("http://rdf.geohistoricaldata.org/id/codes/address/attributeType/", "") ;

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

function displaySnapshotFromSelectedTime(graphDBRepositoryURI, timeStampDivId, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers){
    var timeStamp = document.getElementById(timeStampDivId).value;
    removeOverlayLayers(overlayLayers, map, layerControl);
    getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers) ;
}