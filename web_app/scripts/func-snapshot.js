function getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers){

    var queryValidAttrVersFromTime = getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
    var queryValidLandmarksFromTime = getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
    var landmarks = {}
    var landmarkLayers = {sure:[], unsure:[]};
    var landmarkLayersStyles = {
      sure: {marker:lo.greenDot, polyline:lo.greenDefaultLineStringStyle, polygon:lo.greenDefaultPolygonStyle},
      unsure: {marker:lo.redDot, polyline:lo.redDefaultLineStringStyle, polygon:lo.redDefaultPolygonStyle}
    } ;

    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":queryValidLandmarksFromTime}
    }).done((promise) => {
      promise.results.bindings.forEach(binding => {
        initLandmarkDescription(binding.lm, binding.existsForSure, landmarks);
      });
  
      $.ajax({
        url: graphDBRepositoryURI,
        Accept: "application/sparql-results+json",
        contentType:"application/sparql-results+json",
        dataType:"json",
        data:{"query":queryValidAttrVersFromTime}
      }).done((promise) => {
        
        promise.results.bindings.forEach(binding => {
          addAtributeVersionToLandmarks(landmarks, binding);
        });
  
        for (var key in landmarks){
          var landmark = landmarks[key] ;
          initGeoJsonForLandmark(landmark, landmarkLayers) ;
        }

        for (var key in landmarkLayers){
          var landmarkFeatures = landmarkLayers[key] ;
          var style = landmarkLayersStyles[key];
          var landmarkLayerGroup = displayLandmarkLayerGroup(key, landmarkFeatures, style, map, layerControl);
          overlayLayers[key] = landmarkLayerGroup;
        }
        
        fitBoundsToLayerGroups(map, Object.values(overlayLayers));
    });
  
    }) ;
  
  }
 
function initLandmarkDescription(lm, existsForSure, landmarks){
  var boolExistsForSure = getBooleanFromXSDBoolean(existsForSure) ;
  var landmarkDescription = getLandmarkDescription(lm, boolExistsForSure);
  var landmarkUri = lm.value ;

  landmarkDescription.existsForSure = boolExistsForSure ;
  landmarks[landmarkUri] = landmarkDescription ;
}


function getLandmarkDescription(lm){
  var landmarkDescription = {lm : lm} ;
  landmarkDescription.properties = {} ;
  landmarkDescription.geometries = [] ;
  return landmarkDescription ;
}

function addAtributeVersionToLandmarks(landmarks, binding){
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
  
  if (landmark.properties["Name"] && Array.isArray(landmark.properties["Name"])){
    var lmName = landmark.properties["Name"][0] ;
  } else {
    var lmName = null ;
  }

  var lmGeometries = landmark.geometries ;
  var lmProperties = landmark.properties ;
  
  lmGeometries.forEach(lmGeom => {
    var geoJsonForLandmark = getGeoJsonForLandmark(lmName, lmGeom, lmProperties);
    layer.push(geoJsonForLandmark);
  });
}

function getGeoJsonForLandmark(name, geom, properties){
  var geoJsonForLandmark = {type:"Feature", name:name} ;
  var geojsonGeom = getGeoJsonGeom(geom) ;
  geoJsonForLandmark.geometry = geojsonGeom ;
  geoJsonForLandmark.properties = properties ;
  return geoJsonForLandmark ;
}

function initGeoJsonFeatureCollection(featuresList){
  var ftCol = {type:"FeatureCollection", features:featuresList};
  return ftCol;
}

function getLandmarkLayerGroup(featuresList, styleSettings){
  var featureCollection = initGeoJsonFeatureCollection(featuresList);
  var leafletGeom = L.geoJSON(featureCollection) ;
  var layers = [];

  leafletGeom.eachLayer(function (layer) {
    setLayerStyle(layer, styleSettings);
    setPopup(layer);
    layers.push(layer);
  });

  var layerGroup = L.layerGroup(layers);
  return layerGroup ;
}

function displayLandmarkLayerGroup(landmarkLayerName, featuresList, styleSettings, map, layerControl){
  var landmarkLayerGroup = getLandmarkLayerGroup(featuresList, styleSettings);
  landmarkLayerGroup.addTo(map);
  layerControl.addOverlay(landmarkLayerGroup, landmarkLayerName);
  return landmarkLayerGroup;
}

function setLayerStyle(layer, styleSettings){
  // styleSettings = {marker: iconSettings, polyline: polylineStyleSettings, polygon:polygonStyleSettings}
  if (layer instanceof L.Marker) {
      layer.setIcon(styleSettings.marker);
  } else if (layer instanceof L.Polyline) {
      layer.setStyle(styleSettings.polyline);
  } else if (layer instanceof L.Polygon) {
      layer.setStyle(styleSettings.polygon);
  }
}

function setPopup(layer){
  var featureName = layer.feature.name;
  var popupContent = boldText(featureName);
  layer.bindPopup(popupContent) ;
}

function displaySnapshotFromSelectedTime(graphDBRepositoryURI, timeStampDivId, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers){
    var timeStamp = document.getElementById(timeStampDivId).value;
    clearOverlayLayers(overlayLayers);
    getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, layerControl, overlayLayers) ;
}

function clearOverlayLayers(overlayLayers){
  var overlayLayersList = Object.values(overlayLayers);
  clearLayerGroups(overlayLayersList) ;
}