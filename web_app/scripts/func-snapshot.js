function getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName){

    var queryValidAttrVersFromTime = getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
    var queryValidLandmarksFromTime = getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI) ;
    var landmarks = {} ;
  
    $.ajax({
      url: graphDBRepositoryURI,
      Accept: "application/sparql-results+json",
      contentType:"application/sparql-results+json",
      dataType:"json",
      data:{"query":queryValidLandmarksFromTime}
    }).done((promise) => {
      $.each(promise.results.bindings, function(i,bindings){
        var landmarkDescription = initLandmarkDescription(bindings.lm, bindings.existsForSure) ;
        landmarks[bindings.lm.value] = landmarkDescription ;
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
          addLandmarkToMap(landmark, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName) ;
        }
  
        fitBoundsToOverlayLayerGroups(overlayLayerGroups) ;
    });
  
    }) ;
  
  }
  
function initLandmarkDescription(lm, existsForSure){
    var boolExistsForSure = getBooleanFromXSDBoolean(existsForSure) ;
    var landmarkDescription = {lm : lm, existsForSure : boolExistsForSure} ;
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

    function addLandmarkToMap(landmark, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName){
    if (landmark.existsForSure){
        var style = {color: "#32a852", fillColor: "#32a852"} ;
        var layerGroup = overlayLayerGroups[certainLayerGroupName] ;
    }
    else {
        var style = {color: "#b30904", fillColor: "#b30904"} ;
        var layerGroup = overlayLayerGroups[uncertainLayerGroupName] ;
    };

    var lmName = landmark.properties["Name"][0] ;
    var lmGeometries = landmark.geometries ;
    lmGeometries.forEach(geom => {
        var geojsonGeom = getGeoJsonGeom(geom) ;
        var leafletGeom = L.geoJSON(geojsonGeom, {style:style});
        var popupContent = "<b>" + lmName + "</b>";
        leafletGeom.bindPopup(popupContent) ;
        layerGroup.addLayer(leafletGeom) ;
    });
}


function displaySnapshotFromSelectedTime(graphDBRepositoryURI, timeStampDivId, timeCalendarURI, namedGraphURI, map, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName){
    var timeStamp = document.getElementById(timeStampDivId).value;
    clearOverlayLayerGroups(overlayLayerGroups) ;
    getSnapshotFromTimeStamp(graphDBRepositoryURI, timeStamp, timeCalendarURI, namedGraphURI, map, overlayLayerGroups, certainLayerGroupName, uncertainLayerGroupName) ;
}