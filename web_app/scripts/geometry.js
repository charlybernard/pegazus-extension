// Définir les systèmes de coordonnées
proj4.defs([
    [
      "EPSG:2154",
      "+proj=lcc +lat_1=49.000 +lat_2=44.000 +lat_0=46.500 +lon_0=3.000 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    ],
    [
      "EPSG:4326",
      "+proj=longlat +datum=WGS84 +no_defs"
    ],
    [
      "EPSG:3857",
      "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    ]
  ]);

// Gestion du côté leaflet

function initLeafletMap(id, lat, lon, zoom, tileLayersSettings=[], messages={}, maxZoom=18, minZoom=1){
  var mapSettings = {};

  // Initialisation de la carte
  mapSettings.map = L.map(id , {center: [lat, lon], zoom: zoom, maxZoom: maxZoom, minZoom: minZoom}); ;
  mapSettings.layerControl = L.control.layers()
  mapSettings.tileLayers = {};
  mapSettings.overlayLayers = {};
  mapSettings.selectedTileLayer = null;
  mapSettings.layersToRemove = [];
  mapSettings.selectedFeature = null;
  mapSettings.messages = messages;

  // Ajout des couches de tuiles
  initLeafletTileLayers(tileLayersSettings, mapSettings.map, mapSettings.layerControl, mapSettings.tileLayers);

  return mapSettings;
}

function initLeafletTileLayers(tileLayersSettings, map, layerControl, tileLayers){

  tileLayersSettings.forEach(setting => {
    var tileLayer = initLeafletTileLayer(setting);
    if (tileLayer){
      layerControl.addBaseLayer(tileLayer, setting.name);
      tileLayers[setting.name] = tileLayer;
    }
  });

  if (Object.keys(tileLayers).length == 0){
    var xyzUrl = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    var tileLayerToDisplay = initLeafletTileLayerForXyz(xyzUrl);
  } else if (Object.keys(tileLayers).length == 1){
    var tileLayerToDisplay = tileLayers[Object.keys(tileLayers)[0]];
  } else {
    var tileLayerToDisplay = tileLayers[Object.keys(tileLayers)[0]];
    layerControl.addTo(map);
  }
  tileLayerToDisplay.addTo(map);
  
}

function initLeafletTileLayer(tileLayerSettings){
  
  if (tileLayerSettings.type == "xyz") {
    var tileLayer = initLeafletTileLayerForXyz(tileLayerSettings.url);
  } else if (tileLayerSettings.type == "wms") {
    var tileLayer = initLeafletTileLayerForWms(tileLayerSettings.url, tileLayerSettings.layer);
  } else { 
    var tileLayer = null;
  }
  return tileLayer;
}

function initLeafletTileLayerForXyz(url){
  var tileLayer = L.tileLayer(url);
  return tileLayer;
}

function initLeafletTileLayerForWms(url, layer){
  var tileLayer = L.tileLayer.wms(url, {
    layers: layer});
    return tileLayer;
}

// Fonction de projection des coordonnées
function projectCoordinates(coords, sourceCRS, targetCRS) {
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
      // Projection d'un point
      return proj4(sourceCRS, targetCRS, coords);
  } else if (coords.constructor.name == "Array"){
    var newCoords = [] ;
    for (let i = 0 ; i < coords.length; i++){
      var newCoordsElem = projectCoordinates(coords[i], sourceCRS, targetCRS);
      newCoords.push(newCoordsElem);
    }
    return newCoords;
  } else {
    console.log("Problème dans les données") ;
    return null;
  }
}

// Fonction pour projeter un WKT
function projectWkt(wkt, sourceCRS, targetCRS) {
    var geoJson = Terraformer.WKT.parse(wkt);

    // Appliquer la projection
    geoJson.coordinates = projectCoordinates(geoJson.coordinates, sourceCRS, targetCRS);
    return geoJson; // Retourner le GeoJSON projeté
}

function wktToGeojsonGeom(wktStr){
  return Terraformer.WKT.parse(wktStr);
}

function getGeojsonObj(id, geomWkt, properties={}){

  var geojsonGeom = wktToGeojsonGeom(geomWkt);

  var geojsonObj = {
      "type": "Feature",
      "id":id,
      "properties":properties,
      "geometry": geojsonGeom
      }

  return geojsonObj;

}

function addGeometriesOfVersion(version, mapSettings, styleSettings){
  removeLayersFromList(mapSettings.layersToRemove, mapSettings.map) ;
  displayGeometriesOnMapFromList(version.values, mapSettings, styleSettings) ;
}

function removeLayersFromList(layersToRemove, map){
  // Suppression de toutes les géométries
  layersToRemove.forEach(layer => {
    map.removeLayer(layer);
  });
}

function displayGeometriesOnMapFromList(geomsToDisplay, mapSettings, styleSettings){
  var map = mapSettings.map ;
  var layersToRemove = mapSettings.layersToRemove ;
  var featuresList = [];
  geomsToDisplay.forEach(element => {
    var geojsonFeature = getGeoJsonForLandmark("", element, {}) ;
    // Add geojsonFeature to the list only if it has a geometry
    if (geojsonFeature.geometry != null){ featuresList.push(geojsonFeature); }
  });

  var layerGroup = getLandmarkLayerGroup(featuresList, mapSettings, styleSettings, hasPopup=false);
  layerGroup.getLayers().forEach(layer => {layersToRemove.push(layer)});
  layerGroup.addTo(map);
  fitBoundsToLayerGroups(map, [layerGroup]);
}


function getCrsFromWkt(geomWkt){
  var match = geomWkt.match(/EPSG\/0\/(\d+)/); // Cherche le numéro après "EPSG/0/"

  if (match) {
      var crsCode = match[1];
      return crsCode;
  } else {
      // console.log("CRS non trouvé");
      return null;
  }
}

function getGeoJsonGeom(element){
  var geoSparqlWktLiteral = "http://www.opengis.net/ont/geosparql#wktLiteral" ;
  if (element.datatype !=  geoSparqlWktLiteral){return null} ;
  var wktWithCrs = element.value ;
  var crsCode = getCrsFromWkt(wktWithCrs);
  var geomWkt = wktWithCrs.replace(/<.*?>\s*/, "");

  if (crsCode == null) { crsCode = "4326"; }

  var geoJsonGeom = projectWkt(geomWkt, 'EPSG:' + crsCode, 'EPSG:4326');
  return geoJsonGeom;
}

//////////////////////////////////////// Functions around layer group management ////////////////////////////////////////////////////

function createLayerGroup(map){
  var layerGroup = L.layerGroup().addTo(map);
  return layerGroup ;
}

function createOverlayLayerGroups(map, layerGroupNames){
  var overlayLayerGroups = {};
  layerGroupNames.forEach(layerGroupName => {
    overlayLayerGroups[layerGroupName] = createLayerGroup(map);
  });

  return overlayLayerGroups ;
}

function initOverlayLayerGroups(map, layerControl, layerGroupNames){
  var overlayLayerGroups = createOverlayLayerGroups(map, layerGroupNames);

    $.each(overlayLayerGroups, function(key, value){
      layerControl.addOverlay(value, key);
      value.addTo(map);
    });

  return overlayLayerGroups ;
}

function removeLayerGroups(layerGroups, map, layerControl){
  layerGroups.forEach(layerGroup => {
    layerControl.removeLayer(layerGroup);
    map.removeLayer(layerGroup);
  });
}


function getLayerGroupBounds(layerGroup){
  var bounds = L.latLngBounds();  // Créer un LatLngBounds vide pour accumuler les limites
  var layers = layerGroup.getLayers()

  layers.forEach(layer => {
    if (layer.getBounds) {
      var lBounds = layer.getBounds();
      bounds.extend(lBounds);
    } else if (layer.getLatLng) {
      var lBounds = layer.getLatLng();
      bounds.extend(lBounds);
    }
  });

  return bounds;
}

function getLayerGroupsBounds(layerGroups){
  var bounds = L.latLngBounds();  // Créer un LatLngBounds vide pour accumuler les limites
  layerGroups.forEach(layerGroup => {
    bounds.extend(getLayerGroupBounds(layerGroup));
  })

  return bounds;
}

function fitBoundsToLayerGroups(map, layerGroups) {
  var bounds = getLayerGroupsBounds(layerGroups);
  if (bounds.isValid()){
    map.fitBounds(bounds);
    return true;
  } else {
    return false;
  }

}

function removeOverlayLayers(overlayLayers, map, layerControl){
  var overlayLayersList = Object.values(overlayLayers);
  removeLayerGroups(overlayLayersList, map, layerControl) ;
}

//////////////////////////////////////// Functions around popup management ////////////////////////////////////////////////////

function initInfoControl(mapSettings){
  var infoControl = L.control({ position: 'topleft' });
  infoControl.onAdd = function(map) {
    this._div = L.DomUtil.create('div', 'info-control');
    // this.update(); // Initialise avec un contenu
    this._div.innerHTML = mapSettings.messages.flyOverLandmark;

    // Styles appliqués directement en JavaScript
    Object.assign(this._div.style, {
      backgroundColor: 'rgba(255, 255, 255, 0.9)',
      color: '#000',
      padding: '10px 15px',
      borderRadius: '8px',
      boxShadow: '0 2px 6px rgba(0, 0, 0, 0.5)',
      maxWidth: '300px',
  });
    return this._div;
  };

  infoControl.addTo(mapSettings.map);
  mapSettings.infoControl = infoControl;
}

function reinitInfoControlContent(infoControl, content){
  var italicsContent = `${getHTMLItalicsText(content)}`;
  infoControl._div.innerHTML = italicsContent;
}

function updateInfoControlContent(infoControl, layer, nameTitle){
  var content = `${getHTMLBoldText(nameTitle +  " :")} `;
  if (layer.feature.name){
    content += layer.feature.name;
  } else {
    content += "...";
  }
  infoControl._div.innerHTML = content;
}


///////////////////////////////////////////////////////////////

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

function getLandmarkLayerGroup(featuresList, mapSettings, styleSettings, selectedStyleSettings=null, hasPopup=true){
  var featureCollection = initGeoJsonFeatureCollection(featuresList);
  var leafletGeom = L.geoJSON(featureCollection) ;
  var layers = [];
  leafletGeom.eachLayer(function (layer) { setLayer(layer, layers, mapSettings, styleSettings, selectedStyleSettings, hasPopup) ; });

  var layerGroup = L.layerGroup(layers);
  return layerGroup ;
}

function setLayer(layer, layers, mapSettings, styleSettings, selectedStyleSettings=null, hasPopup=false){
  // Style initialisation
  var multiStylesSettings = {default:styleSettings} ;

  // Style settings for selected and hovered features (if they exist)
  if (selectedStyleSettings){
    multiStylesSettings.selected = selectedStyleSettings;
    multiStylesSettings.hovered = selectedStyleSettings;
  }
  initLayerStyles(layer, multiStylesSettings);
  setLayerStyle(layer);
  
  // if (hasPopup){setPopup(layer);}
  if (selectedStyleSettings){
    // actionsOnLayerSelection(layer, mapSettings);
    actionsOnLayerHovered(layer, mapSettings);
  }
  layers.push(layer);
}

function initLayerStyles(layer, multiStylesSettings){
  // multiStylesSettings = {name1:styleSettings1, name2:styleSettings2}
  layer.styles = multiStylesSettings;
}

function setLayerStyle(layer, styleKey="default"){
  var styleSettings = layer.styles[styleKey];
  if (layer instanceof L.Marker) {
    layer.setIcon(styleSettings.marker);
  } else if (layer instanceof L.Polyline) {
    if (layer instanceof L.Polygon) {
      layer.setStyle(styleSettings.polygon);
    } else {
      layer.setStyle(styleSettings.polyline);
    }
  }
}

function actionsOnLayerSelection(layer, mapSettings){
  layer.on('click', function(e){
    // Change the style of the selected feature before having clicked on this one
    if (mapSettings.selectedFeature){ setLayerStyle(mapSettings.selectedFeature, "default") ; }

    // Update the selected feature and change its style
    mapSettings.selectedFeature = layer;
    setLayerStyle(layer, "selected");
  });
}

function actionsOnLayerHovered(layer, mapSettings){
  
  layer.on('mouseover', function(e){
    updateInfoControlContent(mapSettings.infoControl, layer, mapSettings.messages.nameTitle);
    if (mapSettings.selectedFeature != layer){
      setLayerStyle(layer, "hovered"); 
    }
  });

  layer.on('mouseout', function(e){
    reinitInfoControlContent(mapSettings.infoControl, mapSettings.messages.flyOverLandmark);
    if (mapSettings.selectedFeature != layer){
      setLayerStyle(layer, "default"); 
    }
  });
}

function setPopup(layer){
  var featureName = layer.feature.name;
  var popupContent = getHTMLBoldText(featureName);
  layer.bindPopup(popupContent) ;
}

function displayLandmarkLayerGroup(landmarkLayerName, featuresList, mapSettings, styleSettings){
  var map = mapSettings.map ;
  var layerControl = mapSettings.layerControl;
  var defaultStyleSettings = styleSettings.default;
  var selectedStyleSettings = styleSettings.selected;
  var landmarkLayerGroup = getLandmarkLayerGroup(featuresList, mapSettings, defaultStyleSettings, selectedStyleSettings, hasPopup=true);
  landmarkLayerGroup.addTo(map);
  layerControl.addOverlay(landmarkLayerGroup, landmarkLayerName);
  return landmarkLayerGroup;
}