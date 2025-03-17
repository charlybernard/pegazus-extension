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

function initLeaflet(lat, lon, zoom) {
    // Initialisation de la carte
    var map = L.map('map').setView([lat, lon], zoom);

    // Ajout de la couche de tuiles OpenStreetMap
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 20
    }).addTo(map);
    return map;
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

  geojsonObj = {
      "type": "Feature",
      "id":id,
      "properties":properties,
      "geometry": geojsonGeom
      }

  return geojsonObj;

}

function addGeometriesOfVersion(version, map, layersToRemove){

  // Suppression de toutes les géométries
  layersToRemove.forEach(layer => {
    map.removeLayer(layer);
  });


  version.values.forEach(element => {
    displayGeometryOnMap(element, map, layersToRemove);
  });
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

function displayGeometryOnMap(element, map, layersToRemove=null, style=null, fitBounds=true){
  var geoJsonGeom = getGeoJsonGeom(element);
  if (geoJsonGeom != null) { 
    var layer = L.geoJSON(geoJsonGeom, {style:style}).addTo(map);
    if (fitBounds){map.fitBounds(layer.getBounds());}
    if (layersToRemove != null) { layersToRemove.push(layer); }
   }
}

function getGeoJsonGeom(element){
  if (element.datatype !=  "http://www.opengis.net/ont/geosparql#wktLiteral"){return null} ;
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

function initOverlayLayerGroups(map, layerGroupNames){
  var overlayLayerGroups = createOverlayLayerGroups(map, layerGroupNames);

  var layerControl = L.control.layers().addTo(map);

    $.each(overlayLayerGroups, function(key, value){
      layerControl.addOverlay(value, key);
      value.addTo(map);
    });

  return overlayLayerGroups ;
}

function clearOverlayLayerGroups(overlayLayerGroups){
  for (var key in overlayLayerGroups) {
    if (overlayLayerGroups.hasOwnProperty(key)) {
      overlayLayerGroups[key].clearLayers();
    }
  }
}

function fitBoundsToOverlayLayerGroups(overlayLayerGroups) {
  var bounds = L.latLngBounds();  // Créer un LatLngBounds vide pour accumuler les limites

  // Itérer sur chaque couche dans overlayMaps
  for (var key in overlayLayerGroups) {
      if (overlayLayerGroups.hasOwnProperty(key)) {
          // Récupérer la couche
          var layerGroup = overlayLayerGroups[key];
          var layers = layerGroup.getLayers() ;
          layers.forEach(layer => {
            if (layer.getBounds) {
              var lBounds = layer.getBounds();
              bounds.extend(lBounds);
            } else if (layer.getLatLng) {
              var lBounds = layer.getBounds();
              bounds.extend(lBounds);
            }
          });
      }
  }
  // Appliquer fitBounds sur la carte avec les limites combinées
  map.fitBounds(bounds);
}