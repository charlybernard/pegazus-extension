document.getElementsByClassName("map_timeline")[0].style.height = window.screen.height;

var graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
var factsNamedGraphURI = getNamedGraphURI(graphDBURI, graphName, namedGraphName) ;
var landmarkValidTimeDivId = "landmark_valid_time" ;
var landmarkNamesDivId = "landmark_names" ;


////////////////////////////////////////////////////////////////////////////////////

function getGraphDBRepositoryURI(graphDBURI, graphName){
  return graphDBURI + "/repositories/" + graphName ;
}

function getNamedGraphURI(graphDBURI, graphName, namedGraphName){
  return graphDBURI + "/repositories/" + graphName + "/" + namedGraphName ;
}

function getQueryValidTimeForLandmark(landmarkURI, namedGraphURI){
  var query = `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ctype: <http://rdf.geohistoricaldata.org/id/codes/address/changeType/>
SELECT DISTINCT ?lm 
?tStampApp ?tPrecApp ?tStampDis ?tPrecDis
?tStampAppBefore ?tPrecAppBefore ?tStampAppAfter ?tPrecAppAfter
?tStampDisBefore ?tPrecDisBefore ?tStampDisAfter ?tPrecDisAfter
WHERE {
    BIND(<` + namedGraphURI + `> AS ?g)
    BIND (<` + landmarkURI + `> AS ?lm)

    ?changeApp a addr:Change ; addr:isChangeType ctype:LandmarkAppearance ; addr:appliedTo ?lm ; addr:dependsOn ?evApp .
    ?changeDis a addr:Change ; addr:isChangeType ctype:LandmarkDisappearance ; addr:appliedTo ?lm ; addr:dependsOn ?evDis .

    OPTIONAL { ?evApp addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }
    OPTIONAL { ?evApp addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }
    OPTIONAL { ?evApp addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }
    OPTIONAL { ?evDis addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }
    OPTIONAL { ?evDis addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }
    OPTIONAL { ?evDis addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }
}
LIMIT 1
`
return query ;
}

function getQueryToInitTimeline(landmarkURI, namedGraphURI){
  // Requête qui marche bien, ne pas y toucher !!!!
  var query = `PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?lm ?attrType ?attrVers ?cgME ?cgO
?tStampME ?tPrecME ?tStampO ?tPrecO
?tStampMEBefore ?tPrecMEBefore ?tStampMEAfter ?tPrecMEAfter ?tStampOBefore ?tPrecOBefore ?tStampOAfter ?tPrecOAfter
WHERE {
    BIND(<` + namedGraphURI + `> AS ?g)
    BIND (<` + landmarkURI + `> AS ?lm)
    ?lm a addr:Landmark ; addr:hasAttribute [addr:isAttributeType ?attrType ; addr:hasAttributeVersion ?attrVers] .
    ?cgME addr:makesEffective ?attrVers ; addr:dependsOn ?evME.
    ?cgO addr:outdates ?attrVers ; addr:dependsOn ?evO.
    OPTIONAL { ?evME addr:hasTime [addr:timeStamp ?tStampME ; addr:timePrecision ?tPrecME] }
    OPTIONAL { ?evO addr:hasTime [addr:timeStamp ?tStampO ; addr:timePrecision ?tPrecO] }
    OPTIONAL { ?evME addr:hasTimeBefore [addr:timeStamp ?tStampMEBefore ; addr:timePrecision ?tPrecMEBefore] }
    OPTIONAL { ?evME addr:hasTimeAfter [addr:timeStamp ?tStampMEAfter ; addr:timePrecision ?tPrecMEAfter] }
    OPTIONAL { ?evO addr:hasTimeBefore [addr:timeStamp ?tStampOBefore ; addr:timePrecision ?tPrecOBefore] }
    OPTIONAL { ?evO addr:hasTimeAfter [addr:timeStamp ?tStampOAfter ; addr:timePrecision ?tPrecOAfter] }
}
ORDER BY ?tStampME
  `

  return query ;
}

function initTimeline(graphDBRepositoryURI, landmarkURI, namedGraphURI, map){

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
    console.log(queryValidTimeForLandmark);
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
      landmarkValidTimeDiv.innerHTML = validTimeForLandmarkLabel
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
    var query = `PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    SELECT DISTINCT ?vers ?val WHERE {` + valuesForQuery +
    `?vers addr:versionValue ?val }`

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
          addGeometriesOfVersion(versions[uri], map);
        });
      } );

  }); // AJAX END


};//FUNCTION END


function getValidTimeForLandmark(timeApp={}, timeDis={}, timeBeforeApp={}, timeAfterApp={}, timeBeforeDis={}, timeAfterDis={}){
  var startTime = {} ;
  var startTimePrec = undefined ;
  var startTimeBefore = undefined ;
  var startTimeAfter = undefined ;
  if(timeApp.stamp && timeApp.precision){
    var startTimePrec = getTimeWithFrenchabel(timeApp.stamp.value, timeApp.precision.value) ;
    startTime.precise = startTimePrec ;
  }else if(timeBeforeApp.stamp && timeBeforeApp.precision && timeAfterApp.stamp && timeAfterApp.precision){
    var startTimeBefore = getTimeWithFrenchabel(timeBeforeApp.stamp.value, timeBeforeApp.precision.value) ;
    var startTimeAfter = getTimeWithFrenchabel(timeAfterApp.stamp.value, timeAfterApp.precision.value) ;
    startTime.before = startTimeBefore ;
    startTime.after = startTimeAfter ;
  }else if (timeBeforeApp.stamp && timeBeforeApp.precision){
    var startTimeBefore = getTimeWithFrenchabel(timeBeforeApp.stamp.value, timeBeforeApp.precision.value) ;
    startTime.before = startTimeBefore ;
  }else if (timeAfterApp.stamp && timeAfterApp.precision){
    var startTimeAfter = getTimeWithFrenchabel(timeAfterApp.stamp.value, timeAfterApp.precision.value) ;
    startTime.after = startTimeAfter ;
  }

  var endTime = {} ;
  if(timeDis.stamp && timeDis.precision){
    var endTimePrec = getTimeWithFrenchabel(timeDis.stamp.value, timeDis.precision.value) ;
    endTime.precise = endTimePrec ;
  }else if(timeBeforeDis.stamp && timeBeforeDis.precision && timeAfterDis.stamp && timeAfterDis.precision){
    var endTimeBefore = getTimeWithFrenchabel(timeBeforeDis.stamp.value, timeBeforeDis.precision.value) ;
    var endTimeAfter = getTimeWithFrenchabel(timeAfterDis.stamp.value, timeAfterDis.precision.value) ;
    endTime.before = endTimeBefore ;
    endTime.after = endTimeAfter ;
  }else if (timeBeforeDis.stamp && timeBeforeDis.precision){
    var endTimeBefore = getTimeWithFrenchabel(timeBeforeDis.stamp.value, timeBeforeDis.precision.value) ;
    endTime.before = endTimeBefore ;
  }else if (timeAfterDis.stamp && timeAfterDis.precision){
    var endTimeAfter = getTimeWithFrenchabel(timeAfterDis.stamp.value, timeAfterDis.precision.value) ;
    endTime.after = endTimeAfter ;
  }

  return {"appTime":startTime, "disTime":endTime}
}


function createTimelineFeature(attrVersion, attrType, attrVersionValues, timeME={}, timeO={}, timeBeforeME={}, timeAfterME={}, timeBeforeO={}, timeAfterO={}){
  var groupName = attrType.value.replace("http://rdf.geohistoricaldata.org/id/codes/address/attributeType/", "") ;
  var text = createTimelineText(attrVersion, attrVersionValues);

  var feature = {
    "group":groupName,
    "background":{"color":"#1c244b"},
    "unique_id":attrVersion.value
    }

  var startTime = undefined ;
  if(timeME.stamp && timeME.precision){
    var startTime = createTime(timeME.stamp.value, timeME.precision.value) ;
  }else if(timeBeforeME.stamp && timeBeforeME.precision && timeAfterME.stamp && timeAfterME.precision){
    var startTime = createTimeFromTwoTimes(timeBeforeME.stamp.value, timeBeforeME.precision.value, timeAfterME.stamp.value, timeAfterME.precision.value)
  }else if (timeBeforeME.stamp && timeBeforeME.precision){
    var startTime = createTime(timeBeforeME.stamp.value, timeBeforeME.precision.value) ;
  }else if (timeAfterME.stamp && timeAfterME.precision){
    var startTime = createTime(timeAfterME.stamp.value, timeAfterME.precision.value) ;
  }

  var endTime = undefined ;
  if(timeO.stamp && timeO.precision){
    var endTime = createTime(timeO.stamp.value, timeO.precision.value) ;
  }else if(timeBeforeO.stamp && timeBeforeO.precision && timeAfterO.stamp && timeAfterO.precision){
    var endTime = createTimeFromTwoTimes(timeBeforeO.stamp.value, timeBeforeO.precision.value, timeAfterO.stamp.value, timeAfterO.precision.value)
  }else if (timeBeforeO.stamp && timeBeforeO.precision){
    var endTime = createTime(timeBeforeO.stamp.value, timeBeforeO.precision.value) ;
  }else if (timeAfterO.stamp && timeAfterO.precision){
    var endTime = createTime(timeAfterO.stamp.value, timeAfterO.precision.value) ;
  }

  feature["start_date"] = startTime ;
  feature["end_date"] = endTime ;

  feature["text"]  = text ;

  return feature ;
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


function getValuesForQuery(variable, values){
  var strValues = "VALUES ?" + variable + "{"
  for (uri in values){
    strValues += "<" + uri + "> " ;
  }
  strValues += "}" ;
  return strValues ;
}

/// Gestion des temps dans timeline.js


function getValidTimeForLandmarkLabel(appTime, disTime){
  var label = "";
  if (appTime.precise){
    label += "<div><b>Date de création :</b> " + appTime.precise.label + "</div>" ;
  } else if (appTime.before && appTime.after){
    label += "<div><b>Date de création :</b> entre " + appTime.before.label + " et " + appTime.after.label + "</div>" ;
  } else if (appTime.before){
    label += "<div><b>Date de création :</b> avant " + appTime.before.label + "</div>" ;
  } else if (appTime.after){
    label += "<div><b>Date de création :</b> après " + appTime.after.label + "</div>" ;
  }

  label += "<br>" ;

  if (disTime.precise){
    label += "<div><b>Date de disparition :</b> " + disTime.precise.label + "</div>" ;
  } else if (disTime.before && disTime.after){
    label += "<div><b>Date de disparition :</b> entre " + disTime.before.label + " et " + disTime.after.label + "</div>" ;
  } else if (disTime.before){
    label += "<div><b>Date de disparition :</b> avant " + disTime.before.label + "</div>" ;
  } else if (disTime.after){
    label += "<div><b>Date de disparition :</b> après " + disTime.after.label + "</div>" ;
  }

  return label

}

function getTimeWithFrenchabel(timeStamp, timePrecision){
  var timeElems = extractElementsFromTimeStamp(timeStamp) ;
  var precision = extractElementsFromTimePrecision(timePrecision) ;
  var months = {1:"janvier", 2:"février", 3:"mars", 4:"avril", 5:"mai", 6:"juin", 7:"juillet", 8:"août", 9:"septembre", 10:"octobre", 11:"novembre", 12:"décembre"} ;
  
  var frenchTimeString = "";
  if (precision == "millenium"){
    var millenium = String(Math.ceil(parseInt(timeElems.year)/1000)) ;
    var superscript = "e"
    if (millenium = "1"){superscript = "re"};
    var frenchTimeString =  millenium + superscript + " millénaire" ;
  } else if (precision == "century"){
    var century = String(Math.ceil(parseInt(timeElems.year)/100)) ;
    var superscript = "e"
    if (century = "1"){superscript = "er"};
    var frenchTimeString =  millenium + superscript + " siècle" ;
  } else if (precision == "decade"){
    var decade = String(Math.trunc(parseInt(timeElems.year)/10)*10) ;
    var frenchTimeString =  "années " + decade ;
  } else if (precision == "year"){
    var year = timeElems.year ;
    var frenchTimeString = year ;
  } else if (precision == "month"){
    var year = timeElems.year ;
    var intMonth = parseInt(timeElems.month);
    var month = months[intMonth] ;
    var frenchTimeString =  month + " " + year ;
  } else if (["day", "hours", "minutes", "seconds", "milliseconds"].includes(precision)){
    var year = timeElems.year ;
    var intMonth = parseInt(timeElems.month);
    var month = months[intMonth] ;
    var day = timeElems.day ;
    if (day == "1"){day = "1er"}
    var frenchTimeString =  day + " " + month + " " + year ;
  }

  timeElems.label = frenchTimeString ;
  timeElems.precision = precision
  return timeElems;
}

function createTimelineTime(year=null, month=null, day=null, hour=null, minute=null, second=null, millisecond=null, format=null){
  year = (!year) ? '' : year ;
  month = (!month) ? '' : month ;
  day = (!day) ? '' : day ;
  hour = (!hour) ? '' : hour ;
  minute = (!minute) ? '' : minute ;
  second = (!second) ? '' : second ;
  millisecond = (!millisecond) ? '' : millisecond ;
  format = (!format) ? '' : format ;
  return {year, month, day, hour, minute, second, millisecond, format}
}

function createTime(timeStamp, timePrecision){
  var timeElems = extractElementsFromTimeStamp(timeStamp) ;
  var precision = extractElementsFromTimePrecision(timePrecision) ;
  timeElems = correctTimeAccordingPrecision(timeElems, precision) ;

  var time = createTimelineTime(timeElems.year, timeElems.month, timeElems.day, timeElems.hour, timeElems.minute, timeElems.second, timeElems.millisecond, timeElems.format) ;
  return time
}

function createTimeFromTwoTimes(timeStamp1, timePrecision1, timeStamp2, timePrecision2){
  var time1 = getDateObjectFromTimeStamp(timeStamp1) ;
  var time2 = getDateObjectFromTimeStamp(timeStamp2) ;
  var meanTimes = getMeanOfTwoTimes(time1, time2);
  var meanTimesElems = extractElementsFromTime(meanTimes);
  var precision = "day" ;
  meanTimesElems = correctTimeAccordingPrecision(meanTimesElems, precision) ;
  var time = createTimelineTime(meanTimesElems.year, meanTimesElems.month, meanTimesElems.day,
    meanTimesElems.hour, meanTimesElems.minute, meanTimesElems.second, meanTimesElems.millisecond,
    meanTimesElems.format) ;
  return time
}

function correctTimeAccordingPrecision(time, precision){
  time.format = null ;

  if (precision == "year"){
    time.month, time.day, time.hour, time.minute, time.second, time.millisecond = null, null, null, null, null, null ;
  }else if (precision == "month"){
    time.day, time.hour, time.minute, time.second, time.millisecond = null, null, null, null, null ;
  }else if (precision == "day"){
    time.hour, time.minute, time.second, time.millisecond = null, null, null, null ;
  }

  return time
}

function getMeanOfTwoTimes(time1, time2){
  var intTime1 = time1.getTime() ;
  var intTime2 = time2.getTime() ;
  var meanIntTimes = (intTime1 + intTime2) / 2 ;
  var meanTimes = new Date(meanIntTimes) ;
  return meanTimes ;
}

function getMeanOfTwoTimesFromStamps(timeStamp1, timeStamp2){
  var formattedTimeStamp1 = timeStamp1.replace("+",""); // Retirer le +
  var formattedTimeStamp2 = timeStamp2.replace("+",""); // Retirer le +
  var time1 = new Date(formattedTimeStamp1); // Créer un objet Date
  var time2 = new Date(formattedTimeStamp2); // Créer un objet Date
  return getMeanOfTwoTimes(time1, time2);
}

function extractElementsFromTimePrecision(timePrecision){
  if (timePrecision == "http://www.w3.org/2006/time#unitDay"){
    return "day";
  }else if (timePrecision == "http://www.w3.org/2006/time#unitMonth"){
    return "month"
  }else if (timePrecision == "http://www.w3.org/2006/time#unitYear"){
    return "year"
  }else if (timePrecision == "http://www.w3.org/2006/time#unitDecade"){
    return "decade"
  }else if (timePrecision == "http://www.w3.org/2006/time#unitCentury"){
    return "century"
  }else if (timePrecision == "http://www.w3.org/2006/time#unitMillenium"){
    return "millenium"
  }else{
    return null
  }
}

function getDateObjectFromTimeStamp(timeStamp){
  // Convertir la chaîne en un objet datetime
  // Note : le "Z" indique UTC (temps universel coordonné), donc on l'enlève avec replace
  // var formattedTimeStamp = timeStamp.replace("Z", "").replace("+",""); // Retirer le "Z" et le +
  var formattedTimeStamp = timeStamp.replace("+",""); // Retirer le +
  var date = new Date(formattedTimeStamp); // Créer un objet Date
  return date;
}

function extractElementsFromTimeStamp(timeStamp){
  var time = getDateObjectFromTimeStamp(timeStamp);
  return extractElementsFromTime(time);

}

function extractElementsFromTime(time){
  // Récupérer les différentes parties
  var year = String(time.getUTCFullYear());      // Année
  var month = String(time.getUTCMonth() + 1);   // Mois (commence à 0, donc ajouter 1)
  var day = String(time.getUTCDate());          // Jour
  var hours = String(time.getUTCHours());       // Heures
  var minutes = String(time.getUTCMinutes());   // Minutes
  var seconds = String(time.getUTCSeconds());   // Secondes
  var milliseconds = String(time.getUTCMilliseconds()); // Millisecondes
  return { year, month, day, hours, minutes, seconds, milliseconds }
}


// Gestion du côté leaflet

function initLeaflet() {
    // Initialisation de la carte
    var map = L.map('map').setView([48.8566, 2.3522], 13); // Coordonnées de Paris

    // Ajout de la couche de tuiles OpenStreetMap
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);
    return map;
}


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

function addGeometriesOfVersion(version, map){

  // Suppression de toutes les géométries
  layersToRemove.forEach(layer => {
    map.removeLayer(layer);
  });


  version.values.forEach(element => {
    displayGeometryOnMap(element, map);
  });
}

function getCrsFromWkt(geomWkt){
  var match = geomWkt.match(/EPSG\/0\/(\d+)/); // Cherche le numéro après "EPSG/0/"

  if (match) {
      var crsCode = match[1];
      return crsCode;
  } else {
      console.log("CRS non trouvé");
      return null;
  }
}

function displayGeometryOnMap(element, map){
  if (element.datatype !=  "http://www.opengis.net/ont/geosparql#wktLiteral"){return null} ;
  var wktWithCrs = element.value ;
  var crsCode = getCrsFromWkt(wktWithCrs);
  var geomWkt = wktWithCrs.replace(/<.*?>\s*/, "");

  if (crsCode == null) { crsCode = "4326"; }

  var geoJsonGeom = projectWkt(geomWkt, 'EPSG:' + crsCode, 'EPSG:4326');
  var layer = L.geoJSON().addTo(map);
  layer.addData(geoJsonGeom);
  map.fitBounds(layer.getBounds());
  layersToRemove.push(layer);
}

function changeSelectedLandmark(event){
  initTimeline(graphDBRepositoryURI, dropDownMenu.value, factsNamedGraphURI, map=map);
}

function getLandmarks(graphDBRepositoryURI, dropDownMenu){
  var query = `
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
SELECT ?lm ?lmLabel ?lmType ?lmTypeLabel ?relatumLabel
WHERE {
    ?lm rdfs:label ?lmPartLabel .
    FILTER(LANG(?lmPartLabel) IN ("fr", ""))
    {
        SELECT DISTINCT ?lm ?lmType WHERE {
            BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?g)
            GRAPH ?g { ?lm a addr:Landmark . }
            ?lm addr:isLandmarkType ?lmType .
        }
    }
    OPTIONAL {
        ?lmType skos:prefLabel ?lmTypeLabel .
        FILTER(LANG(?lmTypeLabel) IN ("fr", ""))
    }
    OPTIONAL {
        ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?lm ; addr:relatum [rdfs:label ?relatumLabel] .
        FILTER(LANG(?relatumLabel) IN ("fr", ""))
    }
    BIND(IF(BOUND(?relatumLabel), CONCAT(?lmPartLabel, " ", ?relatumLabel), ?lmPartLabel) AS ?lmLabel)
}
    ORDER BY ?lmTypeLabel ?lmLabel
` ;

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

/////////////////////////////////////////////////////////////////////

// Changer dynamiquement la largeur de la carte et de la timeline

var resizer = document.querySelector('.resizer');
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


//////////////////////////////////////////////////////////////////

var layersToRemove = [];
// Appel aux fonctions d'initialisation
var map = initLeaflet();
//initTimeline(graphDBRepositoryURI, lmLabel, lmLabelLang, map=map);

// var button = document.getElementById("enterName") ;
var dropDownMenu = document.getElementById(landmarkNamesDivId);

// Afficher la timeline quand on clique sur un bouton (ou entrée dans le drop menu)
// button.addEventListener("click",changeSelectedLandmark);
dropDownMenu.addEventListener("change",changeSelectedLandmark);

// Afficher les landmarks dans un menu déroulant
getLandmarks(graphDBRepositoryURI, dropDownMenu) ;
