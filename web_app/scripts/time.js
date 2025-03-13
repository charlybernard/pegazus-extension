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