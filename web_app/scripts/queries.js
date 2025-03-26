function getValuesForQuery(variable, values){
    var strValues = "VALUES ?" + variable + "{"
    for (uri in values){
      strValues += "<" + uri + "> " ;
    }
    strValues += "}" ;
    return strValues ;
  }
  
function getQueryForLandmarks(namedGraphURI){
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
            BIND(<` + namedGraphURI + `> AS ?g)
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

    return query;
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


function getValidLandmarksFromTime(timeStamp, timeCalendarURI, namedGraphURI, lowTimeStamp=null, highTimeStamp=null){
    
    var lowTimeStampFilter = ``;
    var highTimeStampFilter = ``;
    if (lowTimeStamp){
        var lowTimeStampFilter = `BIND("${lowTimeStamp}"^^xsd:dateTimeStamp AS ?lowTimeStamp)
            BIND(?disTimeAfterStamp >= ?lowTimeStamp AS ?disTimeAfterExists)`;
    }
    if (highTimeStamp){
        var highTimeStampFilter = `BIND("${highTimeStamp}"^^xsd:dateTimeStamp AS ?highTimeStamp)
            BIND(?appTimeBeforeStamp <= ?highTimeStamp AS ?appTimeBeforeExists)`;;
    }

    var query = `
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ctype: <http://rdf.geohistoricaldata.org/id/codes/address/changeType/>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
  
    SELECT DISTINCT ?lm ?lmLabel ?existsForSure WHERE {
        BIND(<` + namedGraphURI + `> AS ?g)
        BIND("`+ timeStamp + `"^^xsd:dateTimeStamp AS ?timeStamp)
        BIND(<` + timeCalendarURI + `> AS ?timeCalendar)
  
        GRAPH ?g {
            ?lm a addr:Landmark ; rdfs:label ?lmPartLabel .
            OPTIONAL {
                ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?lm ; addr:relatum [rdfs:label ?relatumLabel] .
                FILTER(LANG(?relatumLabel) IN ("fr", ""))
            }
            BIND(IF(BOUND(?relatumLabel), CONCAT(?lmPartLabel, " ", ?relatumLabel), ?lmPartLabel) AS ?lmLabel)
            ?appCg addr:isChangeType ctype:LandmarkAppearance ; addr:appliedTo ?lm ; addr:dependsOn ?appEv .
            ?disCg addr:isChangeType ctype:LandmarkDisappearance ; addr:appliedTo ?lm ; addr:dependsOn ?disEv .
        }
  
        OPTIONAL {
            ?appEv addr:hasTime ?appTime .
            ?appTime addr:timeStamp ?appTimeStamp ; addr:timeCalendar ?timeCalendar .
            BIND(?appTimeStamp <= ?timeStamp AS ?appTimeExists)
        }
        OPTIONAL {
            ?disEv addr:hasTime ?disTime .
            ?disTime addr:timeStamp ?disTimeStamp ; addr:timeCalendar ?timeCalendar .
            BIND(?disTimeStamp > ?timeStamp AS ?disTimeExists)
        }
        OPTIONAL {
            ?appEv addr:hasTimeBefore ?appTimeBefore .
            ?appTimeBefore addr:timeStamp ?appTimeBeforeStamp ; addr:timeCalendar ?timeCalendar .
            `+ highTimeStampFilter + `
            BIND(?appTimeBeforeStamp <= ?timeStamp AS ?appTimeBeforeExistsForSure)
        }
        OPTIONAL {
            ?disEv addr:hasTimeBefore ?disTimeBefore .
            ?disTimeBefore addr:timeStamp ?disTimeBeforeStamp ; addr:timeCalendar ?timeCalendar .
            BIND(?disTimeBeforeStamp > ?timeStamp AS ?disTimeBeforeExists)
        }
        OPTIONAL {
            ?appEv addr:hasTimeAfter ?appTimeAfter .
            ?appTimeAfter addr:timeStamp ?appTimeAfterStamp ; addr:timeCalendar ?timeCalendar .
            BIND(?appTimeAfterStamp <= ?timeStamp AS ?appTimeAfterExists)
        }
        OPTIONAL {
            ?disEv addr:hasTimeAfter ?disTimeAfter .
            ?disTimeAfter addr:timeStamp ?disTimeAfterStamp ; addr:timeCalendar ?timeCalendar .
            `+ lowTimeStampFilter + `
            BIND(?disTimeAfterStamp > ?timeStamp AS ?disTimeAfterExistsForSure)
        }
  
        FILTER(!BOUND(?appTimeExists) || ?appTimeExists)
        FILTER(!BOUND(?disTimeExists) || ?disTimeExists)
        FILTER(BOUND(?appTimeExists) || !BOUND(?appTimeAfterExists) || ?appTimeAfterExists)
        FILTER(BOUND(?disTimeExists) || !BOUND(?disTimeBeforeExists) || ?disTimeBeforeExists)
        FILTER(BOUND(?appTimeExists) || !BOUND(?appTimeBeforeExists) || ?appTimeBeforeExists)
        FILTER(BOUND(?disTimeExists) || !BOUND(?disTimeAfterExists) || ?disTimeAfterExists)

        BIND(IF((BOUND(?appTimeBeforeExistsForSure) && !?appTimeBeforeExistsForSure) || (BOUND(?disTimeAfterExistsForSure) && !?disTimeAfterExistsForSure), "false"^^xsd:boolean, "true"^^xsd:boolean) AS ?existsForSure)
    }
    `
    console.log(query);
    return query ;
  }
  
  function getValidAttributeVersionsFromTime(timeStamp, timeCalendarURI, namedGraphURI){
    var query = `
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ctype: <http://rdf.geohistoricaldata.org/id/codes/address/changeType/>

SELECT DISTINCT ?vers ?versValue ?existsForSure ?attrType ?lm WHERE {
    BIND(<` + namedGraphURI + `> AS ?g)
    BIND("`+ timeStamp + `"^^xsd:dateTimeStamp AS ?timeStamp)
    BIND(<` + timeCalendarURI + `> AS ?timeCalendar)

    GRAPH ?g {
        ?vers a addr:AttributeVersion .
        ?attr a addr:Attribute ; addr:isAttributeType ?attrType.
        ?lm addr:hasAttribute ?attr .
        ?vers addr:versionValue ?versLabel .
        ?meCg addr:makesEffective ?vers ; addr:appliedTo ?attr ; addr:dependsOn ?meEv .
        ?oCg addr:outdates ?vers ; addr:appliedTo ?attr ; addr:dependsOn ?oEv .
    }

    OPTIONAL {
        ?vers addr:versionValue ?versValue .
    }

    OPTIONAL {
        ?meEv addr:hasTime ?meTime .
        ?meTime addr:timeStamp ?meTimeStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?meTimeStamp <= ?timeStamp AS ?meTimeExists)
    }
    OPTIONAL {
        ?oEv addr:hasTime ?oTime .
        ?oTime addr:timeStamp ?oTimeStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?oTimeStamp > ?timeStamp AS ?oTimeExists)
    }
    OPTIONAL {
        ?meEv addr:hasTimeBefore ?meTimeBefore .
        ?meTimeBefore addr:timeStamp ?meTimeBeforeStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?meTimeBeforeStamp <= ?timeStamp AS ?meTimeBeforeExists)
    }
    OPTIONAL {
        ?oEv addr:hasTimeBefore ?oTimeBefore .
        ?oTimeBefore addr:timeStamp ?oTimeBeforeStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?oTimeBeforeStamp > ?timeStamp AS ?oTimeBeforeExists)
    }
    OPTIONAL {
        ?meEv addr:hasTimeAfter ?meTimeAfter .
        ?meTimeAfter addr:timeStamp ?meTimeAfterStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?meTimeAfterStamp <= ?timeStamp AS ?meTimeAfterExists)
    }
    OPTIONAL {
        ?oEv addr:hasTimeAfter ?oTimeAfter .
        ?oTimeAfter addr:timeStamp ?oTimeAfterStamp ; addr:timeCalendar ?timeCalendar .
        BIND(?oTimeAfterStamp > ?timeStamp AS ?oTimeAfterExists)
    }

    FILTER(!BOUND(?meTimeExists) || ?meTimeExists)
    FILTER(!BOUND(?oTimeExists) || ?oTimeExists)
    FILTER(BOUND(?meTimeExists) || !BOUND(?meTimeAfterExists) || ?meTimeAfterExists)
    FILTER(BOUND(?oTimeExists) || !BOUND(?oTimeBeforeExists) || ?oTimeBeforeExists)

    BIND(IF((BOUND(?meTimeBeforeExists) && !?meTimeBeforeExists) || (BOUND(?oTimeAfterExists) && !?oTimeAfterExists), "false"^^xsd:boolean, "true"^^xsd:boolean) AS ?existsForSure)
}  
    `
    return query ;
  }

function getQueryForAttributeVersionValues(valuesForQuery){
    var query = `
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    SELECT DISTINCT ?vers ?val WHERE {` + valuesForQuery +
    `?vers addr:versionValue ?val }`

    return query;
  }