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