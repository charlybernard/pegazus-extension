function getGraphDBRepositoryURI(graphDBURI, graphName){
  return graphDBURI + "/repositories/" + graphName ;
}

function getNamedGraphURI(graphDBURI, graphName, namedGraphName){
  return graphDBURI + "/repositories/" + graphName + "/rdf-graphs/" + namedGraphName ;
}


//////////////////////////////////////// Functions to transform values from SPARQL results ////////////////////////////////////////////////////

function getBooleanFromXSDBoolean(xsdBoolean){
  if (xsdBoolean.datatype == "http://www.w3.org/2001/XMLSchema#boolean" && xsdBoolean.type == "literal"){ 
    if (xsdBoolean.value == "false") { return false ; }
    else if (xsdBoolean.value == "true") { return true ; }
  }
  return null ;  
}

//////////////////////////////////////// Functions to manage boolean values /////////////////////////////////////////////////////////////////////

function getValueAccordingBool(boolValue){
  // This function returned a null of empty value according boolValue :
  // - value = "" if boolValue is true
  // - value = null if boolValue is false

  if (boolValue){
      return "";
  }else{
      return null;
  }
}