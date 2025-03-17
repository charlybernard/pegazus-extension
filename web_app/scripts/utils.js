function getGraphDBRepositoryURI(graphDBURI, graphName){
  return graphDBURI + "/repositories/" + graphName ;
}

function getNamedGraphURI(graphDBURI, graphName, namedGraphName){
  return graphDBURI + "/repositories/" + graphName + "/rdf-graphs/" + namedGraphName ;
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

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Changer dynamiquement la largeur de la carte et de la timeline

function allowMapTimelineResize(resizerClassName, map) {

  var resizer = document.querySelector('.' + resizerClassName);
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
}

function setInnerHTMLToDivFromId(divId, content){
  var div = document.getElementById(divId);
  div.innerHTML = content;
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