function createDiv(L, divType, attributes = {}, innerHTML = "", divClass = ""){
    var div = L.DomUtil.create(divType, divClass);
  
    // Attributes is a dictionary whose keys are attributes and values are their values
    for (attr in attributes){
        var value = attributes[attr];
  
        // Don't add an attribute if its value is null
        if (value != null){
            div.setAttribute(attr, value)
        }
    }
  
    div.innerHTML = innerHTML;
  
    return div;
  }
  
  function createLabel(L, labelFor, labelContent, labelClass, labelContentIsBold = false){
    if (labelContentIsBold){
        var finalLabelContent = boldText(labelContent);
    }else{
        var finalLabelContent = labelContent;
    }
  
    return createDiv(L, "label", {"for": labelFor}, finalLabelContent, labelClass);
  }
  
  function createInputRadio(L, name, value, inputId, inputClass, isChecked=false){
    var divType = "input";
    var checked = getValueAccordingBool(isChecked);
    var attributes = {"type":"radio", "name":name, "value":value, "id": inputId, "checked":checked};
  
    return createDiv(L, divType, attributes, "", inputClass);
  }
  
  function boldText(textToBeBold){
    return `<b>${textToBeBold}</b>`;
  }
  

  
  function createInputText(L, name, value, inputId, inputClass, isRequired = true, isReadOnly = false){
    var divType = "input";
  
    var required = getValueAccordingBool(isRequired);
    var readOnly = getValueAccordingBool(isReadOnly);
  
    var attributes = {"type":"text", "name":name, "value":value, "id": inputId, "required":required, "readonly":readOnly};
  
    return createDiv(L, divType, attributes, "", inputClass);
  }
  
  function createInputTextDiv(L, input, isReadOnly){
    var emptyDiv = createDiv(L, "div", {}, null, null);
    var labelDiv = createLabel(L, input.name, input.label, null, labelContentIsBold = true);
  
    var textInputDiv = createInputText(L, input.name, null, input.name, null, isRequired = true, isReadOnly = isReadOnly);
    emptyDiv.appendChild(labelDiv)
    emptyDiv.appendChild(textInputDiv);
  
    return emptyDiv;
  }
  
  function createInputRadioDiv(L, input){
    `
    example of input: {"name":"visu_selection", "label":"Type de visualisation", "id":"visu-selection", 
                        "values":{"snapshot":{"label":"Snapshot"}, "timeline":{"label":"Timeline"}}}
    `

    emptyDivAttributes = {};
    if (input.id){
      emptyDivAttributes["id"] = input.id;
    }
    var emptyDiv = createDiv(L, "div", emptyDivAttributes, null, null);
    var labelDiv = createLabel(L, input.name, input.label, null, labelContentIsBold = true);
    var radioInputDiv = createDiv(L, "div", attributes={"id":input.name, "name":input.name});
  
    for (key in input.values){
        var emptyRadioDiv = createDiv(L, "div", {}, null, null);
        var optionDiv = createInputRadio(L, input.name, input.values[key].label, input.values[key].id, null, isChecked=false);
        var labelRadioDiv = createLabel(L, input.values[key].id, input.values[key].label, null, labelContentIsBold = true);
        emptyRadioDiv.appendChild(optionDiv);
        emptyRadioDiv.appendChild(labelRadioDiv);
        radioInputDiv.appendChild(emptyRadioDiv);
    }
    emptyDiv.appendChild(labelDiv);
    emptyDiv.appendChild(radioInputDiv);
  
    return emptyDiv;
  }

  function clearDiv(div){
    div.innerHTML = "";
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

////////////////////////////////////////////////// Functions to create option divs /////////////////////////////////////////////////

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