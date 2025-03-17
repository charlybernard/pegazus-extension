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
  
  
  function createInputRadioDiv(L, input){
    var emptyDiv = createDiv(L, "div", {}, null, null);
    var labelDiv = createLabel(L, input.name, input.label, null, labelContentIsBold = true);
    var radioInputDiv = createDiv(L, "div", attributes={"id":input.name, "name":input.name});
  
    for (key in input.values){
        var emptyRadioDiv = createDiv(L, "div", {}, null, null);
        var optionDiv = createInputRadio(L, input.name, input.values[key].label, null, null, isChecked=false);
        var labelRadioDiv = createLabel(L, input.name, input.values[key].label, null, labelContentIsBold = true);
        emptyRadioDiv.appendChild(optionDiv);
        emptyRadioDiv.appendChild(labelRadioDiv);
        radioInputDiv.appendChild(emptyRadioDiv);
    }
    emptyDiv.appendChild(labelDiv);
    emptyDiv.appendChild(radioInputDiv);
  
    return emptyDiv;
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
    example of input: {"name":"visu_selection", "label":"Type de visualisation", "values":{"snapshot":{"label":"Snapshot"}, "timeline":{"label":"Timeline"}}}
    `
    var emptyDiv = createDiv(L, "div", {}, null, null);
    var labelDiv = createLabel(L, input.name, input.label, null, labelContentIsBold = true);
    var radioInputDiv = createDiv(L, "div", attributes={"id":input.name, "name":input.name});
  
    for (key in input.values){
        var emptyRadioDiv = createDiv(L, "div", {}, null, null);
        var optionDiv = createInputRadio(L, input.name, input.values[key].label, null, null, isChecked=false);
        var labelRadioDiv = createLabel(L, input.name, input.values[key].label, null, labelContentIsBold = true);
        emptyRadioDiv.appendChild(optionDiv);
        emptyRadioDiv.appendChild(labelRadioDiv);
        radioInputDiv.appendChild(emptyRadioDiv);
    }
    emptyDiv.appendChild(labelDiv);
    emptyDiv.appendChild(radioInputDiv);
  
    return emptyDiv;
  }