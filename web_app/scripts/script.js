var value = "Snapshot";
var name = "visu_selection";
var inputId = "snapshot";
var inputClass = "visu_selection";
var isChecked = false;

var div = createInputRadio(L, name, value, inputId, inputClass, isChecked=false);
document.body.appendChild(div);

var inputs = {"name":"visu_selection", "label":"Type de visualisation", "values":{"snapshot":{"label":"Snapshot"}, "timeline":{"label":"Timeline"}}}

var div2 = createInputRadioDiv(L, inputs, isReadOnly=false);
document.body.appendChild(div2);
