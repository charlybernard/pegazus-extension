//////////////////////////////// Actions on the page //////////////////////////////////

const graphDBRepositoryURI = getGraphDBRepositoryURI(graphDBURI, graphName) ;
// Object of LeafletObjects class which contains all markers and dots*
const lo = new LeafletObjects(L);

createMainHTML(L, graphDBRepositoryURI, uiConfig);
const contentDiv = document.getElementById(uiConfig.divIds.content);
const selectDiv = document.getElementById(uiConfig.divIds.selection);
const inputRadioDiv = document.getElementById(uiConfig.divIds.radioInput);
const graphSelectionDiv = document.getElementById(uiConfig.divIds.graphSelection);

var namedGraphURI = null;

// Initial display of the map with the first graph in the dropdown menu
inputRadioDiv.addEventListener("change", function () {
    namedGraphURI = graphSelectionDiv.value;
    handleRadioChange(L, uiConfig, graphDBRepositoryURI, namedGraphURI);
  });

graphSelectionDiv.addEventListener('change', function(){
    namedGraphURI = graphSelectionDiv.value;
});