//////////////////////////////// Variables //////////////////////////////////

const uiConfig = {

  dateSlider: {
    min : 0,
    max : 100,
    value : 0
  },

  divIds: {
    content: "content",
    selection: "selection",

    radioInput: "radio-input",
    radioInputButtons: "radio-input-buttons",

    graph: "graph",
    graphSelection: "graph-selection",

    landmarkSelection: "landmark-selection",
    landmarkValidTime: "landmark-valid-time",

    landmarkTypeNames: "landmark-type-names",
    landmarkTypeNamesLabel: "landmark-type-names-label",

    landmarkNames: "landmark-names",
    landmarkNamesLabel: "landmark-names-label",

    landmarkSelectionSuggestions: "landmark-suggestions",

    landmarkValidationButton: "landmark-validation-button",

    mapTimeline: "map-timeline",
    timeline: "timeline",

    map: "leaflet-map",
    mapTimelineResizer: "map-timeline-resizer",

    dateSelection: "date-selection",
    dateSlider: "date-slider",
    dateInput: "date-input",
    dateValidationButton: "date-validation-button"
  },

  classNames: {
    resizer: "resizer"
  },

  labels: {
    radioInput: "Type de visualisation",
    validationButton: "Valider",
    landmarkSelection: "Entité à sélectionner : ",
    dateSelection: "Sélectionnez une date : ",
    graphSelection: "Graphe à sélectionner : ",
    searchLandmarksPlaceholder: "Rechercher un landmark...",

    snapshot: "Snapshot",
    landmarkEvolution: "Évolution des repères"
  },

  lang : "fr",

  dropDownMenu : {
    limit: 20,
  },

  map: {
    lat: 48.8566,
    lon: 2.3522,
    zoom: 13,

    style: {
        height:"90%",
        width:"100%",
    },

    messages: {
      noLandmarkToDisplay: "Aucun repère à afficher à cette date.",
      nameTitle: "Nom",
      flyOverLandmark: "Survolez un lieu",
      landmarkSelectValue: "Sélectionnez une entité",
      landmarkTypeSelectValue: "Sélectionnez un type d'entité",
      graphSelectValue: "Sélectionnez un graphe"
    },

    tileLayers: [
      // { type:"xyz", url:"https://tile.openstreetmap.org/{z}/{x}/{y}.png", name:"OpenStreetMap" },
      { type:"xyz", url:"https://tile.openstreetmap.de/{z}/{x}/{y}.png ", name:"OpenStreetMap" },
      { type:"wms", url:"https://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:verniquet_1789", name:"Atlas de Verniquet (1789)" },
      { type:"wms", url:"https://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:jacoubet_1836", name:"Plan de Jacoubet (1836)" },
      { type:"wms", url:"https://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:andriveau_1849", name:"Plan d'Andriveau (1849)" },
      { type:"wms", url:"https://geohistoricaldata.org/geoserver/paris-rasters/wms", layer:"paris-rasters:poubelle_1888", name:"Plan Poubelle (1888)" },
      { type:"xyz", url:"https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png", name:"OpenStreetMap Hot" }
    ]
  },

  radioInputs :{
    values : {
      snapshot: {
        label: "Snapshot",
        id: "snapshot-selection"
      },
      timeline: {
        label: "Évolution des repères",
        id: "timeline-selection"
      }
    },

    style: {
      display: "flex",
      flexDirection: "row",
    }
  },

  timeline: {
    startTimestamp: "1790-01-01",
    endTimestamp: "2027-01-01",
    timeDelay: 20, // years (null = no delay),
    headlineLabel: "Attributs de l'entité géographique"
  },

  calendar: {
    uri: gregorianCalendarURI
  },

  layers: {
    certain: "Certains",
    uncertain: "Incertains",
  },

  types: {
    attributes: null, // to be filled with the result of the query getQueryForAttributeTypes
    landmarks: null, // to be filled with the result of the query getQueryForLandmarkTypes
  }
};