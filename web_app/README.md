# Web visualisation for timeline

This repository contains the documentation of the ODySSEA (Ontology of spatio-temporal dynamics for geographical entities and addresses) ontology and knowledge graph construction method.

## Structure of the repository

📂 web-app  
├── 📄 [`README.md`](./README.md)  
├── 📄 [`index.html`](./index.html)  
├── 📂 [`scripts`](./scripts/)  
│   ├── 📄 [`func-evolution.js`](./scripts/func-evolution.js)  
│   ├── 📄 [`func-snapshot.js`](./scripts/func-snapshot.js)  
│   ├── 📄 [`geometry.js`](./scripts/geometry.js)  
│   ├── 📄 [`html-utils.js`](./scripts/html-utils.js)  
│   ├── 📄 [`queries.js`](./scripts/queries.js)  
│   ├── 📄 [`script-evolution.js`](./scripts/script-evolution.js)  
│   ├── 📄 [`script-snapshot.js`](./scripts/script-snapshot.js)  
│   ├── 📄 [`script.js`](./scripts/script.js)  
│   ├── 📄 [`time-slider.js`](./scripts/time-slider.js)  
│   └── 📂 [`libs`](./scripts/libs/)    
│   │   ├── 📄 [`jquery.js`](./scripts/libs/jquery.js)  
│   │   ├── 📄 [`leaflet.js`](./scripts/libs/leaflet.js)  
│   │   ├── 📄 [`proj4.js`](./scripts/libs/proj4.js)  
│   │   ├── 📄 [`terraformer.js`](./scripts/libs/terraformer.js)  
│   │   ├── 📄 [`terraformer-wkt-parser.js`](./scripts/libs/terraformer-wkt-parser.js)  
│   │   └── 📄 [`timeline.js`](./scripts/libs/timeline.js)   
├── 📂 [`styles`](./styles/)   
│   ├── 📄 [`style.css`](./styles/style.css)  
│   └── 📂 [`libs`](./styles/libs/)   
│   │   ├── 📄 [`jquery.js`](./styles/libs/leaflet.css)  
│   │   └── 📄 [`timeline.css`](./styles/libs/timeline.css)    
└── 📄 [`settings.js`](./settings.js)  

### `styles` - CSS files for styling the webpage
This folder contains the CSS styles for the visual presentation of the application.

- **`ext/`**: This subfolder contains the CSS files for external libraries.
  - **`leaflet.css`**: Style file for the [Leaflet](https://leafletjs.com/) library (version [1.9.4](https://unpkg.com/leaflet@1.9.4/dist/leaflet.css)), used for displaying interactive maps.
  - **`timeline.css`**: Style file for the [Timeline.js](https://timeline.knightlab.com/) library, used for displaying events in a timeline format.

- **`style.css`**: The main CSS file that defines the general style of the web application. It controls the overall look and feel of the user interface, such as layout, colors, fonts, etc.

### `scripts` - JavaScript files for the functionality of the application
This folder contains the JavaScript files necessary for managing user interactions and the core functionality of the site.

- **`libs`**: This subfolder contains the third-party JavaScript libraries used in the project.
  - **`jquery.js`**: File to use the [jQuery](https://jquery.com/) library (version 3.6.1), which simplifies DOM manipulation and event handling.
  - **`leaflet.js`**: File to use the [Leaflet](https://leafletjs.com/) library (version [1.9.4](https://unpkg.com/leaflet@1.9.4/dist/leaflet.js)), which is used to display interactive maps.
  - **`proj4.js`**: [Proj4js](https://github.com/proj4js/proj4js) library (version [2.9.0](https://cdnjs.cloudflare.com/ajax/libs/proj4js/2.9.0/proj4.js)), a JavaScript library to transform point coordinates from one coordinate system to another.
  - **`terraformer-wkt-parser.js`**: [Terraformer Well-Known Text Parser](https://github.com/Esri/terraformer-wkt-parser) library (version [1.1.2](https://unpkg.com/terraformer-wkt-parser@1.1.2/terraformer-wkt-parser.js)), a JavaScript library to parse Well-Known Text (WKT) into geo-objects.
  - **`terraformer.js`**: [Terraformer](https://github.com/terraformer-js/terraformer) library (version [1.0.9](https://unpkg.com/terraformer@1.0.8/terraformer.js)), a geographic toolkit for working with geometry, geography, formats, and building geodatabases.
  - **`timeline.js`**: File to use [Timeline.js](https://timeline.knightlab.com/), a JavaScript library for creating interactive timelines.

- **Main Scripts**: These JavaScript files implement the core functionalities of the application.
  - **`func-evolution.js`**: Functions to handle landmark evolutions in the application.
  - **`func-snapshot.js`**: Functions to manage snapshot selection and handling.
  - **`geometry.js`**: Functions to work with geometries, such as geometric transformations.
  - **`htmlutils.js`**: Functions to create HTML elements
  - **`queries.js`**: Functions to build and handle SPARQL queries for querying a graph database.
  - **`script-evolution.js`**: Main script for handling the evolution of landmarks.
  - **`script-snapshot.js`**: Main script for managing snapshot selection functionality.
  - **`script.js`**: Main script to be executed when the webpage is loaded.
  - **`time.js`**: Functions to manage time-related features in the application.
  - **`time-slider.js`**: Functions for controlling and managing the time slider.
  - **`utils.js`**: Utility functions that support the app’s general operations.

### `settings.js` - Configuration file for the GraphDB repository
This file contains the settings for connecting and interacting with the GraphDB repository, including the URL, language, and repository name.

### `index.html` - Main webpage
This is the HTML file that loads the web application, containing the structure and layout of the webpage.


## Requirements

Before launching the web app, please ensure `settings.js` is correctly filled out :
* `graphDBURL`: URL of the GraphDB ;
* `graphName`  name of the repository ;
* `lmLabelLang`: selected language of the labels of landmarks ;
* `namedGraphName`: name of the named graph in which the final construction is made.

Then, to allow the interaction with the repository, you have to avoid CORS errors. To do so, click on `Settings...` in GraphDB Desktop window.

![image](./images/graphdb-desktop.png)

Next, you can custom properties by typing their name and value.

![image](./images/graphdb-settings.png)

Properties are the following ones:
* `graphdb.workbench.cors.enable`:`true` 
* `graphdb.workbench.cors.origin`:`*`

Now you can launch the web app.

