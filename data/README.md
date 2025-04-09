# Initial data used to build the final KG

## BAN
File: `ban_adresses.csv`

Data from [Base Adresse Nationale (BAN)](https://adresse.data.gouv.fr/base-adresse-nationale) are available [here](https://adresse.data.gouv.fr/data/ban/adresses/latest/csv). For this project, downloaded data are related to Paris (`adresses-75.csv.gz`). Addresses selected from this file correspond to Faubourg Saint-Antoine area. File name must correspond to `bpa_csv_file_name` in the notebook.

## OSM
Files: `osm_adresses.csv` and `osm_hn_adresses.csv`

These files are the results of two queries from [OSM planet SPARQL endpoint](https://qlever.cs.uni-freiburg.de/osm-planet). See *Bast, H., Brosi, P., Kalmbach, J., & Lehmann, A. (2021, November). An efficient RDF converter and SPARQL endpoint for the complete OpenStreetMap data. In Proceedings of the 29th International Conference on Advances in Geographic Information Systems (pp. 536-539)*.

Extracted data from OpenStreetMap are :
* house numbers (_house numbers_) : their value (a number and optionally a complement), their geometry, the thoroughfare or the district they belong to ;
* thoroughfares : their name
* districts : their name and INSEE code.

In the query interface, there are two queries to launch to extract Paris addresses.
* Query 1 :
```
PREFIX osmrel: <https://www.openstreetmap.org/relation/>
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
PREFIX osmrdf: <https://osm2rdf.cs.uni-freiburg.de/rdf/member#>
PREFIX osm: <https://www.openstreetmap.org/>
PREFIX ogc: <http://www.opengis.net/rdf#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?houseNumberId ?streetId ?streetName ?arrdtId ?arrdtName ?arrdtInsee
 WHERE {
  ?selectedArea osmkey:wikidata "Q90"; ogc:sfContains ?houseNumberId.
  ?houseNumberId osmkey:addr:housenumber ?housenumberName.
  ?arrdtId ogc:sfContains ?houseNumberId; osmkey:name ?arrdtName; osmkey:ref:INSEE ?arrdtInsee; osmkey:boundary "administrative"; osmkey:admin_level "9"^^xsd:int .
  ?streetId osmkey:type "associatedStreet"; osmrel:member ?member; osmkey:name ?streetName.
  ?member osmrel:member_role "house"; osmrel:member_id ?houseNumberId.
}
```

* Query 2 :
```
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
PREFIX ogc: <http://www.opengis.net/rdf#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT DISTINCT ?houseNumberId ?houseNumberLabel ?houseNumberGeomWKT
 WHERE {
  ?selectedArea osmkey:wikidata "Q90"; ogc:sfContains ?houseNumberId.
  ?houseNumberId osmkey:addr:housenumber ?houseNumberLabel; geo:hasGeometry ?houseNumberGeom.
  ?houseNumberGeom geo:asWKT ?houseNumberGeomWKT.
}
```

The queries select all the house numbers in Paris, but it is possible to change the extraction zone by modifying the `osmkey:wikidata ‘Q90’` condition. For example, you can replace it with `osmkey:wikidata ‘Q2378493’` to restrict it to the Maison Blanche district of Paris (district where is Butte-aux-Cailles area is located). Note that only building numbers belonging to an `associatedStreet` type relationship and having the `house` role in this relationship are selected. For each query, results are exported to `csv` files: `osm_adresses.csv` for query 1 and `osm_hn_adresses.csv` for query 2. There are two queries instead of one because, endpoint is not able to return any result (due to limited performances).

## Wikidata
Files: `wd_paris_landmarks.csv` and `wd_paris_locations.csv`

Via Wikidata, the extracted data are:
* geographical entities:
    * Paris thoroughfares (current and old ones) ;
    * areas linked to Paris:
      * districts of Paris ;
      * arrondissements (those before and after 1860) of Paris;
      * communes (past and present) of the former department of Seine;
* the relationships between these geographical entities.

Obtaining these files is straightforward. Simply run the `get_data_from_wikidata()` function defined in the notebook. To avoid calling Wikidata SPARQL endpoint each time notebook is run and keeping all data of Paris, function can be commented.

## Ville de Paris
Files: `denominations-emprises-voies-actuelles.csv` and `denominations-des-voies-caduques.csv`

* the first file comes from [dénominations des emprises des voies actuelles](https://opendata.paris.fr/explore/dataset/denominations-emprises-voies-actuelles) which a list of current names of thoroughfares of Paris.
* the second one comes from [dénominations caduques des voies](https://opendata.paris.fr/explore/dataset/denominations-des-voies-caduques) which a list of former names of thoroughfares (current and old) of Paris.

For this work, we kept thoroughfares around Faubourg Saint-Antoine district.

## Geojson files
Geojson files are geometrical data of an area (here Faubourg Saint-Antoine) at a given time.

Files:
* `plan_andriveau_1849_voies.geojson`: thoroughfares around 1849 according Andriveau atlas ;
* `atlas_jacoubet_1836_adresses.geojson`: addresses from Jacoubet's 1836 atlas ;
* `atlas_municipal_1888_voies.geojson`: thoroughfares around 1888 according municipal atlas of Paris ;
* `atlas_jacoubet_1836_voies.geojson`: thoroughfares from Jacoubet's 1836 atlas ;
* `atlas_vasserot_1810_adresses.geojson`: addresses from Vasserot's 1810 atlas ;
* `atlas_verniquet_1791_voies.geojson`: thoroughfares around 1791 according Verniquet atlas ;
* `denominations-des-voies-caduques.csv`: list of former names of thoroughfares in Paris ;
* `plan_delagrive_1728_voies.geojson`: thoroughfares around 1728 according to Delagrive's plan ;
* `cadastre_paris_1807_adresses.geojson`: addresses from the 1807 Paris cadastre ;
* `atlas_vasserot_1810_voies.geojson`: thoroughfares from Vasserot's 1810 atlas ;
* `atlas_municipal_1888_adresses.geojson`: addresses from the 1888 municipal atlas of Paris.

## Event file

`events.json`: This file contains historical events related to the development of thoroughfares and districts in Paris, particularly those impacting the area. Each event entry includes several key pieces of information to understand its impact on the road network and urban development of the time.

### Structure of `events.json` File

The `events.json` file contains events related to the development of thoroughfares and districts. Below is an explanation of its structure.

#### 1. **Source Object**

The `source` object provides information about the origin of the data, including the source link and details about the publisher.

##### Example of the `source` object:

```json
"source":{
  "uri":"https://fr.wikipedia.org/",
  "label":"Wikipedia",
  "lang":"fr",
  "comment":"Encyclopédie libre, universelle et collaborative",
  "publisher":{
      "uri":"https://fr.wikipedia.org/wiki/Wikipedia:À_propos",
      "label":"Contributeurs de Wikipedia"
  }
}
```

- \`\`: The URL of the source, in this case, `Wikipedia`.
- \`\`: The name of the source, e.g., "Wikipedia".
- \`\`: The language the data is in, here "fr" for French.
- \`\`: A brief description of the source.
- \`\`: An object containing information about the publisher of the source, including a link and the publisher's name.

#### 2. **Events Array**

The `events` array contains a list of events. Each event represents a significant change or transformation that affects landmarks (e.g., streets or places).

##### Example of an event object:

```json
{
   "time": { ... },
   "label": "By municipal decree on August 30, 1978, its eastern section, from rue Bobillot to rue du Moulin-des-Prés, was named rue du Père-Guérin.",
   "lang": "fr",
   "landmarks": [ ... ],
   "relations": [ ... ],
   "provenance": { ... }
}
```

Each event object has several key properties:

### \`\` (Time of the event)

The `time` object specifies when the event occurred. It includes:

- `stamp`: The exact date of the event (e.g., `"1978-08-30"`).
- `calendar`: The calendar used (e.g., "gregorian").
- `precision`: The precision of the date (e.g., "day" for a specific day, "year" for a specific year).

#### Example:

```json
"time": {
   "stamp": "1978-08-30",  // Date of the event
   "calendar": "gregorian",  // Calendar used
   "precision": "day"  // Precision of the event (specific day)
}
```

### \`\` (Description of the event)

The `label` is a textual description that explains what happened during the event. It could describe an administrative change, such as a new name given to a street, or a modification in urban planning.

#### Example:

```json
"label": "By municipal decree on August 30, 1978, its eastern section, from rue Bobillot to rue du Moulin-des-Prés, was named rue du Père-Guérin."
```

### \`\` (Language of the event)

The `lang` property indicates the language in which the event is documented. In this example, it is in French (`"fr"`).

### \`\` (Landmarks affected)

The `landmarks` array contains objects representing the landmarks (usually streets or places) affected by the event. Each landmark has a unique identifier and details about the changes made to it.

#### Example of the `landmarks` object:

```json
"landmarks": [
   {
      "id": 1,  // Unique identifier of the landmark
      "label": "rue du Père Guérin",  // Name of the landmark
      "type": "thoroughfare",  // Type (here, a street)
      "changes": [ ... ]  // Changes applied to the landmark
   }
]
```

#### **Changes** to Landmarks

Each landmark contains a `changes` array that describes the changes made to the landmark's attributes. These changes can include:

- \`\`: The element that is being changed (e.g., "landmark" or "attribute").
- \`\`: The type of change (e.g., "appearance" or "disappearance").
- \`\`: The specific attribute being changed (e.g., "geometry" or "name").
- `** / **`: The new value of the attribute or the old value being replaced.

#### Example:

```json
"changes": [
   {
      "on": "landmark",
      "type": "appearance"
   },
   {
      "on": "attribute",
      "attribute": "geometry"
   },
   {
      "on": "attribute",
      "attribute": "name",
      "makes_effective": [
         {
            "value": "rue du Père Guérin",
            "lang": "fr"
         }
      ]
   }
]
```

### \`\` (Relations between landmarks)

The `relations` array describes relationships between landmarks. Each relationship includes:

- \`\`: The type of relationship (e.g., "touches").
- \`\`: The first landmark in the relationship.
- \`\`: The second landmark in the relationship.

#### Example:

```json
"relations": [
   {
      "type": "touches",  // Relationship type
      "locatum": 1,  // ID of the first landmark
      "relatum": 2  // ID of the second landmark
   }
]
```

### \`\` (Source of the event)

The `provenance` object contains details about the source from which the event information was obtained.

#### Example:

```json
"provenance": {
   "uri": "https://fr.wikipedia.org/wiki/Rue_Bobillot",
   "label": "Wikipedia Page of Rue Bobillot",
   "lang": "fr"
}
```

- \`\`: The URL of the page from which the event was sourced.
- \`\`: The title of the page or source.
- \`\`: The language of the source.

---

This structure allows the tracking and representation of significant events related to urban development, such as street name changes, modifications in geometry, and the relationships between various landmarks. Each event can describe when and how these changes occurred, who initiated them, and how they impacted the physical layout of the city.

