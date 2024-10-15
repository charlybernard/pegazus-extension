# ODySSEA: Ontologie des DYnamiques Spatio-temporelles pour les Suivi d'Entités géographiques et d'Adresses

This repository contains the documentation of the ODySSEA (Ontology of spatio-temporal dynamics for geographical entities and addresses) ontology and knowledge graph construction method.

## Structure of the repository
```
├── data                      <- Raw resources used to build the graph
├── ontology                  <- ODySSEA Ontology
│   ├── ontology.ttl          <- Core part of the ontology to describe landmarks and addresses
│   ├── documentation
│       ├── addresses
│       ├── sources
│       ├── temporal_evolution
├── scripts                    <- Implementation of the algorithm
├── LICENCE.md
└── README.md
```

### `data` folder

This folder stores files used to build knowledge graph. It contains csv and geojson files which describe addresses and streets from different sources at different times (RDF resources are built during process.

⚠️ To get more information about their content, please read their readme : [addresses](data/README.md).

### `ontology` folder
`ontology.ttl` describes the ontology: it is the core part of the ontology and describe landmarks and addresses.

[Ontology documentation](ontology/documentation) is divided into as many parts as there are modelets:
* [`addresses`](ontology/documentation/addresses) ;
* [`sources`](ontology/documentation/sources) ;
* [`temporal_evolution`](ontology/documentation/temporal_evolution).

Each modelet documentation has 3 or 4 files:
* `{modelet_name}_scenario.md`: the natural-language argument describing the sub-problem to be addressed ;
* `{modelet_name}_glossary.md`: glossary which defines the main terms involved ;
* `{modelet_name}_competency_questions.md`: set of informal competence questions (in natural language) which represent the questions to be answered by the knowledge base ;
* `{modelet_name}_sparql_queries.md`: it translates informal competence questions into SPARQL queries.

### `scripts` folder
This folder contains code to build knowledge graphs.

⚠️ To get more information about their content, please read their [readme](scripts/README.md).
