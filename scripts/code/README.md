# `code` folder
This folder is composed of multiple python files to create and manage knowledge graph:
* `attribute_version_comparisons.py`: functions to compare attribute version thanks to their values ;
* `evolution_construction.py`: functions to make the process about construction of the evoltution of landmark attributes ;
* `factoids_creation.py`: functions to create graphs from different data, they are used for a specific source ;
* `factoids_cleaning.py`: functions to clean graphs created by functions in `factoids_creation.py` ;
* `file_management.py`: read or write files (csv, json, ttl...) ;
* `geom_processing.py`: functions to work with WKT geometries in knowledge graph ;
* `graphdb.py`: functions to make actions via GraphDB API (make queries, remove named graph...) ;
* `graphrdf.py`: functions to improve the use of RDFLib library
* `multi_source_processing.py`: functions which centralize process of all other files to construct and manage knowledge graph ;
* `namespaces.py`: initialisation of namespaces used for the KG ;
* `resource_initialisation.py`: functions to initialise resources with to RDFLib library ;
* `resource_rooting.py`: functions to root resources, i.e. to link resources from factoids named graph to facts one ;
* `resource_transfert.py`: functions to transfert information of resources from one resource to an other one ;
* `str_processing.py`: functions to work with labels which are in the knowledge graph ;
* `time_processing.py`: functions about time : compare instants, intevals... ;
* `wikidata.py`: functions get access to SPARQL endpoint of Wikidata.