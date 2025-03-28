from rdflib import Namespace

class NameSpaces():
    def __init__(self) -> None:
        self.__get_namespace_variables()
        self.__get_namespaces_with_prefixes()
        self.__get_query_prefixes_from_namespaces()

    def __get_namespace_variables(self):
        self.ADDR = Namespace("http://rdf.geohistoricaldata.org/def/address#")

        self.ATYPE = Namespace("http://rdf.geohistoricaldata.org/id/codes/address/attributeType/")
        self.LTYPE = Namespace("http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/")
        self.LRTYPE = Namespace("http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/")
        self.CTYPE = Namespace("http://rdf.geohistoricaldata.org/id/codes/address/changeType/")

        self.FACTS = Namespace("http://rdf.geohistoricaldata.org/id/address/facts/")
        self.FACTOIDS = Namespace("http://rdf.geohistoricaldata.org/id/address/factoids/")

        self.TIME = Namespace("http://www.w3.org/2006/time#")
        self.PROV = Namespace("http://www.w3.org/ns/prov#")
        self.RICO = Namespace("https://www.ica.org/standards/RiC/ontology#")
        self.GEO = Namespace("http://www.opengis.net/ont/geosparql#")
        self.GEOFLA = Namespace("http://data.ign.fr/def/geofla#")
        self.WD = Namespace("http://www.wikidata.org/entity/")

        self.SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
        self.RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        self.RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        self.XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
        self.OWL = Namespace("http://www.w3.org/2002/07/owl#")


        self.OFN = Namespace("http://www.ontotext.com/sparql/functions/")

    def __get_namespaces_with_prefixes(self):
        self.namespaces_with_prefixes = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Namespace):
                self.namespaces_with_prefixes[key.lower()] = value

    def __get_query_prefixes_from_namespaces(self):
        self.query_prefixes = ""
        for prefix, uri in self.namespaces_with_prefixes.items():
            str_uri = uri[""].n3()
            self.query_prefixes += f"PREFIX {prefix}: {str_uri}\n"
        

class OntologyMapping():
    def __init__(self) -> None:
        self.np = NameSpaces()
        self.__set_landmark_types()
        self.__set_landmark_relation_types()
        self.__set_attribute_types()
        self.__set_change_types()
        self.__set_datatypes()
        self.__set_time_units()
        self.__set_time_calendars()
        
    def __set_landmark_types(self):
        self.landmark_types = {
            "thoroughfare": self.np.LTYPE["Thoroughfare"],
            "house_number": self.np.LTYPE["HouseNumber"],
            "street_number": self.np.LTYPE["StreetNumber"],
            "district_number": self.np.LTYPE["DistrictNumber"],
            "municipality": self.np.LTYPE["Municipality"],
            "district": self.np.LTYPE["District"],
            "structure": self.np.LTYPE["Structure"],
            "country": self.np.LTYPE["Country"],
            "postal_code_area": self.np.LTYPE["PostalCodeArea"],
            "administrative_unity": self.np.LTYPE["AdministrativeUnity"],
            "undefined": self.np.LTYPE["Undefined"]
        }

    def __set_landmark_relation_types(self):
        self.landmark_relation_types = {
            "along": self.np.LRTYPE["Along"],
            "belongs": self.np.LRTYPE["Belongs"],
            "between": self.np.LRTYPE["Between"],
            "corner": self.np.LRTYPE["Corner"],
            "is_part_of": self.np.LRTYPE["IsPartOf"],
            "is_similar": self.np.LRTYPE["IsSimilar"],
            "touches": self.np.LRTYPE["Touches"],
            "within": self.np.LRTYPE["Within"],
            "starts_at": self.np.LRTYPE["StartsAt"],
            "ends_at": self.np.LRTYPE["EndsAt"],
        }

    def __set_attribute_types(self):
        self.attribute_types = {
            "geometry": self.np.ATYPE["Geometry"],
            "insee_code": self.np.ATYPE["InseeCode"],
            "name": self.np.ATYPE["Name"]
        }

    def __set_change_types(self):
        self.change_types = {
            "attribute_version_appearance": self.np.CTYPE["AttributeVersionAppearance"],
            "attribute_version_disappearance": self.np.CTYPE["AttributeVersionDisappearance"],
            "attribute_version_transition": self.np.CTYPE["AttributeVersionTransition"],
            "landmark_appearance": self.np.CTYPE["LandmarkAppearance"],
            "landmark_disappearance": self.np.CTYPE["LandmarkDisappearance"],
            "landmark_relation_appearance": self.np.CTYPE["LandmarkRelationAppearance"],
            "landmark_relation_disappearance": self.np.CTYPE["LandmarkRelationDisappearance"],
        }

    def __set_datatypes(self):
        self.datatypes = {
            "wkt_literal":self.np.GEO.wktLiteral,
            "geojson_literal":self.np.GEO.geoJSONLiteral,
            "gml_literal":self.np.GEO.gmlLiteral,
            "data_time_stamp":self.np.XSD.dateTimeStamp,
            "data_time":self.np.XSD.dateTime,
            "string":self.np.XSD.string,
            "integer":self.np.XSD.integer,
            "double":self.np.XSD.double,
            "bool":self.np.XSD.boolean
        }

    def __set_time_units(self):
        self.time_units = {
            "day": self.np.TIME["unitDay"],
            "month": self.np.TIME["unitMonth"],
            "year": self.np.TIME["unitYear"],
            "decade": self.np.TIME["unitDecade"],
            "century": self.np.TIME["unitCentury"],
            "millenium": self.np.TIME["unitMillenium"]
        }
        
    def __set_time_calendars(self):
        self.time_calendars = {
            "gregorian": self.np.WD["Q1985727"],
            "republican": self.np.WD["Q181974"],
            "julian": self.np.WD["Q1985786"],
        }
            

    def get_landmark_type(self, landmark_type: str):
        return self.landmark_types.get(landmark_type)
    
    def get_landmark_relation_type(self, landmark_relation_type: str):
        return self.landmark_relation_types.get(landmark_relation_type)
    
    def get_attribute_type(self, attribute_type: str):
        return self.attribute_types.get(attribute_type)
    
    def get_change_type(self, change_type: str):
        return self.change_types.get(change_type)
    
    def get_datatype(self, datatype: str):
        return self.datatypes.get(datatype)
    
    def get_time_unit(self, time_unit: str):
        return self.time_units.get(time_unit)
    
    def get_time_calendar(self, time_calendar: str):
        return self.time_calendars.get(time_calendar)