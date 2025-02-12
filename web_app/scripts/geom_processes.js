//####################################################################################################################//
//################################### Functions to get WKT strings from Geojson ######################################//
//####################################################################################################################//

function geojsonToWktForPoint(pointCoords){
    // From the list of coords of a point, get a Point WKT

    return "POINT ({} {})".format(pointCoords[0], pointCoords[1]);
}

function geojsonToWktForLineString(lineStringCoords){
    // From the list of coords of a point, get a Linestring WKT

    var str1 = lineStringCoords.join('-');
    var str2 = str1.replaceAll(',', ' ').replaceAll('-', ',');
    var wktLineString = "LINESTRING({})".format(str2);

    return wktLineString;
}

function geojsonToWktForPolygon(polygonCoords){
    // From the list of coords of a point, get a Polygon WKT

    var subPolygons = []

    for (index in polygonCoords){
        var str1 = polygonCoords[index].join('&');
        var str2 = str1.replaceAll(',', ' ');
        subPolygons.push(str2);
    }

    var str3 = subPolygons.join('@');
    var str4 = str3.replaceAll('@','),(').replaceAll('&', ',');
    var wktPolygon = "POLYGON(({}))".format(str4);

    return wktPolygon;
}

function geojsonToWkt(geojsonFeature){
    var geomType = geojsonFeature.geometry.type;
    var geomCoords = geojsonFeature.geometry.coordinates;

    if (geomType.toLowerCase() === 'point'){
        return geojsonToWktForPoint(geomCoords);
    }else if (geomType.toLowerCase() === 'linestring'){
        return geojsonToWktForLineString(geomCoords);
    }else if (geomType.toLowerCase() === 'polygon'){
        return geojsonToWktForPolygon(geomCoords);
    }else{
        return null;
    }
}

//####################################################################################################################//
//####################################### Functions to get Geojson from WKT strings ##################################//
//####################################################################################################################//


a


//####################################################################################################################//
//####################################################################################################################//
//####################################################################################################################//

function latLngToPtWkt(lat, lng){
    return "POINT({} {})".format(lng, lat);
}

function wktPtToLatLng(wktPt){
    // From a wkt describing a point, get a [lat, lng] list

    var lngLat = wktToGeojsonGeom(wktPt).coordinates;
    return [lngLat[1], lngLat[0]];
}