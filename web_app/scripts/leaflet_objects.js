//############################################ Definitions of markers ################################################//

class LeafletObjects{
    constructor(L){
        // L is the object used by Leaflet
        var symbolsFolder = "./symbols/";

        this.blackMarkerPath = symbolsFolder + 'black_marker.svg';
        var greenMarkerPath = symbolsFolder + 'green_marker.svg';
        var orangeMarkerPath = symbolsFolder + 'orange_marker.svg';
        var redMarkerPath = symbolsFolder + 'red_marker.svg';
        var greyMarkerPath = symbolsFolder + 'grey_marker.svg';
        var blueMarkerPath = symbolsFolder + 'blue_marker.svg';
        var greenDotPath = symbolsFolder + 'green_dot.svg';
        var orangeDotPath = symbolsFolder + 'orange_dot.svg';
        var redDotPath = symbolsFolder + 'red_dot.svg';
        var greyDotPath = symbolsFolder + 'grey_dot.svg';
        var blueDotPath = symbolsFolder + 'blue_dot.svg';
        var blueMarkerPath = symbolsFolder + 'blue_marker.svg';

        var markerIconHeight = 45;
        var markerIconWidth = 30;
        var markerIconAnchorX = markerIconWidth/2;
        var markerIconAnchorY = markerIconHeight;

        var dotIconHeight = 14;
        var dotIconWidth = 14;
        var dotIconAnchorX = dotIconWidth/2;
        var dotIconAnchorY = dotIconHeight/2;

        this.greenMarker = L.icon({
            iconUrl: greenMarkerPath,
            iconSize:     [markerIconWidth, markerIconHeight], // size of the icon
            iconAnchor:   [markerIconAnchorX, markerIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.orangeMarker = L.icon({
            iconUrl: orangeMarkerPath,
            iconSize:     [markerIconWidth, markerIconHeight], // size of the icon
            iconAnchor:   [markerIconAnchorX, markerIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.redMarker = L.icon({
            iconUrl: redMarkerPath,
            iconSize:     [markerIconWidth, markerIconHeight], // size of the icon
            iconAnchor:   [markerIconAnchorX, markerIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.greyMarker = L.icon({
            iconUrl: greyMarkerPath,
            iconSize:     [markerIconWidth, markerIconHeight], // size of the icon
            iconAnchor:   [markerIconAnchorX, markerIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.blueMarker = L.icon({
            iconUrl: blueMarkerPath,
            iconSize:     [markerIconWidth, markerIconHeight], // size of the icon
            iconAnchor:   [markerIconAnchorX, markerIconAnchorY], // point of the icon which will correspond to marker's location
            popupAnchor : [markerIconAnchorX, markerIconAnchorY], // location of marker popup (above marker)

        });

        this.greenDot = L.icon({
            iconUrl: greenDotPath,
            iconSize:     [dotIconWidth, dotIconHeight], // size of the icon
            iconAnchor:   [dotIconAnchorX, dotIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.orangeDot = L.icon({
            iconUrl: orangeDotPath,
            iconSize:     [dotIconWidth, dotIconHeight], // size of the icon
            iconAnchor:   [dotIconAnchorX, dotIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.redDot = L.icon({
            iconUrl: redDotPath,
            iconSize:     [dotIconWidth, dotIconHeight], // size of the icon
            iconAnchor:   [dotIconAnchorX, dotIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.greyDot = L.icon({
            iconUrl: greyDotPath,
            iconSize:     [dotIconWidth, dotIconHeight], // size of the icon
            iconAnchor:   [dotIconAnchorX, dotIconAnchorY], // point of the icon which will correspond to marker's location
        });

        this.blueDot = L.icon({
            iconUrl: blueDotPath,
            iconSize:     [dotIconWidth, dotIconHeight], // size of the icon
            iconAnchor:   [dotIconAnchorX, dotIconAnchorY], // point of the icon which will correspond to marker's location
        });

        // Variables to define style of features according events
        this.greenSelectedPolygonStyle = {'color':'green', 'fillOpacity':'0.5', 'weight':'4'};
        this.greenDefaultPolygonStyle = {'color':'green', 'fillOpacity':'0.2', 'weight':'2'};
        this.redSelectedPolygonStyle = {'color':'red', 'fillOpacity':'0.5', 'weight':'4'};
        this.redDefaultPolygonStyle = {'color':'red', 'fillOpacity':'0.2', 'weight':'2'};
        this.greenSelectedLineStringStyle = {'color':'green', 'opacity':'0.8', 'weight':'8'};
        this.greenDefaultLineStringStyle = {'color':'green', 'opacity':'0.5', 'weight':'4'};
        this.redSelectedLineStringStyle = {'color':'red', 'opacity':'0.8', 'weight':'8'};
        this.redDefaultLineStringStyle = {'color':'red', 'opacity':'0.5', 'weight':'4'};

    }
}