var map = L.map('map').setView([37.8, -96], 4);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19,
            attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

async function addGeoJsonLayer(endpoint) {
  L.geoJson(await (await fetch(endpoint)).json()).addTo(map);
}

addGeoJsonLayer("county-geo")



