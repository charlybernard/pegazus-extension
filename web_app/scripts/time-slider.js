function manageTimeSlider(dateSliderDivId, dateInputDivId, startTimeStamp, endTimeStamp) {
    var dateSlider = document.getElementById(dateSliderDivId);
    var dateInput = document.getElementById(dateInputDivId);

    // Dates de référence (point de départ et d'arrivée)
    var startDate = new Date(startTimeStamp);
    var endDate = new Date(endTimeStamp);

    // Définir les bornes du slider
    var totalDays = Math.floor((endDate - startDate) / (1000 * 60 * 60 * 24));
    dateSlider.min = 0;
    dateSlider.max = totalDays;

    // Synchronisation des événements
    dateSlider.addEventListener("input", () => updateDateFromSlider(startDate, dateSlider, dateInput));
    dateInput.addEventListener("input", () => updateSliderFromInput(startDate, endDate, dateSlider, dateInput));

    // Initialiser la date affichée
    updateDateFromSlider(startDate, dateSlider, dateInput);
}

// Fonction pour mettre à jour l'affichage de la date depuis le slider
function updateDateFromSlider(startDate, dateSlider, dateInput) {
    var newDate = new Date(startDate);
    newDate.setDate(startDate.getDate() + parseInt(dateSlider.value));

    // Mettre à jour l'input date
    dateInput.value = newDate.toISOString().split('T')[0];
}

// Fonction pour mettre à jour le slider depuis l'input date
function updateSliderFromInput(startDate, endDate, dateSlider, dateInput) {
    var selectedDate = new Date(dateInput.value);

    // Vérifier si la date est dans la plage autorisée
    if (selectedDate < startDate) {
        selectedDate = startDate;
        dateInput.value = startDate.toISOString().split('T')[0];
    }
    if (selectedDate > endDate) {
        selectedDate = endDate;
        dateInput.value = endDate.toISOString().split('T')[0];
    }

    var diffDays = Math.floor((selectedDate - startDate) / (1000 * 60 * 60 * 24));

    // Mettre à jour le slider
    dateSlider.value = diffDays;
}