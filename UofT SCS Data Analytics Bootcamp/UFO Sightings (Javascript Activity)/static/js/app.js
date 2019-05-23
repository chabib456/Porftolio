// from data.js
var tableData = data;

// YOUR CODE HERE!
var tbody = d3.select("tbody");
tableData.forEach((ufoSighting) => {
    var row = tbody.append("tr");
    Object.entries(ufoSighting).forEach(([key, value]) => {
      var cell = row.append("td");
      cell.text(value);
    });
  });
var submit = d3.select("#filter-btn");
// Delete Existing Table
function deleteChild() { 
    var e = document.querySelector("tbody"); 
     e.innerHTML = ""; 
 } 
submit.on("click", function() {
    deleteChild();
    // Prevent the page from refreshing
    d3.event.preventDefault();
  
    // Select the input element and get the raw HTML node
    var inputElement_date = d3.select("#datetime");
    var inputElement_city = d3.select("#city");
    var inputElement_state = d3.select("#state");
    var inputElement_country = d3.select("#country");
    var inputElement_shape = d3.select("#shape");
    // Get the value property of the input elements
    var inputValue_date = inputElement_date.property("value");
    var inputValue_city = inputElement_city.property("value");
    var inputValue_state = inputElement_state.property("value");
    var inputValue_country = inputElement_country.property("value");
    var inputValue_shape = inputElement_shape.property("value");
    // Initialiaze filtered data variable
    var filteredData = tableData
    // Check for user inouts and filter accordingly
    if(inputValue_date){filteredData = filteredData.filter(sighting => sighting.datetime == inputValue_date);};
    if(inputValue_city){filteredData = filteredData.filter(sighting => sighting.city == inputValue_city);};
    if(inputValue_state){filteredData = filteredData.filter(sighting => sighting.state == inputValue_state);};
    if(inputValue_country){filteredData = filteredData.filter(sighting => sighting.country == inputValue_country);};
    if(inputValue_shape){filteredData = filteredData.filter(sighting => sighting.shape == inputValue_shape);};
   // Append filtered data to table
    var tbody = d3.select("tbody");
    filteredData.forEach((ufoSighting) => {
        var row = tbody.append("tr");
        Object.entries(ufoSighting).forEach(([key, value]) => {
      var cell = row.append("td");
      cell.text(value);
    });
  });
});