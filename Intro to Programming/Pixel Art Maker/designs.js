//Set constants
const colorVal = document.querySelector('#colorPicker');
const sizePicker = document.querySelector("#sizePicker");
const table = document.querySelector('#pixelCanvas');

// Select size input
// const sizePicker = document.getElementById("sizePicker");

// const subBtn = document.getElementById("btnSubmit").addEventListener("click", function () {
//     sizePicker.submit();
//     makeGrid();
// });

//add listener for clicking the submit button
sizePicker.addEventListener("submit", function(e) {
    e.preventDefault();
    makeGrid();
});

//runs a loop taking the height and width requested for the grid
//and creates cells that can be colored when click
function makeGrid() {
    //clears the table data when submit is clicked
    table.innerHTML = "";

    let gridH = document.getElementById('inputHeight').value;
    let gridW = document.getElementById('inputWidth').value;

    for (let r = 0; r < gridH; r++) {
        let row = table.insertRow(r);
        for (let c = 0; c < gridW; c++) {
            let column = row.insertCell(c);

            //color a cell by clicking it
            column.addEventListener('click', function (event) {
                column.style.backgroundColor = colorVal.value;
            });
            //clear a cell by double clicking it
            column.addEventListener("dblclick", function(event){
                column.style.backgroundColor = "";

            });
        }
    }
}

