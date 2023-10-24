let NUM_NODES = 32;
let WINDOW_WIDTH;
let WINDOW_HEIGHT;
let cell_width;
let cell_height;
let left_margin;
let right_margin;
let bottom_margin;

let data;
let prev_data = {};
let loadingData = false;
function setup() {
  adjustVariables();
  createCanvas(WINDOW_WIDTH, WINDOW_HEIGHT);
  frameRate(1);
  background(200);

  dropdown = createSelect();
  dropdown.position(10, 10);
  dropdown.option('4');
  dropdown.option('8');
  dropdown.option('16');
  dropdown.option('32');
  dropdown.selected('32');  // Default value
  dropdown.changed(updateNodes);
}

function updateNodes() {
    NUM_NODES = int(dropdown.value());  // Update the number of nodes based on the dropdown value
    adjustVariables();
    redraw();  // Redraw the heatmap with the new number of nodes
}

function adjustVariables() { 
    WINDOW_WIDTH = windowWidth;
    WINDOW_HEIGHT = windowHeight;

    left_margin = WINDOW_WIDTH * 0.05;  // 5% of window width
    right_margin = WINDOW_WIDTH * 0.05;  // 5% of window width
    bottom_margin = WINDOW_HEIGHT * 0.1;  // 10% of window height

    cell_width = (WINDOW_WIDTH - left_margin - right_margin) / NUM_NODES;
    cell_height = (WINDOW_HEIGHT - bottom_margin) * 0.3 / 4;  // 60% of the available height divided by 4 rows
}

function get_cell_color(value) {
    let shade = 255 - (100 * value);
    return color(shade, 0, 0);
}

function draw_gradient_bar(x, y, w, h) {
    for (let i = 0; i < w; i++) {
        let shade = 255 - (100 * i / w);
        stroke(shade, 0, 0);
        line(x + i, y, x + i, y + h);
    }
}

function draw_row_labels(start_y) {
    fill(0);
    noStroke();
    let labels = ["Memory", "Nvme", "BB", "Pfs"];
    for (let i = 0; i < labels.length; i++) {
        text(labels[i], 10, start_y + i * cell_height + cell_height / 2);
    }
}

function draw_arrow(x, y, direction) {
    let arrow_size = cell_width / 8;  // Calculate arrow size based on cell width
    let max_arrow_size = 10;  // Set a maximum size for the arrow
    arrow_size = min(arrow_size, max_arrow_size);  // Use the smaller of the two values

    push();  // Save the current drawing style
    noStroke();  // Remove the stroke for the arrows

    if (direction === "up") {
        fill(255, 255, 0);  // Yellow for increase
        triangle(x, y - arrow_size,
                 x - arrow_size, y + arrow_size,
                 x + arrow_size, y + arrow_size);
    } else if (direction === "down") {
        fill(0, 0, 255);  // Blue for decrease
        triangle(x, y + arrow_size,
                 x - arrow_size, y - arrow_size,
                 x + arrow_size, y - arrow_size);
    }

    pop();  // Restore the previous drawing style
}

function generate_heatmap(data) {
    let heatmap_height = 4 * cell_height;
    let start_y = height - bottom_margin - heatmap_height;

    draw_row_labels(start_y);  // Draw the row labels

    // Get the list of node names
    let nodeNames = Object.keys(data);

    // Loop over the top NUM_NODES nodes
    for (let i = 0; i < NUM_NODES; i++) {
        let col = nodeNames[i];  // Get the node name for the current iteration

        // Draw column labels
        fill(0);  // Set text color to black
        text("Node " + (parseInt(col.split('-')[2]) + 1), left_margin + i * cell_width + cell_width / 2 - textWidth("Node " + (parseInt(col.split('-')[2]) + 1)) / 2, start_y - 5);

        let nodeData = data[col];
        for (let key in nodeData) {
            let value = nodeData[key];
            fill(get_cell_color(value));
            rect(left_margin + i * cell_width, start_y + Object.keys(nodeData).indexOf(key) * cell_height, cell_width, cell_height);

            // Draw arrows for increase or decrease
            let arrow_x = left_margin + i * cell_width + cell_width / 2;
            let arrow_y = start_y + Object.keys(nodeData).indexOf(key) * cell_height + cell_height / 2;
            if (prev_data[col] && prev_data[col][key] !== undefined) {
                if (value > prev_data[col][key]) {
                    draw_arrow(arrow_x, arrow_y, "up");
                } else if (value < prev_data[col][key]) {
                    draw_arrow(arrow_x, arrow_y, "down");
                }
            }

            // Print the actual value at the bottom of the square
            fill(255);  // White color for text
            text(value.toFixed(2), arrow_x - textWidth(value.toFixed(2)) / 2, start_y + Object.keys(nodeData).indexOf(key) * cell_height + cell_height - 5);
        }
    }

    // Draw the gradient bar below the heatmap
    let gradient_bar_y = start_y + heatmap_height + 10;  // 10 units below the heatmap
    let gradient_bar_width = (WINDOW_WIDTH - left_margin - right_margin) * 0.6;  // Reduced length of the gradient bar
    let gradient_bar_height = 15;  // Reduced height of the gradient bar
    let gradient_bar_x = (WINDOW_WIDTH - gradient_bar_width) / 2;  // Center the gradient bar
    draw_gradient_bar(gradient_bar_x, gradient_bar_y, gradient_bar_width, gradient_bar_height);
    
    // Label the gradient bar
    fill(0);  // Set text color to black
    noStroke();
    text("0", gradient_bar_x - textWidth("0") - 5, gradient_bar_y + gradient_bar_height / 2);
    text("1", gradient_bar_x + gradient_bar_width + 5, gradient_bar_y + gradient_bar_height / 2);
}

function draw() {
  background(255);

  if (!loadingData) {
    loadingData = true;
    httpGet('/data', 'json', false, function(response) {
      data = response;
      loadingData = false;
    }, function(error) {
      console.error(error);
      loadingData = false;
    });
  }

  if (data) {
    console.log('Data received:', data);
    text("Window Width is " + windowWidth, 30, 40);
    generate_heatmap(data);
    prev_data = data;
  }
}

function windowResized() {
    adjustVariables();
    resizeCanvas(WINDOW_WIDTH, WINDOW_HEIGHT);
}
