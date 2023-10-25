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

let dropdown;
let DELTA_THRESHOLD = 0.05;
let userSetNodes = false;

let isPlaying = true;
let playPauseButton;

let rectHeight = 130; // Global parameter to set the rectangle's height

function setup() {
    WINDOW_WIDTH = windowWidth;
    WINDOW_HEIGHT = windowHeight;

    createCanvas(WINDOW_WIDTH, WINDOW_HEIGHT);
    frameRate(1);
    background(200);

    dropdown = createSelect();
    dropdown.position(10, 10);
    updateDropdownOptions(NUM_NODES, NUM_NODES);  // Initialize dropdown with current NUM_NODES
    dropdown.changed(updateNodes);

    playPauseButton = createButton('Pause');
    playPauseButton.position(dropdown.x + dropdown.width + 20, 10);
    playPauseButton.mousePressed(togglePlayPause);
}

function draw() {
    background(255);

    if (isPlaying) {
        if (!loadingData) {
            loadingData = true;
            httpGet('/metadata', 'json', false, function (response) {
                data = preprocessData(response);
                loadingData = false;
            }, function (error) {
                console.error(error);
                loadingData = false;
            });
        }
    }

    if (data) {
        fill(0);  // Set text color to black
        generate_heatmap(data.nodes);
        drawNodesAndTargets(data.nodes);
        // drawBlobs(data);
        if (isPlaying) {
            prev_data = JSON.parse(JSON.stringify(data.nodes));
        }
    }
}

function preprocessData(data) {
    const nodes = {};
    console.log("received data", data)

    for (let target of data.targets) {
        const node_id = target.node_id;
        const target_type = target.name;
        const remaining_capacity = target.remaining_capacity;
        const target_id = target.id;

        // Ensure the node object exists
        if (!nodes[node_id]) {
            nodes[node_id] = {};
        }

        // Ensure the target type object exists
        if (!nodes[node_id][target_type]) {
            nodes[node_id][target_type] = {};
        }

        // Set the cap and id properties
        nodes[node_id][target_type]['cap'] = remaining_capacity;
        nodes[node_id][target_type]['id'] = target_id;

    }

    if(!userSetNodes){
        NUM_NODES = Object.keys(nodes).length
        console.log("num Nodes", nodes)
        updateDropdownOptions(NUM_NODES, NUM_NODES)
        adjustVariables()
    }
    const blobs = data.blobs || [];
    console.log(nodes)
    console.log(blobs)
    return {nodes, blobs};
}

///////////////////////////////////////
//blobs
//////////////////////////////////////
function drawNodesAndTargets(nodes) {
    pop();
  if (!nodes) {
    console.error("Nodes are undefined");
    return; // Exit the function if nodes are not available
  }

  let nodeIndex = 0;
  let marginX = 100; // Adjust as needed
  let marginY = 50;  // Adjust as needed

  Object.entries(nodes).forEach(([nodeName, targets]) => {
    let targetIndex = 0;

    // Draw node label on top
    pop();
    fill(0); // Black text
    textSize(14); // Adjust as needed
    textAlign(CENTER, BOTTOM);
    text(nodeName, marginX + (nodeIndex * cell_width) + (cell_width / 2), marginY - 10);
    push();
    Object.entries(targets).forEach(([targetType, targetData]) => {
      let x = marginX + (nodeIndex * cell_width);
      let y = marginY + (targetIndex * rectHeight); // Use the rectHeight here

      // Draw a rectangle for each target
      fill(200); // A grey color, change as needed
      stroke(0); // Black border
      rect(x, y, cell_width, rectHeight); // Use the rectHeight here

      // Draw target label on the left side
      if (nodeIndex === 0) { // Only draw once for the first nod
          pop();
        fill(0); // Black text for target names
        textAlign(RIGHT, CENTER);
        text(targetType, marginX - 10, y + rectHeight / 2);
        push();
      }

      targetIndex++;
    });

    nodeIndex++;
  });
  push();
}

function drawBlobs(data) {
    data.blobs.forEach(blob => {
        let targetInfo = findTargetByBlobId(data.targets, blob.buffer_info.target_id);
        if (targetInfo) {
            let blobX = targetInfo.nodeX + random(nodeWidth - blobDiameter);
            let blobY = targetInfo.targetY + random(targetHeight - blobDiameter);

            fill(255, 0, 0); // Red color for blobs
            ellipse(blobX, blobY, blobDiameter, blobDiameter);

            // Optional: Add text for blob name/score
            fill(0);
            textSize(10);
            text(blob.name, blobX - textWidth(blob.name) / 2, blobY + 4);
        }
    });
}

function findTargetByBlobId(targets, targetId) {
    let nodeX = 0;
    for (let [nodeName, nodeTargets] of Object.entries(targets)) {
        for (let [targetName, target] of Object.entries(nodeTargets)) {
            if (target.id === targetId) {
                return {
                    nodeX: nodeX,
                    targetY: targetHeight * Object.keys(nodeTargets).indexOf(targetName) + 50,
                };
            }
        }
        nodeX += nodeWidth + 10;
    }
    return null;
}

////////////////////////////////////////
//Control
///////////////////////////////////////
function togglePlayPause() {
    isPlaying = !isPlaying;
    playPauseButton.html(isPlaying ? 'Pause' : 'Play');
}

function updateDropdownOptions(numNodes, selectedValue) {
    dropdown.html('');  // Clear existing options
    for (let i = 1; i <= numNodes; i++) {
        dropdown.option(i.toString());
    }
    dropdown.selected(selectedValue.toString());  // Set default selection based on parameter
}

//Callback for the drop down menu
function updateNodes() {
    NUM_NODES = int(dropdown.value());
    userSetNodes = true; // User has manually set the number of nodes
    adjustVariables();
    redraw();
}

//Adjust all global variables for the window size and num nodes chosen
function adjustVariables() {
    WINDOW_WIDTH = windowWidth;
    WINDOW_HEIGHT = windowHeight;

    left_margin = WINDOW_WIDTH * 0.05;
    right_margin = WINDOW_WIDTH * 0.05;
    bottom_margin = WINDOW_HEIGHT * 0.1;

    cell_width = (WINDOW_WIDTH - left_margin - right_margin) / NUM_NODES;
    cell_height = (WINDOW_HEIGHT - bottom_margin) * 0.3 / 4;
}

function windowResized() {
    adjustVariables();
    resizeCanvas(WINDOW_WIDTH, WINDOW_HEIGHT);
}


////////////////////////////////////////
// heatmap
////////////////////////////////////////

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

function draw_row_labels(start_y, labels) {
    fill(0);
    noStroke();
    for (let i = 0; i < labels.length; i++) {
        text(labels[i], 10, start_y + i * cell_height + cell_height / 2);
    }
}

function draw_arrow(x, y, change) {
    let arrow_size = cell_width / 10;  // Start with a base size
    let scaling_factor = constrain(map(NUM_NODES, 1, 32, 1.5, 1), 1, 1.5);  // Scale based on number of nodes
    arrow_size *= scaling_factor;

    let max_arrow_size = 15;  // Set a maximum size for the arrow
    arrow_size = min(arrow_size, max_arrow_size);  // Use the smaller of the two values

    push();  // Save the current drawing style

    if (Math.abs(change) < DELTA_THRESHOLD) {
        fill(255, 255, 255);  // White color for negligible change
        textSize(arrow_size * 5);  // Increase text size
        textAlign(CENTER, CENTER);
        text("=", x, y);
    } else {
        let direction = change > 0 ? "up" : "down";
        if (direction === "up") {
            fill(255, 255, 0);  // Yellow for increase
            noStroke();
            triangle(x, y - arrow_size,
                x - arrow_size, y + arrow_size,
                x + arrow_size, y + arrow_size);
        } else if (direction === "down") {
            fill(0, 0, 255);  // Blue for decrease
            noStroke();
            triangle(x, y + arrow_size,
                x - arrow_size, y - arrow_size,
                x + arrow_size, y - arrow_size);
        }
    }

    pop();  // Restore the previous drawing style
}

function generate_heatmap(data) {
    pop();
    let nodeNames = Object.keys(data);
    let targetTypes = Object.keys(data[nodeNames[0]]);  // Assuming all nodes have the same target types
    let heatmap_height = targetTypes.length * cell_height;

    let start_y = height - bottom_margin - heatmap_height;

    draw_row_labels(start_y, targetTypes);

    // Loop over the top NUM_NODES nodes
    for (let i = 0; i < NUM_NODES; i++) {
        let col = nodeNames[i];  // Get the node name for the current iteration

        // Draw column labels
        fill(0);  // Set text color to black
        text("Node " + (i + 1), left_margin + i * cell_width + cell_width / 2 - textWidth("Node " + (i + 1)) / 2, start_y - 5);

        let nodeData = data[col];
        for (let key in nodeData) {
            let target = nodeData[key];
            let value = target.cap
            fill(get_cell_color(value));
            rect(left_margin + i * cell_width, start_y + Object.keys(nodeData).indexOf(key) * cell_height, cell_width, cell_height);

            // Draw arrows for increase or decrease
            let arrow_x = left_margin + i * cell_width + cell_width / 2;
            let arrow_y = start_y + Object.keys(nodeData).indexOf(key) * cell_height + cell_height / 2;
            if (prev_data[col] && prev_data[col][key] !== undefined) {
                let change = value - prev_data[col][key].cap;
                if (change > 0) {
                    draw_arrow(arrow_x, arrow_y, change);
                } else if (change < 0) {
                    draw_arrow(arrow_x, arrow_y, change);
                } else {
                    draw_arrow(arrow_x, arrow_y, change);
                }
            }

            // Print the actual value at the bottom of the square
            fill(255);  // White color for text
            text(value.toFixed(2), arrow_x - textWidth(value.toFixed(2)) / 2, start_y + Object.keys(nodeData).indexOf(key) * cell_height + cell_height - 5);
        }
    }

    // Draw the gradient bar below the heatmap
    let gradient_bar_y = start_y + heatmap_height + 10;  // Adjusted to use heatmap_height
    let gradient_bar_width = (WINDOW_WIDTH - left_margin - right_margin) * 0.6;  // Reduced length of the gradient bar
    let gradient_bar_height = 15;  // Reduced height of the gradient bar
    let gradient_bar_x = (WINDOW_WIDTH - gradient_bar_width) / 2;  // Center the gradient bar
    draw_gradient_bar(gradient_bar_x, gradient_bar_y, gradient_bar_width, gradient_bar_height);

    // Label the gradient bar
    fill(0);  // Set text color to black
    noStroke();
    text("0", gradient_bar_x - textWidth("0") - 5, gradient_bar_y + gradient_bar_height / 2);
    text("1", gradient_bar_x + gradient_bar_width + 5, gradient_bar_y + gradient_bar_height / 2);
    push();
}