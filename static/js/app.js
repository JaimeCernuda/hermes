let HEATMAP_NODES = 32;
let BLOB_NODES = 32;
let WINDOW_WIDTH;
let WINDOW_HEIGHT;
let cell_width_heatmap;
let cell_width_blobs;
let cell_height;
let left_margin;
let right_margin;
let bottom_margin;

let data;
let prev_id = null;
let prev_data = null;
let temp_data = null; // Newly fetched data goes here

let loadingData = false;

let dropdown_heatmap;
let dropdown_blobs;
let checkbox_buckets;
let checkbox_heatmap;
let checkbox_focus_bucket;
let dropdown_buckets;
let checkbox_focus_blobs;
let input_blob_focus;

let DELTA_THRESHOLD = 0.05;
let userSetNodes = false;


let isPlaying = true;
let playPauseButton;

let blobTableHeight = 130; // Global parameter to set the rectangle's height
let blobRadius = 5;
let blobToBucketMap = {};
let blob_stroke_weight = 3;
let displayBucket = true;
let displayHeatmap = true;
let focusedBlobs = [];
let shouldFocusBuckets = false;
let shouldFocusBlobs = false;

let img;

const colors = [
    "#FF5733", "#33FF57", "#5733FF",
    "#FF3357", "#33FF33", "#3333FF",
    "#FFD700", "#FF6347", "#40E0D0",
    "#EE82EE", "#8A2BE2", "#5F9EA0",
    "#D2691E", "#6495ED", "#7FFF00",
    "#DC143C", "#00FFFF", "#00008B",
    "#008B8B", "#B8860B", "#A9A9A9",
    "#006400", "#BDB76B", "#8B008B",
    "#556B2F", "#FF8C00", "#9932CC",
    "#8B0000", "#E9967A", "#9400D3",
    "#FF00FF", "#1E90FF", "#00FF00",
    "#FF4500", "#DA70D6", "#EEE8AA",
    "#98FB98", "#AFEEEE", "#D87093",
    "#FFEFD5", "#FFDAB9", "#CD853F",
    "#FFC0CB", "#DDA0DD", "#B0E0E6",
    "#800080", "#FF0000", "#BC8F8F",
    "#4169E1", "#8B4513", "#FA8072",
    "#FAA460", "#2E8B57", "#FFF5EE",
    "#A0522D", "#C71585", "#6B8E23",
    "#FFA07A", "#FFA500", "#FFB6C1",
]

function preload() {
    grc_png = loadImage(IMAGE_URL1);
    grc_jpeg = loadImage(IMAGE_URL2);
}

function setup() {
    background(255)
    WINDOW_WIDTH = windowWidth;
    WINDOW_HEIGHT = windowHeight;

    createCanvas(WINDOW_WIDTH, WINDOW_HEIGHT);

    const base_x = 10

    playPauseButton = createButton('Pause');
    playPauseButton.position(10, 110);
    playPauseButton.mousePressed(togglePlayPause);

    dropdown_blobs = createSelect('Blob Nodes');
    dropdown_blobs.position(base_x, playPauseButton.y + playPauseButton.height + 10);

    dropdown_heatmap = createSelect('Hmap Nodes');
    dropdown_heatmap.position(base_x, dropdown_blobs.y + dropdown_blobs.height + 10);

    updateDropdownOptions(HEATMAP_NODES, HEATMAP_NODES);  // Initialize dropdown with current NUM_NODES
    dropdown_heatmap.changed(updateHeatMapNodes);
    dropdown_blobs.changed(updateBlobNodes);


    checkbox_buckets = createCheckbox('Buckets', true);
    checkbox_buckets.position(base_x, dropdown_heatmap.y + dropdown_heatmap.height + 10);
    checkbox_buckets.changed(checkboxBucketEvent);

    checkbox_heatmap = createCheckbox('Heatmap', true);
    checkbox_heatmap.position(base_x, checkbox_buckets.y + checkbox_buckets.height + 10);
    checkbox_heatmap.changed(checkboxHeatmapEvent);

    checkbox_focus_bucket = createCheckbox('FC Buck', false);
    checkbox_focus_bucket.position(base_x, checkbox_heatmap.y + checkbox_heatmap.height + 10);
    checkbox_focus_bucket.changed(checkboxFocusBucketEvent);

    dropdown_buckets = createSelect('');
    dropdown_buckets.position(base_x, checkbox_focus_bucket.y + checkbox_focus_bucket.height + 10);
    dropdown_buckets.attribute('disabled', '');

    checkbox_focus_blobs = createCheckbox('Fc Blobs', false);
    checkbox_focus_blobs.position(base_x, dropdown_buckets.y + dropdown_buckets.height + 10);
    checkbox_focus_blobs.changed(checkboxFocusBlobsEvent);

    input_blob_focus = createInput('');
    input_blob_focus.position(checkbox_focus_blobs.x, checkbox_focus_blobs.y + checkbox_focus_blobs.height + 10);
    input_blob_focus.input(inputBlobEvent);
    input_blob_focus.size(70, 20)
}

function inputBlobEvent() {
    let inputString = this.value(); // This refers to the input element
    let parsedNumbers = [];

    // Split the input string by commas
    let components = inputString.split(",");

    components.forEach(component => {
        // Check if the component has a '-'
        if (component.includes("-")) {
            // Extract start and end of the range
            let [start, end] = component.split("-").map(Number);

            if (isNaN(start) || isNaN(end)) {
                // If either value is NaN, skip this segment
                return;
            }

            // Generate and push all numbers in the range to parsedNumbers
            for (let i = start; i <= end; i++) {
                parsedNumbers.push(i);
            }
        } else {
            // Directly push the number to parsedNumbers
            parsedNumbers.push(Number(component));
        }
    });

    // Update focusedBlobs array
    focusedBlobs = parsedNumbers;

    console.log(focusedBlobs); // For debugging
}

function checkboxFocusBucketEvent() {
  if (checkbox_focus_bucket.checked()) {
    shouldFocusBuckets = true;
  } else {
    shouldFocusBuckets = false;
  }
}

function checkboxFocusBlobsEvent() {
  if (checkbox_focus_blobs.checked()) {
    shouldFocusBlobs = true;
  } else {
    shouldFocusBlobs = false;
  }
}
function checkboxBucketEvent() {
  if (checkbox_buckets.checked()) {
    displayBucket = true;
    blob_stroke_weight = 3
  } else {
    displayBucket = false;
    blob_stroke_weight = 0
  }
}

function checkboxHeatmapEvent() {
  if (checkbox_heatmap.checked()) {
    displayHeatmap = true;
  } else {
    displayHeatmap = false;
  }
  adjustVariables();
}

function draw() {
    background(255)
    if (isPlaying && !loadingData) {
        loadingData = true;
        httpGet('/metadata', 'json', false, function (response) {
            let newData = preprocessData(response);

            if (data && newData && newData.data_id !== data.data_id) {
                prev_data = data;  // Set prev_data to the current value of data before updating it
                data = newData;  // Now update data with the new fetched value
            } else if (!data && newData) {
                data = newData;  // If data is undefined, simply populate it with the fetched data
            }

            loadingData = false;
        }, function (error) {
            console.error(error);
            loadingData = false;
        });
    }

    image(grc_jpeg, 1, 10, left_margin/2, left_margin/2);
    line(left_margin - 60, bottom_margin, left_margin - 60, WINDOW_HEIGHT);

    if (data && data.nodes && prev_data) {
        fill(0);  // Set text color to black
        if(displayHeatmap){
            console.log("heatmap", displayHeatmap)
            generate_heatmap(data.nodes);
        }
        drawBlobs(data.nodes, data.blobs);
    }
}

function preprocessData(data) {
    const nodes = {};
    const data_id = data.id;
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

    if (!userSetNodes) {
        HEATMAP_NODES = Object.keys(nodes).length
        BLOB_NODES = Object.keys(nodes).length
        updateDropdownOptions(BLOB_NODES, BLOB_NODES)
        adjustVariables()
    }
    const blobs = data.blobs || [];
    const buckets = data.buckets || [];
    mapBlobsToBuckets(buckets);
    setBucketFocusValue(buckets)
    return {data_id, nodes, blobs};
}

///////////////////////////////////////
//blobs
//////////////////////////////////////
function setBucketFocusValue(buckets){
    let currentValue = dropdown_buckets.value(); // Store the current selected value

    dropdown_buckets.html('');  // Clear existing options

    buckets.forEach(bucket => {
        dropdown_buckets.option(bucket.name);
    });

    // Reset the selected value if it's still a valid option
    if (buckets.some(bucket => bucket.name === currentValue)) {
        dropdown_buckets.selected(currentValue);
    }

    dropdown_buckets.removeAttribute('disabled');
}
function drawBlobs(nodes, blobs) {
    if (!nodes || !blobs) {
        console.error("Data is undefined");
        return;
    }

    let nodeIndex = 0;
    let marginX = left_margin;
    let marginY = bottom_margin - 20;
    let hoveredBlob = null; // To store blob data when hovered

    const nodeEntries = Object.entries(nodes);

    for (let i = 0; i < BLOB_NODES; i++) {
        let [nodeName, targets] = nodeEntries[i];
        let targetIndex = 0;

        fill(0);
        noStroke();
        textSize(14);
        // text(nodeName, left_margin + i * cell_width + cell_width / 2 - textWidth(nodeName) / 2, start_y - 5);
        text(nodeName, marginX + (nodeIndex * cell_width_blobs) + (cell_width_blobs / 2) - textWidth(nodeName) / 2, marginY - 10);

        Object.entries(targets).forEach(([targetType, targetData]) => {
            let x = marginX + (nodeIndex * cell_width_blobs);
            let y = marginY + (targetIndex * blobTableHeight);

            fill(200);
            stroke(0);
            rect(x, y, cell_width_blobs, blobTableHeight);

            fill(0);
            noStroke();
            if (nodeIndex === 0) {
                fill(0);
                textSize(14);
                text(targetType, marginX - 30 - textWidth(targetType) / 2, y + blobTableHeight / 2);
            }

            let blobRow = 0;
            let blobCol = 0;
            let maxBlobsPerRow = Math.floor(cell_width_blobs / 30);
            let exceededHeight = false;

            blobs.forEach(blob => {
                if (blob.buffer_info.target_id === targetData.id &&
                    (shouldFocusBlobs === false || (shouldFocusBlobs === true && focusedBlobs.includes(blob.id))) &&
                    (shouldFocusBuckets === false || (shouldFocusBuckets === true && blob.bucketName === dropdown_buckets.value()))
                ){
                    let blobX = x + 10 + (blobCol * 30) + blob_stroke_weight/2;
                    let blobY = y + 10 + (blobRow * 30) + blob_stroke_weight/2;

                    if (blobY + 20 > y + blobTableHeight) {
                        exceededHeight = true;
                        return;
                    }

                    if (!exceededHeight) {
                        const bucketInfo = blobToBucketMap[blob.id];
                        if (bucketInfo && displayBucket) {
                            // console.log("Bucket: ", bucketInfo.bucketName)
                            const bucketColor = getColorForBucket(bucketInfo.bucketName); // A function to return a color based on the bucket's name.
                            strokeWeight(blob_stroke_weight)
                            stroke(bucketColor);
                            // If you want to add the bucket's name to the blob's details:
                            blob.bucketName = bucketInfo.bucketName;
                        }
                        fill(0, 0, 255);
                        ellipse(blobX, blobY, 20);

                        stroke(0)
                        strokeWeight(1)
                        fill(255);
                        textSize(10);
                        text(blob.id, blobX - textWidth(blob.id)/2, blobY+2.5);
                        fill(0);

                        // Check for hover over blob
                        if (dist(mouseX, mouseY, blobX, blobY) <= 10) {
                            hoveredBlob = blob;
                        }

                        blobCol++;
                        if (blobCol >= maxBlobsPerRow) {
                            blobRow++;
                            blobCol = 0;
                        }
                    }
                }

            });

            if (exceededHeight) {
                fill(0);
                textSize(20);
                text("...", x + cell_width_blobs / 2, y + blobTableHeight - 10);
            }

            targetIndex++;
        });

        nodeIndex++;
    }

    // If a blob is hovered over, display its details
    if (hoveredBlob) {
        displayBlobDetails(hoveredBlob);
    }
}


function displayBlobDetails(blob) {
    push();
    const x = mouseX + 10;  // Offset to not overlap the cursor
    const y = mouseY;
    const width = 150;  // Adjust as necessary
    const height = 160; // Adjust based on the number of details

    // Draw a semi-transparent background
    fill(255, 255, 255, 200);
    stroke(0);
    rect(x, y, width, height);

    // Display blob details
    noStroke();
    fill(0);
    textSize(12);
    text(`Name: ${blob.name}`, x + 10, y + 20);
    text(`ID: ${blob.id}`, x + 10, y + 40);
    text(`Access Frequency: ${blob.access_frequency}`, x + 10, y + 60);
    text(`Size: ${blob.buffer_info.size}`, x + 10, y + 80);
    text(`Target: ${blob.buffer_info.target_id}`, x + 10, y + 100);
    text(`Score: ${blob.score}`, x + 10, y + 120);
    if (blob.bucketName) {
        text(`Bucket: ${blob.bucketName}`, x + 10, y + 140);
    }
    pop();
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

function mapBlobsToBuckets(buckets) {
    buckets.forEach(bucket => {
        bucket.blobs.forEach(blobId => {
            blobToBucketMap[blobId] = {
                bucketName: bucket.name,
                bucketId: bucket.id
            };
        });
    });
}

function getColorForBucket(bucketName) {
    // Simple hashing function to convert a string to a number
    let hash = 0;
    for (let i = 0; i < bucketName.length; i++) {
        hash = (hash << 5) - hash + bucketName.charCodeAt(i);
        hash |= 0; // Convert to a 32-bit integer
    }

    // Use the absolute value of hash % colors.length to get an index for our colors array
    const colorIndex = Math.abs(hash) % colors.length;
    return colors[colorIndex];
}

////////////////////////////////////////
//Control
///////////////////////////////////////
function togglePlayPause() {
    isPlaying = !isPlaying;
    playPauseButton.html(isPlaying ? 'Pause' : 'Play');
}

function updateDropdownOptions(numNodes, selectedValue) {
    dropdown_heatmap.html('');  // Clear existing options
    for (let i = 1; i <= numNodes; i++) {
        dropdown_heatmap.option(i.toString());
    }
    dropdown_heatmap.selected(selectedValue.toString());  // Set default selection based on parameter

    dropdown_blobs.html('');  // Clear existing options
    for (let i = 1; i <= numNodes; i++) {
        dropdown_blobs.option(i.toString());
    }
    dropdown_blobs.selected(selectedValue.toString());  // Set default selection based on parameter
}

//Callback for the drop down menu
function updateHeatMapNodes() {
    HEATMAP_NODES = int(dropdown_heatmap.value());
    userSetNodes = true; // User has manually set the number of nodes
    adjustVariables();
    redraw();
}

function updateBlobNodes() {
    BLOB_NODES = int(dropdown_blobs.value());
    userSetNodes = true; // User has manually set the number of nodes
    adjustVariables();
    redraw();
}

//Adjust all global variables for the window size and num nodes chosen
function adjustVariables() {
    WINDOW_WIDTH = windowWidth;
    WINDOW_HEIGHT = windowHeight;

    left_margin = WINDOW_WIDTH * 0.08;
    right_margin = WINDOW_WIDTH * 0.02;
    bottom_margin = WINDOW_HEIGHT * 0.07;

    cell_width_heatmap = (WINDOW_WIDTH - left_margin - right_margin) / HEATMAP_NODES;
    cell_width_blobs = (WINDOW_WIDTH - left_margin - right_margin) / BLOB_NODES;
    cell_height = (WINDOW_HEIGHT - bottom_margin) * 0.3 / 4;

    if(displayHeatmap){
        blobTableHeight = (WINDOW_HEIGHT - bottom_margin - 5.4* cell_height) / 4
    }else{
        blobTableHeight = (WINDOW_HEIGHT - 1.5* bottom_margin) / 4
    }
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
    textSize(14);
    for (let i = 0; i < labels.length; i++) {
        //textAlign(CENTER, CENTER);
        text(labels[i], left_margin - 30 - textWidth(labels[i]) / 2, start_y + i * cell_height + cell_height / 2);
    }
}

function draw_arrow(x, y, change) {
    let arrow_size = cell_width_heatmap / 10;  // Start with a base size
    let scaling_factor = constrain(map(HEATMAP_NODES, 1, 32, 1.5, 1), 1, 1.5);  // Scale based on number of nodes
    arrow_size *= scaling_factor;

    let max_arrow_size = 15;  // Set a maximum size for the arrow
    arrow_size = min(arrow_size, max_arrow_size);  // Use the smaller of the two values

    push();  // Save the current drawing style

    if (Math.abs(change) < DELTA_THRESHOLD) {
        fill(255, 255, 255);  // White color for negligible change
        textSize(arrow_size * 5);  // Increase text size
        //textAlign(CENTER, CENTER);
        text("=", x - textWidth("=")/2, y + 1.5*arrow_size);
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
    push();
    let nodeNames = Object.keys(data);
    let targetTypes = Object.keys(data[nodeNames[0]]);  // Assuming all nodes have the same target types
    let heatmap_height = targetTypes.length * cell_height;

    let start_y = height - bottom_margin - heatmap_height;

    draw_row_labels(start_y, targetTypes);

    // Loop over the top NUM_NODES nodes
    for (let i = 0; i < HEATMAP_NODES; i++) {
        let col = nodeNames[i];  // Get the node name for the current iteration

        // Draw column labels
        fill('black');  // Set text color to black
        textSize(14);
        noStroke();
        text("Node " + (i + 1), left_margin + i * cell_width_heatmap + cell_width_heatmap / 2 - textWidth("Node " + (i + 1)) / 2, start_y - 5);

        let nodeData = data[col];
        for (let key in nodeData) {
            let target = nodeData[key];
            let value = target.cap
            fill(get_cell_color(value));
            rect(left_margin + i * cell_width_heatmap, start_y + Object.keys(nodeData).indexOf(key) * cell_height, cell_width_heatmap, cell_height);

            // Draw arrows for increase or decrease
            let arrow_x = left_margin + i * cell_width_heatmap + cell_width_heatmap / 2;
            let arrow_y = start_y + Object.keys(nodeData).indexOf(key) * cell_height + cell_height / 2;
            // console.log("prev data", prev_data)
            if (prev_data.nodes[col] && prev_data.nodes[col][key] !== undefined) {
                let change = value - prev_data.nodes[col][key].cap;
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
            textSize(10);
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
    textSize(14);
    text("0", gradient_bar_x - textWidth("0") - 5, gradient_bar_y + gradient_bar_height / 2 + 6);
    text("1", gradient_bar_x + gradient_bar_width + 5, gradient_bar_y + gradient_bar_height / 2 + 6);
    pop();
}