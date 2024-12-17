var select_count = 2;

var select_container = document.getElementById("select-container");

function addSelect() {
    const select = `
        <select name="select-${select_count}">
            <option value="1">99443 - Telephone medical discussion with physician, 21-30 minutes</option>
            <option value="2">70492 - Ct scan of soft tissue of neck before and after contrast</option>
            <option value="3">73590 - X-ray of lower leg, 2 views</option>
            <option value="4">99347 - Established patient home visit, typically 15 minutes</option>
        </select>
        <input type="text" name="text-${select_count}" class="form-control" placeholder="Brand name">
        <button class="btn btn-danger dlt-btn" onclick="removeRecord(${select_count})">Delete</button>
    `;
    var select_group = document.createElement("div");
    select_group.className = "main";
    select_group.id = `main-${select_count}`;
    select_group.innerHTML = select;
    select_container.append(select_group);
    select_count++;
    create_custom_dropdowns();
}

function removeRecord(id) {
    var select_group = document.getElementById(`main-${id}`);
    select_group.remove()
}
