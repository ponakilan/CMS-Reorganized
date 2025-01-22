document.addEventListener("DOMContentLoaded", async () => {
    const dropdownContainer = document.getElementById("dropdown-container");
    const addDropdownButton = document.getElementById("add-dropdown");
    const form = document.getElementById("hcpcs-form");
    const heading = document.querySelector("h1"); // Heading to be dynamically updated
    const skipBtn = document.getElementById("skip");

    let hcpcsData = [];
    let drugsData = [];
    let nuccData = [];
    let isSecondSubmission = false;
    let isThirdSubmission = false;
    let count = 0;
    let csrf_token = document.getElementById("csrf").value;

    const selectedCodes = [];
    const brandNamesCodes = [];
    const selectedBrnds = [];
    const selectedGnrcs = [];
    const brandNamesDrugs = [];
    const selectedScodes = [];
    
    // Fetch HCPCS data
    async function fetchHCPCSData() {
        try {
            const response = await fetch("/cms-b");
            if (!response.ok) throw new Error("Failed to fetch HCPCS data");
            const data = await response.json();
            hcpcsData = data.drugs.map((item) => ({
                value: item.HCPCS_Cd,
                label: `${item.HCPCS_Cd} - ${item.HCPCS_Desc}`,
            }));
        } catch (error) {
            console.error("Error fetching HCPCS data:", error);
            alert("Failed to load HCPCS data. Please try again.");
        }
    }

    // Fetch Drugs data
    async function fetchDrugsData() {
        try {
            const response = await fetch("/cms-d");
            if (!response.ok) throw new Error("Failed to fetch Drugs data");
            const data = await response.json();
            drugsData = data.drugs.map((item) => ({
                value: `${item.Brnd_Name} - ${item.Gnrc_Name}`,
                label: `${item.Brnd_Name} - ${item.Gnrc_Name}`,
            }));
        } catch (error) {
            console.error("Error fetching Drugs data:", error);
            alert("Failed to load Drugs data. Please try again.");
        }
    }

    // Fetch NUCC data
    async function fetchNuccData() {
        try {
            const response = await fetch("/nucc");
            if (!response.ok) throw new Error("Failed to fetch NUCC data");
            const data = await response.json();
            nuccData = data.codes.map((item) => ({
                value: item.Code,
                label: `${item.Code} - ${item.Classification} - ${item.Specialization}`,
            }));
        } catch (error) {
            console.error("Error fetching NUCC data:", error);
            alert("Failed to load NUCC data. Please try again.");
        }
    }

// Create dropdown
function createDropdown(id, dataSource, includeBrandName = true, placeholder = "Search...", isDeletable = true) {
    const dropdownGroup = document.createElement("div");
    dropdownGroup.classList.add("mb-3", "dropdown-group");
    dropdownGroup.id = id;

    // Search box
    const searchBox = document.createElement("input");
    searchBox.type = "text";
    searchBox.placeholder = placeholder;
    searchBox.classList.add("form-control", "search-box");

    // Custom dropdown container
    const dropdownList = document.createElement("div");
    dropdownList.classList.add("dropdown-list", "form-select");

    // Keep track of the selected option
    let selectedOption = null;

    dataSource.forEach((item) => {
        const option = document.createElement("div");
        option.classList.add("dropdown-item");

        // Add checkbox image
        const checkboxIcon = document.createElement("img");
        checkboxIcon.src = "/static/assets/empty.png";
        checkboxIcon.alt = "empty checkbox";
        checkboxIcon.classList.add("checkbox-icon");

        // Add text content
        const optionText = document.createElement("span");
        optionText.textContent = item.label;

        // Append image and text to the option
        option.appendChild(checkboxIcon);
        option.appendChild(optionText);

        // Add click event to toggle the checkbox icon (Allow only one selection at a time)
        option.addEventListener("click", () => {
            // If an option is already selected, move it back to its original position
            if (selectedOption) {
                selectedOption.classList.remove("selected");
                const img = selectedOption.querySelector(".checkbox-icon");
                img.src = "/static/assets/empty.png";
            }

            // Set the clicked option as selected
            selectedOption = option;
            option.classList.add("selected");
            const img = option.querySelector(".checkbox-icon");
            img.src = "/static/assets/filled.png";

            // Move the selected option to the top of the dropdown
            dropdownList.prepend(option);
        });

        dropdownList.appendChild(option);
    });

    searchBox.addEventListener("input", () => {
        const query = searchBox.value.toLowerCase();
        Array.from(dropdownList.children).forEach((option) => {
            const text = option.textContent.toLowerCase();
            option.style.display = text.includes(query) ? "" : "none";
        });
    });

    if (includeBrandName) {
        const brandInput = document.createElement("input");
        brandInput.type = "text";
        brandInput.placeholder = "Enter Brand Name";
        brandInput.classList.add("form-control", "brand-input");
        brandInput.required = true;
        dropdownGroup.appendChild(brandInput);
    }

    dropdownGroup.appendChild(searchBox);
    dropdownGroup.appendChild(dropdownList);

    if (isDeletable) {
        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.textContent = "Delete";
        deleteButton.classList.add("btn", "btn-danger", "mt-2");
        deleteButton.addEventListener("click", () => {
            dropdownGroup.remove();
        });
        dropdownGroup.appendChild(deleteButton);
    }

    dropdownContainer.appendChild(dropdownGroup);
}


 // Form submission
form.addEventListener("submit", async (event) => {
    event.preventDefault();

    // Get all dropdowns and brand name inputs
    const dropdownGroups = Array.from(dropdownContainer.querySelectorAll(".dropdown-group"));
    const brandNameInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input"));

    // Arrays to hold selected values
    let selectedCodes = [];
    let selectedBrnds = [];
    let selectedGnrcs = [];
    let brandNamesCodes = [];
    let brandNamesDrugs = [];

    // Helper function to get selected values from the dropdown list
    const getSelectedItems = (dropdownList) => {
        const selectedItems = [];
        const items = dropdownList.querySelectorAll(".dropdown-item");

        items.forEach(item => {
            const checkboxIcon = item.querySelector(".checkbox-icon");
            if (checkboxIcon.src.includes("filled.png")) {
                const label = item.querySelector("span").textContent;
                selectedItems.push(label);
            }
        });

        return selectedItems;
    };

    if (!isSecondSubmission) {
        // Process the first phase (Codes Selection)
        dropdownGroups.forEach((dropdownGroup, index) => {
            const dropdownList = dropdownGroup.querySelector(".dropdown-list");
            const selectedItems = getSelectedItems(dropdownList);

            selectedCodes.push(...selectedItems);
            brandNamesCodes.push(brandNameInputs[index]?.value || "");
        });

        if (selectedCodes.length === 0) {
            alert("Please select at least one Code before proceeding.");
            return;
        }

        await fetchDrugsData();
        heading.textContent = "Part-D Drugs Selection";
        dropdownContainer.innerHTML = "";
        createDropdown("dropdown-1", drugsData, true, "Search Brand Name...", false);
        isSecondSubmission = true;
        count = 1;
    } else if (!isThirdSubmission) {
        // Process the second phase (Drugs Selection)
        dropdownGroups.forEach((dropdownGroup, index) => {
            const dropdownList = dropdownGroup.querySelector(".dropdown-list");
            const selectedItems = getSelectedItems(dropdownList);

            selectedItems.forEach(item => {
                const [brndName, gnrcName] = item.split(" - ");
                selectedBrnds.push(brndName);
                selectedGnrcs.push(gnrcName);
            });

            brandNamesDrugs.push(brandNameInputs[index]?.value || "");
        });

        if (selectedBrnds.length === 0) {
            alert("Please select at least one Drug before proceeding.");
            return;
        }

        await fetchNuccData();
        heading.textContent = "Speciality Taxonomy Code Selection";
        skipBtn.style.display = 'none';
        addDropdownButton.textContent = 'Add Another Code';
        dropdownContainer.innerHTML = "";
        createDropdown("dropdown-1", nuccData, false, "Search NUCC Codes...", false);
        isThirdSubmission = true;
    } else {
        // Process the final phase (Speciality Codes Selection)
        dropdownGroups.forEach((dropdownGroup) => {
            const dropdownList = dropdownGroup.querySelector(".dropdown-list");
            const selectedItems = getSelectedItems(dropdownList);

            selectedScodes.push(...selectedItems);
        });

        // Send POST request to `/cms-all-data`
        await fetch("/cms-all-data/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": csrf_token,
            },
            body: JSON.stringify({
                selected_codes: selectedCodes,
                brand_names_codes: brandNamesCodes,
                selected_brnds: selectedBrnds,
                selected_gnrcs: selectedGnrcs,
                brand_names_drugs: brandNamesDrugs,
                selected_scodes: selectedScodes,
            }),
        });

        // Update form action and method for redirection
        form.action = "/openpay-cat-page";
        form.method = "get";
        form.submit(); // Submit the form
    }
});


    // Initial setup
    await fetchHCPCSData();
    createDropdown("dropdown-1", hcpcsData, true, "Search HCPCS Code...", false);

    addDropdownButton.addEventListener("click", () => {
        const newId = `dropdown-${dropdownContainer.children.length + 1}`;
        createDropdown(newId, isThirdSubmission ? nuccData : isSecondSubmission ? drugsData : hcpcsData, isThirdSubmission ? false : true);
    });

    skipBtn.addEventListener('click',async()=>{
        if(count == 0){
            
            await fetchDrugsData();
            heading.textContent = "Part-D Drugs Selection";
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", drugsData, true, "Search Brand Name...", false);
            isSecondSubmission = true;
            count = 1;
        }
        else if(count == 1){
            await fetchNuccData();
            heading.textContent = "Speciality Taxonomy Code Selection";
            skipBtn.style.display= 'none' ;
            //add btn text modified
            addDropdownButton.textContent = 'Add Another Code';
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", nuccData, false, "Search NUCC Codes...", false);
            isThirdSubmission = true;
        }
    })
});
