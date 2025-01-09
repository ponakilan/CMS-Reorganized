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

        // Dropdown
        const select = document.createElement("select");
        select.name = "dropdown-select";
        select.classList.add("form-select", "dropdown");
        select.size = 5;

        dataSource.forEach((item) => {
            const option = document.createElement("option");
            option.value = item.value;
            option.textContent = item.label;
            select.appendChild(option);
        });

        searchBox.addEventListener("input", () => {
            const query = searchBox.value.toLowerCase();
            Array.from(select.options).forEach((option) => {
                option.style.display = option.textContent.toLowerCase().includes(query) ? "" : "none";
            });
        });
        
        // Add brand input if required
        if (includeBrandName) {
            const brandInput = document.createElement("input");
            brandInput.type = "text";
            brandInput.placeholder = "Enter Brand Name";
            brandInput.classList.add("form-control", "brand-input");
            brandInput.required = true;
            dropdownGroup.appendChild(brandInput);
        }

        dropdownGroup.appendChild(searchBox);
        dropdownGroup.appendChild(select);

        // Delete button
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

        const dropdowns = Array.from(dropdownContainer.querySelectorAll("select"));
        const brandNameInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input"));

        if (!isSecondSubmission) {
            dropdowns.forEach((dropdown, index) => {
                Array.from(dropdown.selectedOptions).forEach((option) => {
                    selectedCodes.push(option.value);
                });
                brandNamesCodes.push(brandNameInputs[index]?.value || "");
            });

            await fetchDrugsData();
            heading.textContent = "Part-D Drugs Selection";
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", drugsData, true, "Search Brand Name...", false);
            isSecondSubmission = true;
        } else if (!isThirdSubmission) {
            dropdowns.forEach((dropdown, index) => {
                Array.from(dropdown.selectedOptions).forEach((option) => {
                    const [brndName, gnrcName] = option.textContent.split(" - ");
                    selectedBrnds.push(brndName);
                    selectedGnrcs.push(gnrcName);
                });
                brandNamesDrugs.push(brandNameInputs[index]?.value || "");
            });

            await fetchNuccData();
            heading.textContent = "Speciality Taxonomy Code Selection";
            skipBtn.style.display= 'none' ;
            //add btn text modified
            addDropdownButton.textContent = 'Add Another Code';
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", nuccData, false, "Search NUCC Codes...", false);
            isThirdSubmission = true;
        } else {
            // Collect selected dropdown values
            dropdowns.forEach((dropdown) => {
                Array.from(dropdown.selectedOptions).forEach((option) => {
                    selectedScodes.push(option.value);
                });
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
             form.action = "/openpay-cat-page"; // Change form action
             form.method = "get"; // Change form method
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

    
});
