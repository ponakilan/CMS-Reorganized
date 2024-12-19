document.addEventListener("DOMContentLoaded", async () => {
    const dropdownContainer = document.getElementById("dropdown-container");
    const addDropdownButton = document.getElementById("add-dropdown");
    const form = document.getElementById("hcpcs-form");
    const heading = document.querySelector("h1"); // Heading to be dynamically updated

    let hcpcsData = []; // Stores initial HCPCS data
    let drugsData = []; // Stores data from /cms-d
    let isSecondSubmission = false; // Flag to track form submission phase

    // Fetch HCPCS data from backend
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

    // Fetch Drugs data from backend
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

    // Function to create dropdown with search, input box, and delete button
    function createDropdown(id, dataSource, placeholder = "Search...", isDeletable = true) {
        const dropdownGroup = document.createElement("div");
        dropdownGroup.classList.add("mb-3", "dropdown-group");
        dropdownGroup.id = id;

        // Search box for filtering dropdown
        const searchBox = document.createElement("input");
        searchBox.type = "text";
        searchBox.placeholder = placeholder;
        searchBox.classList.add("form-control", "search-box");

        // Dropdown element
        const select = document.createElement("select");
        select.name = "dropdown-select";
        select.classList.add("form-select", "dropdown");
        select.size = 5;

        // Populate dropdown options
        dataSource.forEach((item) => {
            const option = document.createElement("option");
            option.value = item.value;
            option.textContent = item.label;
            select.appendChild(option);
        });

        // Search functionality
        searchBox.addEventListener("input", () => {
            const query = searchBox.value.toLowerCase();
            Array.from(select.options).forEach((option) => {
                option.style.display = option.textContent.toLowerCase().includes(query) ? "" : "none";
            });
        });

        // Input box for brand name
        const brandInput = document.createElement("input");
        brandInput.type = "text";
        brandInput.placeholder = "Enter Brand Name";
        brandInput.classList.add("form-control", "brand-input");
        brandInput.required = true;

        // Append elements to dropdown group
        dropdownGroup.appendChild(brandInput);
        dropdownGroup.appendChild(searchBox);
        dropdownGroup.appendChild(select);

        // Add delete button (if deletable)
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

    // Handle form submission
    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        const brandNameInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input"));
        const dropdowns = Array.from(dropdownContainer.querySelectorAll("select"));

        const selectedCodes = [];
        const selectedBrnds = [];
        const selectedGnrcs = [];
        const brandNames = [];

        // Collect data based on the dropdowns
        dropdowns.forEach((dropdown, index) => {
            const selectedOptions = Array.from(dropdown.selectedOptions);

            selectedOptions.forEach((option) => {
                if (!isSecondSubmission) {
                    selectedCodes.push(option.value); // HCPCS Codes for first phase
                } else {
                    const [brndName, gnrcName] = option.textContent.split(" - ");
                    selectedBrnds.push(brndName);
                    selectedGnrcs.push(gnrcName);
                }
            });
            brandNames.push(brandNameInputs[index].value);
        });

        // First submission: POST to /cms-b-selected and fetch new data
        if (!isSecondSubmission) {
            await fetch("/cms-b-selected", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    selected_codes: selectedCodes,
                    brand_names: brandNames,
                }),
            });

            // Fetch new data from /cms-d
            await fetchDrugsData();

            // Update heading and dropdowns
            heading.textContent = "Drugs Selection";
            dropdownContainer.innerHTML = ""; // Clear old dropdowns
            createDropdown("dropdown-1", drugsData, "Search Brand Name...", false);
            isSecondSubmission = true; // Set flag for second submission
        } 
        // Second submission: POST to /cms-d-selected
        else {
            await fetch("/cms-d-selected", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    selected_brnds: selectedBrnds,
                    selected_gnrcs: selectedGnrcs,
                    brand_names: brandNames,
                }),
            });

            console.log("Second submission complete!");
            alert("Data successfully submitted for Drugs Selection!");
        }
    });

    // Fetch initial HCPCS data and create the first dropdown
    await fetchHCPCSData();
    createDropdown("dropdown-1", hcpcsData, "Search HCPCS Code...", false);

    // Add new dropdowns dynamically
    addDropdownButton.addEventListener("click", () => {
        const newId = `dropdown-${dropdownContainer.children.length + 1}`;
        createDropdown(newId, isSecondSubmission ? drugsData : hcpcsData, isSecondSubmission ? "Search Brand Name..." : "Search HCPCS Code...");
    });
});
