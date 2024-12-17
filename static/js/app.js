document.addEventListener("DOMContentLoaded", async () => {
    const dropdownContainer = document.getElementById("dropdown-container");
    const addDropdownButton = document.getElementById("add-dropdown");
    const form = document.getElementById("hcpcs-form");

    let hcpcsData = [];

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

    // Function to create dropdown with search functionality and delete button
    function createDropdown(id, isDeletable = true) {
        const dropdownGroup = document.createElement("div");
        dropdownGroup.classList.add("mb-3", "dropdown-group");
        dropdownGroup.id = id;

        // Search box for filtering dropdown
        const searchBox = document.createElement("input");
        searchBox.type = "text";
        searchBox.placeholder = "Search...";
        searchBox.classList.add("form-control", "search-box");

        // Dropdown element
        const select = document.createElement("select");
        select.name = "hcpcs_codes";
        select.classList.add("form-select", "dropdown");
        select.size = 5;

        // Populate dropdown options
        hcpcsData.forEach((item) => {
            const option = document.createElement("option");
            option.value = item.value;
            option.textContent = item.label;
            select.appendChild(option);
        });

        // Add search functionality
        searchBox.addEventListener("input", () => {
            const query = searchBox.value.toLowerCase();
            Array.from(select.options).forEach((option) => {
                option.style.display = option.textContent.toLowerCase().includes(query) ? "" : "none";
            });
        });

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

    // Fetch HCPCS data and add the first dropdown
    await fetchHCPCSData();
    createDropdown("dropdown-1", false); // First dropdown: non-deletable, with search functionality

    // Add new dropdowns dynamically
    addDropdownButton.addEventListener("click", () => {
        const newId = `dropdown-${dropdownContainer.children.length + 1}`;
        createDropdown(newId);
    });

    // Handle form submission
    form.addEventListener("submit", (event) => {
        const brandName = document.getElementById("brand-name").value;

        const selectedCodes = Array.from(dropdownContainer.querySelectorAll("select")).flatMap((select) =>
            Array.from(select.selectedOptions).map((option) => option.value)
        );

        // Log and send data before submitting
        console.log("Selected Codes:", selectedCodes);
        console.log("Brand Name:", brandName);

        fetch("/cms-b-selected", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ selected_codes: selectedCodes, brand_names: [brandName] }),
        })
            .then((response) => response.json())
            .then((data) => {
                console.log("POST Response:", data);
                form.submit(); // Submit GET request to /cms-d
            })
            .catch((error) => {
                console.error("Error submitting data:", error);
                alert("Failed to submit data.");
            });

        event.preventDefault(); // Prevent immediate form submission
    });
});
