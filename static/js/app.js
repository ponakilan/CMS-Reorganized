document.addEventListener("DOMContentLoaded", async () => {
    const dropdownContainer = document.getElementById("dropdown-container");
    const form = document.getElementById("hcpcs-form");
    const heading = document.querySelector("h1"); // Heading to be dynamically updated
    const skipBtn = document.getElementById("skip");

    let hcpcsData = [];
    let drugsData = [];
    let nuccData = [];
    let isSecondSubmission = false;
    let isThirdSubmission = false;
    let isFourthSubmission = false;
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
    function createDropdown(id, dataSource, placeholder = "Search...") {
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

        // Track selected options
        const selectedOptions = [];

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

            // Add click event to toggle the checkbox icon and manage multi-selection
            option.addEventListener("click", () => {
                const isSelected = selectedOptions.includes(option);

                if (isSelected) {
                    // Deselect
                    selectedOptions.splice(selectedOptions.indexOf(option), 1);
                    option.classList.remove("selected");
                    const img = option.querySelector(".checkbox-icon");
                    img.src = "/static/assets/empty.png";
                } else {
                    // Select
                    selectedOptions.push(option);
                    option.classList.add("selected");
                    const img = option.querySelector(".checkbox-icon");
                    img.src = "/static/assets/filled.png";
                }

                // Move selected option to the top
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

        dropdownGroup.appendChild(searchBox);
        dropdownGroup.appendChild(dropdownList);

        dropdownContainer.appendChild(dropdownGroup);
    }


    // Form submission
    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        // Get all dropdowns and brand name inputs
        const dropdownGroups = Array.from(dropdownContainer.querySelectorAll(".dropdown-group"));
        //const brandNameInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input"));

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

                selectedItems.forEach(item => {
                    //const code = item.split(" - ")[0];
                    selectedCodes.push(item);
                });
                //brandNamesCodes.push(brandNameInputs[index]?.value || "");
            });
            localStorage.setItem("selectedCodes", JSON.stringify(selectedCodes));
            console.log(selectedCodes);

            if (selectedCodes.length === 0) {
                alert("Please select at least one code before proceeding.");
                return; 
            }
            await fetchDrugsData();
            heading.textContent = "Part-D Drugs Selection";
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", drugsData, "Search Brand Name...");
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

                //brandNamesDrugs.push(brandNameInputs[index]?.value || "");
            });
            console.log(selectedCodes);
            console.log(selectedBrnds);

            if (selectedBrnds.length === 0) {
                alert("Please select at least one drug before proceeding.");
                return; 
            }

            localStorage.setItem("selectedBrnds", JSON.stringify(selectedBrnds));
            localStorage.setItem("selectedGnrcs", JSON.stringify(selectedGnrcs));

            await fetchNuccData();
            heading.textContent = "Speciality Taxonomy Code Selection";
            skipBtn.style.display = 'none';
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", nuccData, "Search NUCC Codes...");
            isThirdSubmission = true;
        } else if (!isFourthSubmission) {
            dropdownGroups.forEach((dropdownGroup) => {
                const dropdownList = dropdownGroup.querySelector(".dropdown-list");
                const selectedItems = getSelectedItems(dropdownList);

                selectedItems.forEach(item => {
                    const code = item.split(" - ")[0];
                    selectedScodes.push(code);
                });
            });

            localStorage.setItem("selectedScodes", JSON.stringify(selectedScodes));

            selectedCodes = JSON.parse(localStorage.getItem("selectedCodes")) || [];
            selectedBrnds = JSON.parse(localStorage.getItem("selectedBrnds")) || [];

            console.log(selectedBrnds);
            console.log(selectedCodes);
            heading.textContent = "CMS part-b and part-d Brand name input";
            dropdownContainer.innerHTML = "";

            // Create input fields for selectedCodes
            selectedCodes.forEach((code) => {
                const codeGroup = document.createElement("div");
                codeGroup.classList.add("mb-3");

                const codeLabel = document.createElement("label");
                codeLabel.textContent = code;
                codeLabel.classList.add("form-label");

                const codeInput = document.createElement("input");
                codeInput.type = "text";
                codeInput.classList.add("form-control", "brand-input-codes");

                codeGroup.appendChild(codeLabel);
                codeGroup.appendChild(codeInput);
                dropdownContainer.appendChild(codeGroup);
            });

            // Create input fields for selectedBrnds
            selectedBrnds.forEach((brand) => {
                const brandGroup = document.createElement("div");
                brandGroup.classList.add("mb-3");

                const brandLabel = document.createElement("label");
                brandLabel.textContent = brand;
                brandLabel.classList.add("form-label");

                const brandInput = document.createElement("input");
                brandInput.type = "text";
                brandInput.classList.add("form-control", "brand-input-drugs");

                brandGroup.appendChild(brandLabel);
                brandGroup.appendChild(brandInput);
                dropdownContainer.appendChild(brandGroup);
            });



            isFourthSubmission = true;
        }
        else {
            // Retrieve data from input fields for codes
            const codeInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input-codes"));
            const codesInputValues = codeInputs.map(input => input.value.trim());
            brandNamesCodes = codesInputValues.filter(value => value); // Exclude empty values

            // Retrieve data from input fields for drugs
            const drugInputs = Array.from(dropdownContainer.querySelectorAll(".brand-input-drugs"));
            const drugsInputValues = drugInputs.map(input => input.value.trim());
            brandNamesDrugs = drugsInputValues.filter(value => value); // Exclude empty values

            localStorage.setItem("brandNamesCodes", JSON.stringify(brandNamesCodes));
            localStorage.setItem("brandNamesDrugs", JSON.stringify(brandNamesDrugs));
            selectedCodes = selectedCodes.map(item => item.split(" - ")[0].trim());
            localStorage.setItem("selectedCodes", JSON.stringify(selectedCodes));
            // Send POST request to `/cms-all-data`
            await fetch("/cms-all-data/", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": csrf_token,
                },
                body: JSON.stringify({
                    selected_codes: JSON.parse(localStorage.getItem("selectedCodes")),
                    brand_names_codes: JSON.parse(localStorage.getItem("brandNamesCodes")),
                    selected_brnds: JSON.parse(localStorage.getItem("selectedBrnds")),
                    selected_gnrcs: JSON.parse(localStorage.getItem("selectedGnrcs")),
                    brand_names_drugs: JSON.parse(localStorage.getItem("brandNamesDrugs")),
                    selected_scodes: JSON.parse(localStorage.getItem("selectedScodes")),
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
    createDropdown("dropdown-1", hcpcsData, "Search HCPCS Code...");

    skipBtn.addEventListener('click', async () => {
        if (count == 0) {
            await fetchDrugsData();
            heading.textContent = "Part-D Drugs Selection";
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", drugsData, "Search Brand Name...");
            isSecondSubmission = true;
            count = 1;
        }
        else if (count == 1) {

            await fetchNuccData();
            heading.textContent = "Speciality Taxonomy Code Selection";
            skipBtn.style.display = 'none';
            dropdownContainer.innerHTML = "";
            createDropdown("dropdown-1", nuccData, "Search NUCC Codes...");
            isThirdSubmission = true;
        }
    })
});