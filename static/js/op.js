document.addEventListener("DOMContentLoaded", () => {
    // Fetch JSON data for dropdowns
    const form = document.querySelector("form");
    async function fetchOpenPayData() {
        try {
            const response = await fetch("/openpay-cat");
            if (!response.ok) throw new Error("Failed to fetch data");
            const data = await response.json();

            populateDropdowns(data.categories);
        } catch (error) {
            console.error("Error fetching OpenPay data:", error);
        }
    }

    function populateDropdowns(categories) {
        const originalDropdown = document.getElementById("original-dropdown");
        const renamedDropdown = document.getElementById("renamed-dropdown");

        let hasNullOption = false; // Flag to ensure only one "Unspecified" option is added

        categories.forEach(category => {
            // Populate Original dropdown
            const originalOption = document.createElement("option");
            originalOption.value = category.Original;
            originalOption.textContent = category.Original;
            originalDropdown.appendChild(originalOption);

            // Populate Renamed dropdown
            if (category.Renamed) {
                const renamedOption = document.createElement("option");
                renamedOption.value = category.Renamed;
                renamedOption.textContent = category.Renamed;
                renamedDropdown.appendChild(renamedOption);
            } else if (!hasNullOption) {
                // Add a single "Unspecified" option for all null values
                const unspecifiedOption = document.createElement("option");
                unspecifiedOption.value = "Others";
                unspecifiedOption.textContent = "Others";
                renamedDropdown.appendChild(unspecifiedOption);
                hasNullOption = true;
            }
        });

        // Ensure "Unspecified" is the last option
        if (hasNullOption) {
            const unspecifiedOption = renamedDropdown.querySelector("option[value='Others']");
            renamedDropdown.appendChild(unspecifiedOption);
        }
    }


    // Initialize fetch
    fetchOpenPayData();

    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        // Get selected values from the dropdowns
        const selectedOriginal = document.getElementById("original-dropdown").value;
        const selectedRenamed = document.getElementById("renamed-dropdown").value;

        // Send data to backend
        await fetch("/openpay-data", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                selected_oname: selectedOriginal,
                selected_rename: selectedRenamed,
            }),
        });
    });
});
