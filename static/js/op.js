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

        categories.forEach(category => {
            // Populate Original dropdown
            const originalOption = document.createElement("option");
            originalOption.value = category.Original;
            originalOption.textContent = category.Original;
            originalDropdown.appendChild(originalOption);

            // Populate Renamed dropdown
            const renamedOption = document.createElement("option");
            renamedOption.value = category.Renamed || "Unspecified";
            renamedOption.textContent = category.Renamed || "Unspecified";
            renamedDropdown.appendChild(renamedOption);
        });
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
