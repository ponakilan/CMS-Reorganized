document.addEventListener("DOMContentLoaded", async () => {
    const dropdown = document.getElementById("hcpcs-dropdown");
    const submitButton = document.getElementById("submit-button");

    // Fetch data from /cms-b
    try {
        const response = await fetch("/cms-b", {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
            },
        });

        if (!response.ok) {
            throw new Error("Failed to fetch data");
        }

        const data = await response.json();
        const drugs = data.drugs;

        // Populate dropdown with fetched data
        drugs.forEach((item) => {
            const option = document.createElement("option");
            option.value = item.HCPCS_Cd;
            option.textContent = `${item.HCPCS_Cd} - ${item.HCPCS_Desc}`;
            dropdown.appendChild(option);
        });
    } catch (error) {
        console.error("Error fetching data:", error);
    }

    // Handle form submission
    submitButton.addEventListener("click", async () => {
        const selectedOptions = Array.from(dropdown.selectedOptions);
        const selectedCodes = selectedOptions.map((option) => option.value);

        const payload = {
            selected_codes: selectedCodes,
        };

        try {
            const response = await fetch("/cms-b-selected", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                throw new Error("Failed to submit selected codes");
            }

            const result = await response.json();
            alert(`Successfully submitted ${result.count} codes!`);
        } catch (error) {
            console.error("Error submitting data:", error);
        }
    });
});
